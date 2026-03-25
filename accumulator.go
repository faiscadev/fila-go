package fila

import (
	"context"
	"fmt"
	"sync"
	"time"

	filav1 "github.com/faisca/fila-go/filav1"
)

// AccumulatorMode controls how Enqueue() accumulates messages internally.
type AccumulatorMode interface {
	isAccumulatorMode()
}

// AccumulatorModeAuto enables opportunistic accumulation. A background goroutine
// collects enqueue requests: it blocks waiting for the first message, then
// non-blocking drains any additional messages that are already waiting.
// At low load each message is sent individually; at high load messages
// naturally cluster into batches. Zero configuration, zero latency penalty.
//
// This is the default mode.
type AccumulatorModeAuto struct{}

func (AccumulatorModeAuto) isAccumulatorMode() {}

// AccumulatorModeLinger enables timer-based forced accumulation. Messages are
// accumulated until either LingerMs milliseconds have elapsed since the
// first message in the batch, or MaxSize messages have been collected,
// whichever comes first.
type AccumulatorModeLinger struct {
	LingerMs int
	MaxSize  int
}

func (AccumulatorModeLinger) isAccumulatorMode() {}

// AccumulatorModeDisabled disables accumulation entirely. Each Enqueue() call
// makes a direct gRPC RPC.
type AccumulatorModeDisabled struct{}

func (AccumulatorModeDisabled) isAccumulatorMode() {}

// EnqueueMessage describes a message to be enqueued.
type EnqueueMessage struct {
	Queue   string
	Headers map[string]string
	Payload []byte
}

// EnqueueManyResult holds the result of a single message in an EnqueueMany call.
type EnqueueManyResult struct {
	MessageID string
	Err       error
}

// accumulatorItem is an internal request sent to the accumulator goroutine.
type accumulatorItem struct {
	ctx  context.Context
	msg  EnqueueMessage
	done chan accumulatorResult
}

type accumulatorResult struct {
	messageID string
	err       error
}

// accumulator manages the background goroutine that collects and flushes
// enqueue requests according to the configured AccumulatorMode.
type accumulator struct {
	svc  filav1.FilaServiceClient
	mode AccumulatorMode
	ch   chan *accumulatorItem

	wg     sync.WaitGroup
	stopCh chan struct{}
}

func newAccumulator(svc filav1.FilaServiceClient, mode AccumulatorMode) *accumulator {
	a := &accumulator{
		svc:    svc,
		mode:   mode,
		ch:     make(chan *accumulatorItem, 4096),
		stopCh: make(chan struct{}),
	}
	a.wg.Add(1)
	go a.run()
	return a
}

func (a *accumulator) run() {
	defer a.wg.Done()
	switch m := a.mode.(type) {
	case AccumulatorModeAuto:
		a.runAuto()
	case AccumulatorModeLinger:
		a.runLinger(m)
	default:
		a.runAuto()
	}
}

// runAuto implements the opportunistic accumulation algorithm:
// block for first message, non-blocking drain of anything else waiting,
// then flush concurrently.
func (a *accumulator) runAuto() {
	for {
		// Block waiting for the first item (or stop signal).
		var first *accumulatorItem
		select {
		case first = <-a.ch:
		case <-a.stopCh:
			return
		}

		batch := []*accumulatorItem{first}

		// Non-blocking drain of anything else already in the channel.
	drain:
		for {
			select {
			case item := <-a.ch:
				batch = append(batch, item)
			default:
				break drain
			}
		}

		// Flush concurrently so the accumulator can keep collecting.
		a.wg.Add(1)
		go func(items []*accumulatorItem) {
			defer a.wg.Done()
			a.flush(items)
		}(batch)
	}
}

// runLinger implements timer-based forced accumulation.
func (a *accumulator) runLinger(m AccumulatorModeLinger) {
	lingerDuration := time.Duration(m.LingerMs) * time.Millisecond
	maxSize := m.MaxSize
	if maxSize <= 0 {
		maxSize = 100
	}

	for {
		// Block waiting for the first item.
		var first *accumulatorItem
		select {
		case first = <-a.ch:
		case <-a.stopCh:
			return
		}

		batch := []*accumulatorItem{first}
		timer := time.NewTimer(lingerDuration)

	collect:
		for len(batch) < maxSize {
			select {
			case item := <-a.ch:
				batch = append(batch, item)
			case <-timer.C:
				break collect
			case <-a.stopCh:
				timer.Stop()
				// Flush what we have before stopping.
				a.flush(batch)
				return
			}
		}
		timer.Stop()

		a.wg.Add(1)
		go func(items []*accumulatorItem) {
			defer a.wg.Done()
			a.flush(items)
		}(batch)
	}
}

// flush sends accumulated enqueue requests to the server using the
// unified Enqueue RPC (which accepts repeated messages).
func (a *accumulator) flush(items []*accumulatorItem) {
	msgs := make([]*filav1.EnqueueMessage, len(items))
	for i, item := range items {
		msgs[i] = &filav1.EnqueueMessage{
			Queue:   item.msg.Queue,
			Headers: item.msg.Headers,
			Payload: item.msg.Payload,
		}
	}

	// Use the first item's context for the RPC. If any individual
	// context is already cancelled the caller will see it through their
	// done channel timeout.
	resp, err := a.svc.Enqueue(items[0].ctx, &filav1.EnqueueRequest{
		Messages: msgs,
	})

	if err != nil {
		mappedErr := mapEnqueueError(err)
		for _, item := range items {
			item.done <- accumulatorResult{err: mappedErr}
		}
		return
	}

	results := resp.GetResults()
	for i, item := range items {
		if i < len(results) {
			r := results[i]
			switch v := r.Result.(type) {
			case *filav1.EnqueueResult_MessageId:
				item.done <- accumulatorResult{messageID: v.MessageId}
			case *filav1.EnqueueResult_Error:
				item.done <- accumulatorResult{err: enqueueErrorToItemError(v.Error)}
			default:
				item.done <- accumulatorResult{err: &ItemError{Message: "unknown result type"}}
			}
		} else {
			item.done <- accumulatorResult{err: fmt.Errorf("enqueue: server returned fewer results than messages sent")}
		}
	}
}

// submit sends a message to the accumulator and waits for the result.
func (a *accumulator) submit(ctx context.Context, msg EnqueueMessage) (string, error) {
	item := &accumulatorItem{
		ctx:  ctx,
		msg:  msg,
		done: make(chan accumulatorResult, 1),
	}

	select {
	case a.ch <- item:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	select {
	case res := <-item.done:
		return res.messageID, res.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// drain flushes any pending messages and waits for all in-flight flushes
// to complete.
func (a *accumulator) drain() {
	close(a.stopCh)
	// Drain any remaining items in the channel.
	for {
		select {
		case item := <-a.ch:
			// Flush single items directly during drain.
			a.flush([]*accumulatorItem{item})
		default:
			a.wg.Wait()
			return
		}
	}
}
