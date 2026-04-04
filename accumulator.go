package fila

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/faisca/fila-go/fibp"
)

// maxAutoBatchSize caps the number of items drained in a single auto-mode
// batch to avoid exceeding the maximum frame size.
const maxAutoBatchSize = 1000

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
// makes a direct request.
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
	conn *conn
	mode AccumulatorMode
	ch   chan *accumulatorItem

	wg     sync.WaitGroup
	stopCh chan struct{}
}

func newAccumulator(c *conn, mode AccumulatorMode) *accumulator {
	a := &accumulator{
		conn:   c,
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

func (a *accumulator) runAuto() {
	for {
		var first *accumulatorItem
		select {
		case first = <-a.ch:
		case <-a.stopCh:
			return
		}

		batch := []*accumulatorItem{first}

	drain:
		for len(batch) < maxAutoBatchSize {
			select {
			case item := <-a.ch:
				batch = append(batch, item)
			default:
				break drain
			}
		}

		a.wg.Add(1)
		go func(items []*accumulatorItem) {
			defer a.wg.Done()
			a.flush(items)
		}(batch)
	}
}

func (a *accumulator) runLinger(m AccumulatorModeLinger) {
	lingerDuration := time.Duration(m.LingerMs) * time.Millisecond
	maxSize := m.MaxSize
	if maxSize <= 0 {
		maxSize = 100
	}

	for {
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

func (a *accumulator) flush(items []*accumulatorItem) {
	msgs := make([]fibp.EnqueueMessageReq, len(items))
	for i, item := range items {
		msgs[i] = fibp.EnqueueMessageReq{
			Queue:   item.msg.Queue,
			Headers: item.msg.Headers,
			Payload: item.msg.Payload,
		}
	}

	body, err := fibp.EncodeEnqueue(msgs)
	if err != nil {
		for _, item := range items {
			item.done <- accumulatorResult{err: fmt.Errorf("encode enqueue: %w", err)}
		}
		return
	}

	// Use a background context so that one caller's cancellation does not
	// cascade to the entire batch. Individual callers will time out via
	// their own done channel select.
	_, respBody, err := a.conn.request(context.Background(), fibp.OpcodeEnqueue, body)
	if err != nil {
		for _, item := range items {
			item.done <- accumulatorResult{err: err}
		}
		return
	}

	results, err := fibp.DecodeEnqueueResult(respBody)
	if err != nil {
		for _, item := range items {
			item.done <- accumulatorResult{err: fmt.Errorf("decode enqueue result: %w", err)}
		}
		return
	}

	for i, item := range items {
		if i < len(results) {
			r := results[i]
			if r.ErrorCode != fibp.ErrorOk {
				item.done <- accumulatorResult{err: errorCodeToItemError(r.ErrorCode)}
			} else {
				item.done <- accumulatorResult{messageID: r.MessageID}
			}
		} else {
			item.done <- accumulatorResult{err: fmt.Errorf("enqueue: server returned fewer results than messages sent")}
		}
	}
}

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

func (a *accumulator) drain() {
	close(a.stopCh)
	for {
		select {
		case item := <-a.ch:
			a.flush([]*accumulatorItem{item})
		default:
			a.wg.Wait()
			return
		}
	}
}
