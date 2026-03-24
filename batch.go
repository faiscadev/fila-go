package fila

import (
	"context"
	"sync"
	"time"

	filav1 "github.com/faisca/fila-go/filav1"
)

// BatchMode controls how Enqueue() batches messages internally.
type BatchMode interface {
	isBatchMode()
}

// BatchModeAuto enables opportunistic batching. A background goroutine
// collects enqueue requests: it blocks waiting for the first message, then
// non-blocking drains any additional messages that are already waiting.
// At low load each message is sent individually; at high load messages
// naturally cluster into batches. Zero configuration, zero latency penalty.
//
// This is the default mode.
type BatchModeAuto struct{}

func (BatchModeAuto) isBatchMode() {}

// BatchModeLinger enables timer-based forced batching. Messages are
// accumulated until either LingerMs milliseconds have elapsed since the
// first message in the batch, or BatchSize messages have been collected,
// whichever comes first.
type BatchModeLinger struct {
	LingerMs  int
	BatchSize int
}

func (BatchModeLinger) isBatchMode() {}

// BatchModeDisabled disables batching entirely. Each Enqueue() call makes
// a direct gRPC RPC.
type BatchModeDisabled struct{}

func (BatchModeDisabled) isBatchMode() {}

// EnqueueMessage describes a message to be enqueued.
type EnqueueMessage struct {
	Queue   string
	Headers map[string]string
	Payload []byte
}

// BatchEnqueueResult holds the result of a single message in a batch enqueue.
type BatchEnqueueResult struct {
	MessageID string
	Err       error
}

// batchItem is an internal request sent to the batcher goroutine.
type batchItem struct {
	ctx  context.Context
	msg  EnqueueMessage
	done chan batchResult
}

type batchResult struct {
	messageID string
	err       error
}

// batcher manages the background goroutine that collects and flushes
// enqueue requests according to the configured BatchMode.
type batcher struct {
	svc  filav1.FilaServiceClient
	mode BatchMode
	ch   chan *batchItem

	wg     sync.WaitGroup
	stopCh chan struct{}
}

func newBatcher(svc filav1.FilaServiceClient, mode BatchMode) *batcher {
	b := &batcher{
		svc:    svc,
		mode:   mode,
		ch:     make(chan *batchItem, 4096),
		stopCh: make(chan struct{}),
	}
	b.wg.Add(1)
	go b.run()
	return b
}

func (b *batcher) run() {
	defer b.wg.Done()
	switch m := b.mode.(type) {
	case BatchModeAuto:
		b.runAuto()
	case BatchModeLinger:
		b.runLinger(m)
	default:
		b.runAuto()
	}
}

// runAuto implements the opportunistic batching algorithm:
// block for first message, non-blocking drain of anything else waiting,
// then flush concurrently.
func (b *batcher) runAuto() {
	for {
		// Block waiting for the first item (or stop signal).
		var first *batchItem
		select {
		case first = <-b.ch:
		case <-b.stopCh:
			return
		}

		batch := []*batchItem{first}

		// Non-blocking drain of anything else already in the channel.
	drain:
		for {
			select {
			case item := <-b.ch:
				batch = append(batch, item)
			default:
				break drain
			}
		}

		// Flush concurrently so the batcher can keep collecting.
		b.wg.Add(1)
		go func(items []*batchItem) {
			defer b.wg.Done()
			b.flushBatch(items)
		}(batch)
	}
}

// runLinger implements timer-based forced batching.
func (b *batcher) runLinger(m BatchModeLinger) {
	lingerDuration := time.Duration(m.LingerMs) * time.Millisecond
	batchSize := m.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	for {
		// Block waiting for the first item.
		var first *batchItem
		select {
		case first = <-b.ch:
		case <-b.stopCh:
			return
		}

		batch := []*batchItem{first}
		timer := time.NewTimer(lingerDuration)

	collect:
		for len(batch) < batchSize {
			select {
			case item := <-b.ch:
				batch = append(batch, item)
			case <-timer.C:
				break collect
			case <-b.stopCh:
				timer.Stop()
				// Flush what we have before stopping.
				b.flushBatch(batch)
				return
			}
		}
		timer.Stop()

		b.wg.Add(1)
		go func(items []*batchItem) {
			defer b.wg.Done()
			b.flushBatch(items)
		}(batch)
	}
}

// flushBatch sends a batch of enqueue requests to the server.
// For a single message, uses the singular Enqueue RPC to preserve
// specific error types (e.g., queue-not-found). For 2+ messages,
// uses BatchEnqueue.
func (b *batcher) flushBatch(items []*batchItem) {
	if len(items) == 1 {
		b.flushSingle(items[0])
		return
	}
	b.flushMulti(items)
}

func (b *batcher) flushSingle(item *batchItem) {
	resp, err := b.svc.Enqueue(item.ctx, &filav1.EnqueueRequest{
		Queue:   item.msg.Queue,
		Headers: item.msg.Headers,
		Payload: item.msg.Payload,
	})
	if err != nil {
		item.done <- batchResult{err: mapEnqueueError(err)}
		return
	}
	item.done <- batchResult{messageID: resp.MessageId}
}

func (b *batcher) flushMulti(items []*batchItem) {
	reqs := make([]*filav1.EnqueueRequest, len(items))
	for i, item := range items {
		reqs[i] = &filav1.EnqueueRequest{
			Queue:   item.msg.Queue,
			Headers: item.msg.Headers,
			Payload: item.msg.Payload,
		}
	}

	// Use the first item's context for the batch RPC. If any individual
	// context is already cancelled the caller will see it through their
	// done channel timeout.
	resp, err := b.svc.BatchEnqueue(items[0].ctx, &filav1.BatchEnqueueRequest{
		Messages: reqs,
	})

	if err != nil {
		mappedErr := mapBatchEnqueueError(err)
		for _, item := range items {
			item.done <- batchResult{err: mappedErr}
		}
		return
	}

	results := resp.GetResults()
	for i, item := range items {
		if i < len(results) {
			r := results[i]
			switch v := r.Result.(type) {
			case *filav1.BatchEnqueueResult_Success:
				item.done <- batchResult{messageID: v.Success.MessageId}
			case *filav1.BatchEnqueueResult_Error:
				item.done <- batchResult{err: &BatchItemError{Message: v.Error}}
			default:
				item.done <- batchResult{err: &BatchItemError{Message: "unknown result type"}}
			}
		} else {
			item.done <- batchResult{err: &BatchItemError{Message: "server returned fewer results than messages sent"}}
		}
	}
}

// submit sends a message to the batcher and waits for the result.
func (b *batcher) submit(ctx context.Context, msg EnqueueMessage) (string, error) {
	item := &batchItem{
		ctx:  ctx,
		msg:  msg,
		done: make(chan batchResult, 1),
	}

	select {
	case b.ch <- item:
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
func (b *batcher) drain() {
	close(b.stopCh)
	// Drain any remaining items in the channel.
	for {
		select {
		case item := <-b.ch:
			b.flushSingle(item)
		default:
			b.wg.Wait()
			return
		}
	}
}
