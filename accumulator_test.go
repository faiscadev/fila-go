package fila_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fila "github.com/faisca/fila-go"
)

func TestEnqueueManyExplicit(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-enqueue-many"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messages := []fila.EnqueueMessage{
		{Queue: queueName, Headers: map[string]string{"index": "0"}, Payload: []byte("msg-0")},
		{Queue: queueName, Headers: map[string]string{"index": "1"}, Payload: []byte("msg-1")},
		{Queue: queueName, Headers: map[string]string{"index": "2"}, Payload: []byte("msg-2")},
	}

	results, err := client.EnqueueMany(ctx, messages)
	if err != nil {
		t.Fatalf("enqueue many failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	for i, r := range results {
		if r.Err != nil {
			t.Errorf("result %d: unexpected error: %v", i, r.Err)
		}
		if r.MessageID == "" {
			t.Errorf("result %d: expected non-empty message ID", i)
		}
	}

	// Consume the messages and verify they arrived.
	ch, err := client.Consume(ctx, queueName)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	received := 0
	for received < 3 {
		select {
		case msg := <-ch:
			if msg == nil {
				t.Fatal("channel closed prematurely")
			}
			_ = client.Ack(ctx, queueName, msg.ID)
			received++
		case <-ctx.Done():
			t.Fatalf("timeout waiting for messages, received %d/3", received)
		}
	}
}

func TestEnqueueManyEmpty(t *testing.T) {
	ts := startTestServer(t)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results, err := client.EnqueueMany(ctx, nil)
	if err != nil {
		t.Fatalf("enqueue many with nil should not error: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty input, got %v", results)
	}

	results, err = client.EnqueueMany(ctx, []fila.EnqueueMessage{})
	if err != nil {
		t.Fatalf("enqueue many with empty slice should not error: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty input, got %v", results)
	}
}

func TestEnqueueManyPartialFailure(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-enqueue-many-partial"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Mix valid and invalid queue names to test partial failure.
	messages := []fila.EnqueueMessage{
		{Queue: queueName, Payload: []byte("good-msg")},
		{Queue: "nonexistent-queue", Payload: []byte("bad-msg")},
	}

	results, err := client.EnqueueMany(ctx, messages)
	if err != nil {
		t.Fatalf("enqueue many failed at RPC level: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// First message should succeed.
	if results[0].Err != nil {
		t.Errorf("result 0: expected success, got error: %v", results[0].Err)
	}
	if results[0].MessageID == "" {
		t.Error("result 0: expected non-empty message ID")
	}

	// Second message should fail with queue not found.
	if results[1].Err == nil {
		t.Error("result 1: expected error for nonexistent queue")
	}
}

func TestEnqueueWithAccumulatorDisabled(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-accumulator-disabled"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr, fila.WithAccumulatorMode(fila.AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue should work as a direct RPC.
	msgID, err := client.Enqueue(ctx, queueName, nil, []byte("direct-rpc"))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}

	// Non-existent queue should return ErrQueueNotFound.
	_, err = client.Enqueue(ctx, "nonexistent", nil, []byte("fail"))
	if err == nil {
		t.Fatal("expected error for nonexistent queue")
	}
	if !errors.Is(err, fila.ErrQueueNotFound) {
		t.Errorf("expected ErrQueueNotFound, got: %v", err)
	}
}

func TestEnqueueWithAccumulatorAuto(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-accumulator-auto"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr, fila.WithAccumulatorMode(fila.AccumulatorModeAuto{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Single enqueue through accumulator should work.
	msgID, err := client.Enqueue(ctx, queueName, nil, []byte("auto-accumulate"))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}
}

func TestEnqueueWithAccumulatorLinger(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-accumulator-linger"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr, fila.WithAccumulatorMode(fila.AccumulatorModeLinger{
		LingerMs: 50,
		MaxSize:  10,
	}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgID, err := client.Enqueue(ctx, queueName, nil, []byte("linger-accumulate"))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}
}

func TestEnqueueConcurrentAccumulation(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-concurrent-accumulate"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr, fila.WithAccumulatorMode(fila.AccumulatorModeAuto{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numMessages = 50
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errCount atomic.Int32

	// Send many messages concurrently to exercise accumulation.
	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgID, err := client.Enqueue(ctx, queueName, nil, []byte("concurrent-msg"))
			if err != nil {
				errCount.Add(1)
				return
			}
			if msgID != "" {
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("got %d errors during concurrent enqueue", errCount.Load())
	}
	if successCount.Load() != numMessages {
		t.Errorf("expected %d successful enqueues, got %d", numMessages, successCount.Load())
	}
}

func TestEnqueueDefaultAccumulatorMode(t *testing.T) {
	// Default Dial() should use AccumulatorModeAuto (not panic or error).
	ts := startTestServer(t)
	queueName := "test-default-accumulator"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgID, err := client.Enqueue(ctx, queueName, nil, []byte("default-mode"))
	if err != nil {
		t.Fatalf("enqueue with default accumulator mode failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}
}

func TestCloseFlushesPendingMessages(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-close-flush"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr, fila.WithAccumulatorMode(fila.AccumulatorModeLinger{
		LingerMs: 5000, // Very long linger to ensure message is pending.
		MaxSize:  1000,
	}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send a message that will be lingering in the accumulator.
	var enqErr error
	var msgID string
	done := make(chan struct{})
	go func() {
		defer close(done)
		msgID, enqErr = client.Enqueue(ctx, queueName, nil, []byte("flush-test"))
	}()

	// Give the goroutine a moment to submit, then close the client.
	time.Sleep(100 * time.Millisecond)
	if err := client.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	<-done

	if enqErr != nil {
		t.Fatalf("enqueue should have been flushed on close: %v", enqErr)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID after flush")
	}
}

func TestEnqueueManyNonexistentQueue(t *testing.T) {
	ts := startTestServer(t)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// All messages to a nonexistent queue: depending on server behavior,
	// the RPC may return per-item errors or a top-level error.
	messages := []fila.EnqueueMessage{
		{Queue: "nonexistent-enqueue-many-queue", Payload: []byte("msg1")},
	}

	results, err := client.EnqueueMany(ctx, messages)
	if err != nil {
		// Top-level RPC error is acceptable.
		return
	}

	// If the server returned per-item results, check them.
	if len(results) > 0 && results[0].Err == nil {
		t.Error("expected error for nonexistent queue in result")
	}
}
