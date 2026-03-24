package fila

import (
	"context"

	filav1 "github.com/faisca/fila-go/filav1"
)

// Enqueue sends a message to the specified queue.
//
// Returns the broker-assigned message ID (UUIDv7).
//
// When batching is enabled (the default), the message is submitted to
// an internal batcher that may combine it with other concurrent Enqueue
// calls into a single BatchEnqueue RPC. Use WithBatchMode(BatchModeDisabled{})
// to send each call as a direct RPC.
func (c *Client) Enqueue(ctx context.Context, queue string, headers map[string]string, payload []byte) (string, error) {
	if c.batcher != nil {
		return c.batcher.submit(ctx, EnqueueMessage{
			Queue:   queue,
			Headers: headers,
			Payload: payload,
		})
	}

	// Batching disabled — direct RPC.
	resp, err := c.svc.Enqueue(ctx, &filav1.EnqueueRequest{
		Queue:   queue,
		Headers: headers,
		Payload: payload,
	})
	if err != nil {
		return "", mapEnqueueError(err)
	}
	return resp.MessageId, nil
}

// BatchEnqueue sends multiple messages in a single RPC call.
//
// Each message gets an individual result. Unlike Enqueue(), this method
// always uses the BatchEnqueue RPC regardless of the batch mode setting.
func (c *Client) BatchEnqueue(ctx context.Context, messages []EnqueueMessage) ([]BatchEnqueueResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	reqs := make([]*filav1.EnqueueRequest, len(messages))
	for i, msg := range messages {
		reqs[i] = &filav1.EnqueueRequest{
			Queue:   msg.Queue,
			Headers: msg.Headers,
			Payload: msg.Payload,
		}
	}

	resp, err := c.svc.BatchEnqueue(ctx, &filav1.BatchEnqueueRequest{
		Messages: reqs,
	})
	if err != nil {
		return nil, mapBatchEnqueueError(err)
	}

	results := make([]BatchEnqueueResult, len(resp.Results))
	for i, r := range resp.Results {
		switch v := r.Result.(type) {
		case *filav1.BatchEnqueueResult_Success:
			results[i] = BatchEnqueueResult{MessageID: v.Success.MessageId}
		case *filav1.BatchEnqueueResult_Error:
			results[i] = BatchEnqueueResult{Err: &BatchItemError{Message: v.Error}}
		default:
			results[i] = BatchEnqueueResult{Err: &BatchItemError{Message: "unknown result type"}}
		}
	}

	return results, nil
}
