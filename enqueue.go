package fila

import (
	"context"
	"fmt"

	filav1 "github.com/faisca/fila-go/filav1"
)

// Enqueue sends a message to the specified queue.
//
// Returns the broker-assigned message ID (UUIDv7).
//
// When batching is enabled (the default), the message is submitted to
// an internal accumulator that may combine it with other concurrent Enqueue
// calls into a single RPC. Use WithAccumulatorMode(AccumulatorModeDisabled{})
// to send each call as a direct RPC.
func (c *Client) Enqueue(ctx context.Context, queue string, headers map[string]string, payload []byte) (string, error) {
	if c.accumulator != nil {
		return c.accumulator.submit(ctx, EnqueueMessage{
			Queue:   queue,
			Headers: headers,
			Payload: payload,
		})
	}

	// Accumulator disabled — direct RPC.
	resp, err := c.svc.Enqueue(ctx, &filav1.EnqueueRequest{
		Messages: []*filav1.EnqueueMessage{
			{
				Queue:   queue,
				Headers: headers,
				Payload: payload,
			},
		},
	})
	if err != nil {
		return "", mapEnqueueError(err)
	}
	if len(resp.Results) == 0 {
		return "", fmt.Errorf("enqueue: server returned no results")
	}
	return parseEnqueueResult(resp.Results[0])
}

// EnqueueMany sends multiple messages in a single RPC call.
//
// Each message gets an individual result. Unlike Enqueue(), this method
// always uses a direct Enqueue RPC regardless of the accumulator mode setting.
func (c *Client) EnqueueMany(ctx context.Context, messages []EnqueueMessage) ([]EnqueueManyResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	msgs := make([]*filav1.EnqueueMessage, len(messages))
	for i, msg := range messages {
		msgs[i] = &filav1.EnqueueMessage{
			Queue:   msg.Queue,
			Headers: msg.Headers,
			Payload: msg.Payload,
		}
	}

	resp, err := c.svc.Enqueue(ctx, &filav1.EnqueueRequest{
		Messages: msgs,
	})
	if err != nil {
		return nil, mapEnqueueError(err)
	}

	results := make([]EnqueueManyResult, len(messages))
	for i := range messages {
		if i >= len(resp.Results) {
			results[i] = EnqueueManyResult{Err: fmt.Errorf("enqueue: server returned fewer results than messages sent")}
			continue
		}
		r := resp.Results[i]
		switch v := r.Result.(type) {
		case *filav1.EnqueueResult_MessageId:
			results[i] = EnqueueManyResult{MessageID: v.MessageId}
		case *filav1.EnqueueResult_Error:
			results[i] = EnqueueManyResult{Err: enqueueErrorToItemError(v.Error)}
		default:
			results[i] = EnqueueManyResult{Err: &ItemError{Message: "unknown result type"}}
		}
	}

	return results, nil
}

// parseEnqueueResult extracts a message ID or error from a single EnqueueResult.
func parseEnqueueResult(r *filav1.EnqueueResult) (string, error) {
	switch v := r.Result.(type) {
	case *filav1.EnqueueResult_MessageId:
		return v.MessageId, nil
	case *filav1.EnqueueResult_Error:
		return "", enqueueErrorToItemError(v.Error)
	default:
		return "", fmt.Errorf("enqueue: unknown result type")
	}
}

// enqueueErrorToItemError converts a proto EnqueueError to an ItemError.
func enqueueErrorToItemError(e *filav1.EnqueueError) *ItemError {
	if e == nil {
		return &ItemError{Message: "unknown error"}
	}
	ie := &ItemError{
		Code:    enqueueErrorCodeString(e.Code),
		Message: e.Message,
	}
	switch e.Code {
	case filav1.EnqueueErrorCode_ENQUEUE_ERROR_CODE_QUEUE_NOT_FOUND:
		ie.cause = ErrQueueNotFound
	}
	return ie
}

func enqueueErrorCodeString(code filav1.EnqueueErrorCode) string {
	switch code {
	case filav1.EnqueueErrorCode_ENQUEUE_ERROR_CODE_QUEUE_NOT_FOUND:
		return "queue_not_found"
	case filav1.EnqueueErrorCode_ENQUEUE_ERROR_CODE_STORAGE:
		return "storage"
	case filav1.EnqueueErrorCode_ENQUEUE_ERROR_CODE_LUA:
		return "lua"
	case filav1.EnqueueErrorCode_ENQUEUE_ERROR_CODE_PERMISSION_DENIED:
		return "permission_denied"
	default:
		return "unspecified"
	}
}
