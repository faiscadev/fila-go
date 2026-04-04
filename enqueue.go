package fila

import (
	"context"
	"fmt"

	"github.com/faisca/fila-go/fibp"
)

// Enqueue sends a message to the specified queue.
//
// Returns the broker-assigned message ID (UUIDv7).
//
// When batching is enabled (the default), the message is submitted to
// an internal accumulator that may combine it with other concurrent Enqueue
// calls into a single request. Use WithAccumulatorMode(AccumulatorModeDisabled{})
// to send each call directly.
func (c *Client) Enqueue(ctx context.Context, queue string, headers map[string]string, payload []byte) (string, error) {
	if c.accumulator != nil {
		return c.accumulator.submit(ctx, EnqueueMessage{
			Queue:   queue,
			Headers: headers,
			Payload: payload,
		})
	}

	results, err := c.enqueueRaw(ctx, []fibp.EnqueueMessageReq{
		{Queue: queue, Headers: headers, Payload: payload},
	})
	if err != nil {
		return "", err
	}
	if len(results) == 0 {
		return "", fmt.Errorf("enqueue: server returned no results")
	}
	return parseEnqueueResultItem(results[0])
}

// EnqueueMany sends multiple messages in a single request.
//
// Each message gets an individual result. Unlike Enqueue(), this method
// always sends directly regardless of the accumulator mode setting.
func (c *Client) EnqueueMany(ctx context.Context, messages []EnqueueMessage) ([]EnqueueManyResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	msgs := make([]fibp.EnqueueMessageReq, len(messages))
	for i, msg := range messages {
		msgs[i] = fibp.EnqueueMessageReq{
			Queue:   msg.Queue,
			Headers: msg.Headers,
			Payload: msg.Payload,
		}
	}

	results, err := c.enqueueRaw(ctx, msgs)
	if err != nil {
		return nil, err
	}

	out := make([]EnqueueManyResult, len(messages))
	for i := range messages {
		if i >= len(results) {
			out[i] = EnqueueManyResult{Err: fmt.Errorf("enqueue: server returned fewer results than messages sent")}
			continue
		}
		r := results[i]
		if r.ErrorCode != fibp.ErrorOk {
			out[i] = EnqueueManyResult{Err: errorCodeToItemError(r.ErrorCode)}
		} else {
			out[i] = EnqueueManyResult{MessageID: r.MessageID}
		}
	}

	return out, nil
}

// enqueueRaw sends an Enqueue frame and decodes the result.
func (c *Client) enqueueRaw(ctx context.Context, msgs []fibp.EnqueueMessageReq) ([]fibp.EnqueueResultItem, error) {
	body, err := fibp.EncodeEnqueue(msgs)
	if err != nil {
		return nil, fmt.Errorf("encode enqueue: %w", err)
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeEnqueue, body)
	if err != nil {
		return nil, err
	}
	return fibp.DecodeEnqueueResult(respBody)
}

// parseEnqueueResultItem extracts a message ID or error from a single result.
func parseEnqueueResultItem(r fibp.EnqueueResultItem) (string, error) {
	if r.ErrorCode != fibp.ErrorOk {
		return "", errorCodeToItemError(r.ErrorCode)
	}
	return r.MessageID, nil
}
