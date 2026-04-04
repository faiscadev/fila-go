package fila

import (
	"context"
	"fmt"

	"github.com/faisca/fila-go/fibp"
)

// Ack acknowledges a successfully processed message.
//
// The message is permanently removed from the queue.
func (c *Client) Ack(ctx context.Context, queue string, msgID string) error {
	results, err := c.ackRaw(ctx, []fibp.AckItem{
		{Queue: queue, MessageID: msgID},
	})
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return fmt.Errorf("ack: server returned no results")
	}
	if results[0].ErrorCode != fibp.ErrorOk {
		return errorCodeToItemError(results[0].ErrorCode)
	}
	return nil
}

// AckMany acknowledges multiple messages in a single request.
func (c *Client) AckMany(ctx context.Context, items []AckItem) ([]AckManyResult, error) {
	if len(items) == 0 {
		return nil, nil
	}
	fibpItems := make([]fibp.AckItem, len(items))
	for i, item := range items {
		fibpItems[i] = fibp.AckItem{Queue: item.Queue, MessageID: item.MessageID}
	}
	results, err := c.ackRaw(ctx, fibpItems)
	if err != nil {
		return nil, err
	}
	out := make([]AckManyResult, len(items))
	for i := range items {
		if i >= len(results) {
			out[i] = AckManyResult{Err: fmt.Errorf("ack: server returned fewer results")}
			continue
		}
		if results[i].ErrorCode != fibp.ErrorOk {
			out[i] = AckManyResult{Err: errorCodeToItemError(results[i].ErrorCode)}
		}
	}
	return out, nil
}

func (c *Client) ackRaw(ctx context.Context, items []fibp.AckItem) ([]fibp.AckResultItem, error) {
	body, err := fibp.EncodeAck(items)
	if err != nil {
		return nil, fmt.Errorf("encode ack: %w", err)
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeAck, body)
	if err != nil {
		return nil, err
	}
	return fibp.DecodeAckResult(respBody)
}

// AckItem is a single item for AckMany.
type AckItem struct {
	Queue     string
	MessageID string
}

// AckManyResult holds the result of a single ack in an AckMany call.
type AckManyResult struct {
	Err error
}
