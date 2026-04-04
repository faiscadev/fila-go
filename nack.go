package fila

import (
	"context"
	"fmt"

	"github.com/faisca/fila-go/fibp"
)

// Nack negatively acknowledges a message that failed processing.
//
// The message is requeued for retry or routed to the dead-letter queue
// based on the queue's on_failure Lua hook configuration.
func (c *Client) Nack(ctx context.Context, queue string, msgID string, errMsg string) error {
	results, err := c.nackRaw(ctx, []fibp.NackItem{
		{Queue: queue, MessageID: msgID, Error: errMsg},
	})
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return fmt.Errorf("nack: server returned no results")
	}
	if results[0].ErrorCode != fibp.ErrorOk {
		return errorCodeToItemError(results[0].ErrorCode)
	}
	return nil
}

// NackMany negatively acknowledges multiple messages in a single request.
func (c *Client) NackMany(ctx context.Context, items []NackItem) ([]NackManyResult, error) {
	if len(items) == 0 {
		return nil, nil
	}
	fibpItems := make([]fibp.NackItem, len(items))
	for i, item := range items {
		fibpItems[i] = fibp.NackItem{Queue: item.Queue, MessageID: item.MessageID, Error: item.Error}
	}
	results, err := c.nackRaw(ctx, fibpItems)
	if err != nil {
		return nil, err
	}
	out := make([]NackManyResult, len(items))
	for i := range items {
		if i >= len(results) {
			out[i] = NackManyResult{Err: fmt.Errorf("nack: server returned fewer results")}
			continue
		}
		if results[i].ErrorCode != fibp.ErrorOk {
			out[i] = NackManyResult{Err: errorCodeToItemError(results[i].ErrorCode)}
		}
	}
	return out, nil
}

func (c *Client) nackRaw(ctx context.Context, items []fibp.NackItem) ([]fibp.NackResultItem, error) {
	body, err := fibp.EncodeNack(items)
	if err != nil {
		return nil, fmt.Errorf("encode nack: %w", err)
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeNack, body)
	if err != nil {
		return nil, err
	}
	return fibp.DecodeNackResult(respBody)
}

// NackItem is a single item for NackMany.
type NackItem struct {
	Queue     string
	MessageID string
	Error     string
}

// NackManyResult holds the result of a single nack in a NackMany call.
type NackManyResult struct {
	Err error
}
