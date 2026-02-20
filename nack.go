package fila

import (
	"context"

	filav1 "github.com/faisca/fila-go/filav1"
)

// Nack negatively acknowledges a message that failed processing.
//
// The message is requeued for retry or routed to the dead-letter queue
// based on the queue's on_failure Lua hook configuration.
func (c *Client) Nack(ctx context.Context, queue string, msgID string, errMsg string) error {
	_, err := c.svc.Nack(ctx, &filav1.NackRequest{
		Queue:     queue,
		MessageId: msgID,
		Error:     errMsg,
	})
	if err != nil {
		return mapNackError(err)
	}
	return nil
}
