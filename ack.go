package fila

import (
	"context"

	filav1 "github.com/faisca/fila-go/filav1"
)

// Ack acknowledges a successfully processed message.
//
// The message is permanently removed from the queue.
func (c *Client) Ack(ctx context.Context, queue string, msgID string) error {
	_, err := c.svc.Ack(ctx, &filav1.AckRequest{
		Queue:     queue,
		MessageId: msgID,
	})
	if err != nil {
		return mapAckError(err)
	}
	return nil
}
