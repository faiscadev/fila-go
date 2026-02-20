package fila

import (
	"context"

	filav1 "github.com/faisca/fila-go/filav1"
)

// Enqueue sends a message to the specified queue.
//
// Returns the broker-assigned message ID (UUIDv7).
func (c *Client) Enqueue(ctx context.Context, queue string, headers map[string]string, payload []byte) (string, error) {
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
