package fila

import (
	"context"

	filav1 "github.com/faisca/fila-go/filav1"
)

// ConsumeMessage represents a message received from the broker.
type ConsumeMessage struct {
	ID           string
	Headers      map[string]string
	Payload      []byte
	FairnessKey  string
	AttemptCount uint32
	Queue        string
}

// Consume opens a streaming consumer on the specified queue.
//
// Returns a receive-only channel that delivers messages as they become
// available. The channel is closed when the server stream ends, the context
// is cancelled, or a stream error occurs.
func (c *Client) Consume(ctx context.Context, queue string) (<-chan *ConsumeMessage, error) {
	stream, err := c.svc.Consume(ctx, &filav1.ConsumeRequest{
		Queue: queue,
	})
	if err != nil {
		return nil, mapConsumeError(err)
	}

	ch := make(chan *ConsumeMessage, 1)
	go func() {
		defer close(ch)
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			msg := resp.Message
			if msg == nil {
				// Server keepalive frame — skip.
				continue
			}
			metadata := msg.Metadata
			if metadata == nil {
				metadata = &filav1.MessageMetadata{}
			}
			cm := &ConsumeMessage{
				ID:           msg.Id,
				Headers:      msg.Headers,
				Payload:      msg.Payload,
				FairnessKey:  metadata.FairnessKey,
				AttemptCount: metadata.AttemptCount,
				Queue:        metadata.QueueId,
			}
			select {
			case ch <- cm:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
