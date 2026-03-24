package fila

import (
	"context"

	filav1 "github.com/faisca/fila-go/filav1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// leaderHintKey is the gRPC metadata key the server uses to indicate the
// current leader's client address when returning UNAVAILABLE.
const leaderHintKey = "x-fila-leader-addr"

// Consume opens a streaming consumer on the specified queue.
//
// Returns a receive-only channel that delivers messages as they become
// available. The channel is closed when the server stream ends, the context
// is cancelled, or a stream error occurs.
//
// The consumer transparently unpacks batched delivery: when the server
// sends multiple messages in a single ConsumeResponse (via the repeated
// messages field), each message is delivered individually on the channel.
//
// If the server returns UNAVAILABLE with a leader hint (x-fila-leader-addr
// metadata), the client transparently reconnects to the indicated leader and
// retries once. At most one redirect is attempted per Consume call.
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
				// On UNAVAILABLE, check trailing metadata for a leader hint
				// and transparently reconnect once.
				if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
					if leaderAddr := extractLeaderHintFromTrailer(stream); leaderAddr != "" {
						c.consumeViaLeaderInto(ctx, queue, leaderAddr, ch)
						return
					}
				}
				return
			}
			if !sendMessages(ctx, ch, resp) {
				return
			}
		}
	}()

	return ch, nil
}

// consumeViaLeaderInto dials the leader address and pumps messages into the
// provided channel. The temporary connection is closed when the stream ends.
// This is called from within the goroutine, so it must not spawn another one.
func (c *Client) consumeViaLeaderInto(ctx context.Context, queue, leaderAddr string, ch chan *ConsumeMessage) {
	leaderClient, err := Dial(leaderAddr, c.opts...)
	if err != nil {
		return
	}
	defer leaderClient.Close()

	stream, err := leaderClient.svc.Consume(ctx, &filav1.ConsumeRequest{
		Queue: queue,
	})
	if err != nil {
		return
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return
		}
		if !sendMessages(ctx, ch, resp) {
			return
		}
	}
}

// sendMessages extracts messages from a ConsumeResponse and sends them on
// the channel. It handles both the singular message field (backward compat)
// and the repeated messages field (batched delivery). Returns false if the
// context was cancelled and the caller should stop.
func sendMessages(ctx context.Context, ch chan *ConsumeMessage, resp *filav1.ConsumeResponse) bool {
	msgs := extractMessages(resp)
	for _, msg := range msgs {
		select {
		case ch <- msg:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// extractMessages unpacks a ConsumeResponse into individual ConsumeMessages.
// If the repeated messages field is populated, those are used. Otherwise,
// falls back to the singular message field for backward compatibility.
// Returns nil for keepalive frames (no messages).
func extractMessages(resp *filav1.ConsumeResponse) []*ConsumeMessage {
	// Prefer the repeated messages field (batched delivery).
	if len(resp.Messages) > 0 {
		result := make([]*ConsumeMessage, 0, len(resp.Messages))
		for _, msg := range resp.Messages {
			cm := protoToConsumeMessage(msg)
			if cm != nil {
				result = append(result, cm)
			}
		}
		return result
	}

	// Fall back to singular message field (backward compatible).
	cm := protoToConsumeMessage(resp.Message)
	if cm == nil {
		return nil
	}
	return []*ConsumeMessage{cm}
}

// extractLeaderHintFromTrailer reads the leader address from the stream's
// trailing metadata after a failed Recv.
func extractLeaderHintFromTrailer(stream filav1.FilaService_ConsumeClient) string {
	md := stream.Trailer()
	vals := md.Get(leaderHintKey)
	if len(vals) > 0 && vals[0] != "" {
		return vals[0]
	}
	return ""
}

// protoToConsumeMessage converts a proto Message to a ConsumeMessage.
// Returns nil if msg is nil (keepalive frame).
func protoToConsumeMessage(msg *filav1.Message) *ConsumeMessage {
	if msg == nil {
		return nil
	}
	metadata := msg.Metadata
	if metadata == nil {
		metadata = &filav1.MessageMetadata{}
	}
	return &ConsumeMessage{
		ID:           msg.Id,
		Headers:      msg.Headers,
		Payload:      msg.Payload,
		FairnessKey:  metadata.FairnessKey,
		AttemptCount: metadata.AttemptCount,
		Queue:        metadata.QueueId,
	}
}
