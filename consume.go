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

	// The server may accept the stream but send UNAVAILABLE on the first
	// recv (common for server-streaming RPCs). Do a first recv to check.
	resp, err := stream.Recv()
	if err != nil {
		// On UNAVAILABLE, check trailing metadata for a leader hint.
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
			if leaderAddr := extractLeaderHintFromTrailer(stream); leaderAddr != "" {
				return c.consumeViaLeader(ctx, queue, leaderAddr)
			}
		}
		return nil, mapConsumeError(err)
	}

	ch := make(chan *ConsumeMessage, 1)

	// Send the first message (if it was a real message, not a keepalive).
	if msg := convertMessage(resp); msg != nil {
		select {
		case ch <- msg:
		case <-ctx.Done():
			close(ch)
			return ch, nil
		}
	}

	go func() {
		defer close(ch)
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			msg := convertMessage(resp)
			if msg == nil {
				continue
			}
			select {
			case ch <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// consumeViaLeader dials the leader address and opens a consume stream.
// The temporary connection is closed when the stream ends.
func (c *Client) consumeViaLeader(ctx context.Context, queue, leaderAddr string) (<-chan *ConsumeMessage, error) {
	leaderClient, err := Dial(leaderAddr, c.opts...)
	if err != nil {
		return nil, err
	}

	stream, err := leaderClient.svc.Consume(ctx, &filav1.ConsumeRequest{
		Queue: queue,
	})
	if err != nil {
		leaderClient.Close()
		return nil, mapConsumeError(err)
	}

	ch := make(chan *ConsumeMessage, 1)
	go func() {
		defer close(ch)
		defer leaderClient.Close()
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			msg := convertMessage(resp)
			if msg == nil {
				continue
			}
			select {
			case ch <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
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

// convertMessage converts a proto ConsumeResponse to a ConsumeMessage.
// Returns nil for keepalive frames (nil message).
func convertMessage(resp *filav1.ConsumeResponse) *ConsumeMessage {
	msg := resp.Message
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
