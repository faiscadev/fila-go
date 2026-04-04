package fila

import (
	"context"
	"errors"

	"github.com/faisca/fila-go/fibp"
)

// ConsumeMessage represents a message received from the broker.
type ConsumeMessage struct {
	ID           string
	Headers      map[string]string
	Payload      []byte
	FairnessKey  string
	Weight       uint32
	ThrottleKeys []string
	AttemptCount uint32
	Queue        string
	EnqueuedAt   uint64
	LeasedAt     uint64
}

// Consume opens a streaming consumer on the specified queue.
//
// Returns a receive-only channel that delivers messages as they become
// available. The channel is closed when the context is cancelled, the
// connection is lost, or the server closes the subscription.
//
// If the server returns NotLeader with a leader hint, the client
// transparently reconnects to the indicated leader and retries once.
func (c *Client) Consume(ctx context.Context, queue string) (<-chan *ConsumeMessage, error) {
	reqID, _, deliveryCh, err := c.conn.subscribe(ctx, queue)
	if err != nil {
		// Check for NotLeader with leader hint — transparently redirect.
		var pe *ProtocolError
		if errors.As(err, &pe) && pe.Code == fibp.ErrorNotLeader {
			leaderAddr := pe.LeaderAddr()
			if leaderAddr != "" {
				return c.consumeViaLeader(ctx, queue, leaderAddr)
			}
		}
		return nil, err
	}

	ch := make(chan *ConsumeMessage, 64)
	go func() {
		defer close(ch)
		defer c.conn.cancelConsume(reqID)
		for {
			select {
			case msg, ok := <-deliveryCh:
				if !ok {
					return
				}
				cm := deliveryToConsumeMessage(msg)
				select {
				case ch <- cm:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// consumeViaLeader creates a temporary connection to the leader and pumps
// messages through a channel. Does not recurse — a single redirect attempt.
func (c *Client) consumeViaLeader(ctx context.Context, queue, leaderAddr string) (<-chan *ConsumeMessage, error) {
	leaderClient, err := Dial(leaderAddr, c.opts...)
	if err != nil {
		return nil, err
	}

	// Subscribe directly on the leader connection without going through
	// Consume (which would retry the redirect). This bounds recursion to 1.
	reqID, _, deliveryCh, err := leaderClient.conn.subscribe(ctx, queue)
	if err != nil {
		leaderClient.Close()
		return nil, err
	}

	innerCh := make(chan *ConsumeMessage, 64)
	go func() {
		defer close(innerCh)
		defer leaderClient.conn.cancelConsume(reqID)
		for {
			select {
			case msg, ok := <-deliveryCh:
				if !ok {
					return
				}
				cm := deliveryToConsumeMessage(msg)
				select {
				case innerCh <- cm:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	ch := make(chan *ConsumeMessage, 64)
	go func() {
		defer close(ch)
		defer leaderClient.Close()
		for {
			select {
			case msg, ok := <-innerCh:
				if !ok {
					return
				}
				select {
				case ch <- msg:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func deliveryToConsumeMessage(msg *fibp.DeliveryMessage) *ConsumeMessage {
	return &ConsumeMessage{
		ID:           msg.MessageID,
		Headers:      msg.Headers,
		Payload:      msg.Payload,
		FairnessKey:  msg.FairnessKey,
		Weight:       msg.Weight,
		ThrottleKeys: msg.ThrottleKeys,
		AttemptCount: msg.AttemptCount,
		Queue:        msg.Queue,
		EnqueuedAt:   msg.EnqueuedAt,
		LeasedAt:     msg.LeasedAt,
	}
}
