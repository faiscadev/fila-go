package fila

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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

// defaultInitialCredits is the number of flow credits sent with a new
// consume request. The server uses credits to bound the number of
// in-flight messages per consumer.
const defaultInitialCredits = 256

// Consume opens a streaming consumer on the specified queue.
//
// Returns a receive-only channel that delivers messages as they become
// available. The channel is closed when the server stream ends, the context
// is cancelled, or a connection error occurs.
//
// The consumer transparently unpacks delivery batches: when the server
// sends multiple messages in a single push frame, each message is delivered
// individually on the channel.
//
// If the server returns a leader-hint error (FIBP error code for UNAVAILABLE),
// the client transparently reconnects to the indicated leader and retries once.
func (c *Client) Consume(ctx context.Context, queue string) (<-chan *ConsumeMessage, error) {
	payload := encodeConsumeRequest(queue, defaultInitialCredits)

	corrID, pushCh, err := c.conn.openStream(0, opConsume, payload)
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}

	ch := make(chan *ConsumeMessage, 16)

	go func() {
		defer close(ch)
		defer c.conn.closeStream(corrID)

		for {
			select {
			case <-ctx.Done():
				return
			case f, ok := <-pushCh:
				if !ok {
					return
				}
				if f.op == opError {
					errMsg, errCode := decodeErrorPayload(f.payload)
					if errCode == errCodeLeaderRedirect && errMsg != "" {
						c.consumeViaLeaderInto(ctx, queue, errMsg, ch)
					}
					return
				}
				msgs, err := decodeConsumeFrame(f.payload)
				if err != nil {
					return
				}
				for _, msg := range msgs {
					select {
					case ch <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ch, nil
}

// errCodeLeaderRedirect is the FIBP error code the server sends when the
// client should reconnect to a different node.
const errCodeLeaderRedirect = 0x0010

// consumeViaLeaderInto dials the leader address and pumps messages into the
// provided channel. The temporary connection is closed when the stream ends.
func (c *Client) consumeViaLeaderInto(ctx context.Context, queue, leaderAddr string, ch chan *ConsumeMessage) {
	leaderClient, err := Dial(leaderAddr, c.opts...)
	if err != nil {
		return
	}
	defer leaderClient.Close()

	payload := encodeConsumeRequest(queue, defaultInitialCredits)
	corrID, pushCh, err := leaderClient.conn.openStream(0, opConsume, payload)
	if err != nil {
		return
	}
	defer leaderClient.conn.closeStream(corrID)

	for {
		select {
		case <-ctx.Done():
			return
		case f, ok := <-pushCh:
			if !ok {
				return
			}
			if f.op == opError {
				return
			}
			msgs, err := decodeConsumeFrame(f.payload)
			if err != nil {
				return
			}
			for _, msg := range msgs {
				select {
				case ch <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// encodeConsumeRequest builds the FIBP consume request payload.
//
// Wire format: queue_len:u16 | queue:utf8 | initial_credits:u32
func encodeConsumeRequest(queue string, initialCredits uint32) []byte {
	var buf bytes.Buffer
	writeU16(&buf, uint16(len(queue)))
	buf.WriteString(queue)
	writeU32(&buf, initialCredits)
	return buf.Bytes()
}

// decodeConsumeFrame decodes a server-pushed consume delivery frame.
//
// Server wire format (matches fila-core encode_consume_push):
//
//	msg_count:u16
//	for each message:
//	  msg_id_len:u16 | msg_id:utf8
//	  fairness_key_len:u16 | fairness_key:utf8
//	  attempt_count:u32
//	  header_count:u8
//	  for each header: key_len:u16|key | val_len:u16|val
//	  payload_len:u32 | payload:bytes
//
// Note: there is no queue field in the push frame; the queue name is known
// from the consume subscription.
func decodeConsumeFrame(payload []byte) ([]*ConsumeMessage, error) {
	if len(payload) < 2 {
		// Keepalive or empty push — not an error.
		return nil, nil
	}
	count := int(binary.BigEndian.Uint16(payload[0:2]))
	if count == 0 {
		return nil, nil
	}
	pos := 2

	msgs := make([]*ConsumeMessage, 0, count)
	for i := 0; i < count; i++ {
		msg, n, err := decodeConsumeMessage(payload[pos:])
		if err != nil {
			return nil, fmt.Errorf("consume frame: message %d: %w", i, err)
		}
		pos += n
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// decodeConsumeMessage parses a single message from a consume push frame.
// Returns the message and the number of bytes consumed.
func decodeConsumeMessage(data []byte) (*ConsumeMessage, int, error) {
	pos := 0

	readStr := func(field string) (string, error) {
		if pos+2 > len(data) {
			return "", fmt.Errorf("truncated %s length", field)
		}
		l := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+l > len(data) {
			return "", fmt.Errorf("truncated %s value", field)
		}
		s := string(data[pos : pos+l])
		pos += l
		return s, nil
	}

	msgID, err := readStr("msg_id")
	if err != nil {
		return nil, 0, err
	}
	fairnessKey, err := readStr("fairness_key")
	if err != nil {
		return nil, 0, err
	}

	if pos+4 > len(data) {
		return nil, 0, fmt.Errorf("truncated attempt_count")
	}
	attemptCount := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	if pos+1 > len(data) {
		return nil, 0, fmt.Errorf("truncated header_count")
	}
	headerCount := int(data[pos])
	pos++

	headers := make(map[string]string, headerCount)
	for j := 0; j < headerCount; j++ {
		k, err := readStr(fmt.Sprintf("header[%d] key", j))
		if err != nil {
			return nil, 0, err
		}
		v, err := readStr(fmt.Sprintf("header[%d] val", j))
		if err != nil {
			return nil, 0, err
		}
		headers[k] = v
	}

	if pos+4 > len(data) {
		return nil, 0, fmt.Errorf("truncated payload_len")
	}
	payloadLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if pos+payloadLen > len(data) {
		return nil, 0, fmt.Errorf("truncated payload")
	}
	payload := make([]byte, payloadLen)
	copy(payload, data[pos:pos+payloadLen])
	pos += payloadLen

	return &ConsumeMessage{
		ID:           msgID,
		FairnessKey:  fairnessKey,
		AttemptCount: attemptCount,
		Headers:      headers,
		Payload:      payload,
	}, pos, nil
}
