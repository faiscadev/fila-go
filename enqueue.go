package fila

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// Enqueue sends a message to the specified queue.
//
// Returns the broker-assigned message ID (UUIDv7).
//
// When batching is enabled (the default), the message is submitted to
// an internal accumulator that may combine it with other concurrent Enqueue
// calls into a single request. Use WithAccumulatorMode(AccumulatorModeDisabled{})
// to send each call as a direct request.
func (c *Client) Enqueue(ctx context.Context, queue string, headers map[string]string, payload []byte) (string, error) {
	if c.accumulator != nil {
		return c.accumulator.submit(ctx, EnqueueMessage{
			Queue:   queue,
			Headers: headers,
			Payload: payload,
		})
	}

	// Accumulator disabled — direct request.
	results, err := enqueueRaw(ctx, c.conn, []EnqueueMessage{
		{Queue: queue, Headers: headers, Payload: payload},
	})
	if err != nil {
		return "", err
	}
	if len(results) == 0 {
		return "", fmt.Errorf("enqueue: server returned no results")
	}
	return results[0].MessageID, results[0].Err
}

// EnqueueMany sends multiple messages in a single request.
//
// Each message gets an individual result. Unlike Enqueue(), this method
// always uses a direct request regardless of the accumulator mode setting.
func (c *Client) EnqueueMany(ctx context.Context, messages []EnqueueMessage) ([]EnqueueManyResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	return enqueueRaw(ctx, c.conn, messages)
}

// enqueueRaw sends an ENQUEUE frame and parses the response.
//
// Wire format for request payload:
//
//	queue_len:u16 | queue:utf8 — queue name of the first message
//	msg_count:u16
//	for each message:
//	  header_count:u8
//	  for each header:
//	    key_len:u16 | key:utf8 | val_len:u16 | val:utf8
//	  payload_len:u32 | payload:bytes
//
// Because FIBP's enqueue request carries a single queue name at the front,
// all messages in one call must target the same queue. EnqueueMany with
// mixed queues is split into per-queue requests.
func enqueueRaw(ctx context.Context, c *conn, messages []EnqueueMessage) ([]EnqueueManyResult, error) {
	// Group messages by queue so each FIBP frame stays single-queue.
	type group struct {
		queue    string
		messages []EnqueueMessage
		indices  []int
	}
	var order []string
	groups := make(map[string]*group)
	for i, m := range messages {
		g, ok := groups[m.Queue]
		if !ok {
			g = &group{queue: m.Queue}
			groups[m.Queue] = g
			order = append(order, m.Queue)
		}
		g.messages = append(g.messages, m)
		g.indices = append(g.indices, i)
	}

	results := make([]EnqueueManyResult, len(messages))

	for _, q := range order {
		g := groups[q]
		payload, encErr := encodeEnqueueRequest(g.queue, g.messages)
		if encErr != nil {
			for _, idx := range g.indices {
				results[idx] = EnqueueManyResult{Err: encErr}
			}
			continue
		}

		resp, err := c.send(ctx, 0, opEnqueue, payload)
		if err != nil {
			// Propagate connection-level error to all messages in this group.
			for _, idx := range g.indices {
				results[idx] = EnqueueManyResult{Err: err}
			}
			continue
		}

		if resp.op == opError {
			msg, code := decodeErrorPayload(resp.payload)
			err = &ProtocolError{Code: code, Message: msg}
			for _, idx := range g.indices {
				results[idx] = EnqueueManyResult{Err: err}
			}
			continue
		}

		groupResults, err := decodeEnqueueResponse(resp.payload)
		if err != nil {
			for _, idx := range g.indices {
				results[idx] = EnqueueManyResult{Err: err}
			}
			continue
		}

		for j, idx := range g.indices {
			if j < len(groupResults) {
				results[idx] = groupResults[j]
			} else {
				results[idx] = EnqueueManyResult{
					Err: fmt.Errorf("enqueue: server returned fewer results than messages sent"),
				}
			}
		}
	}

	return results, nil
}

// encodeEnqueueRequest builds the FIBP enqueue request payload.
// Returns an error if any message has more than 255 headers (the wire
// format encodes header_count as a single u8).
func encodeEnqueueRequest(queue string, messages []EnqueueMessage) ([]byte, error) {
	var buf bytes.Buffer

	// queue_len:u16 | queue:utf8
	writeU16(&buf, uint16(len(queue)))
	buf.WriteString(queue)

	// msg_count:u16
	writeU16(&buf, uint16(len(messages)))

	for i, msg := range messages {
		if len(msg.Headers) > 255 {
			return nil, fmt.Errorf("enqueue: message %d has %d headers, max is 255", i, len(msg.Headers))
		}
		// header_count:u8
		buf.WriteByte(uint8(len(msg.Headers)))
		for k, v := range msg.Headers {
			writeU16(&buf, uint16(len(k)))
			buf.WriteString(k)
			writeU16(&buf, uint16(len(v)))
			buf.WriteString(v)
		}
		// payload_len:u32 | payload
		writeU32(&buf, uint32(len(msg.Payload)))
		buf.Write(msg.Payload)
	}
	return buf.Bytes(), nil
}

// decodeEnqueueResponse parses the FIBP enqueue response payload.
//
// Wire format:
//
//	msg_count:u16
//	for each result:
//	  ok:u8 (0=err, 1=ok)
//	  if ok:  msg_id_len:u16 | msg_id:utf8
//	  if err: err_code:u16 | err_len:u16 | err_msg:utf8
func decodeEnqueueResponse(payload []byte) ([]EnqueueManyResult, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("enqueue response: payload too short")
	}
	count := int(binary.BigEndian.Uint16(payload[0:2]))
	pos := 2

	results := make([]EnqueueManyResult, 0, count)
	for i := 0; i < count; i++ {
		if pos >= len(payload) {
			return nil, fmt.Errorf("enqueue response: truncated at result %d", i)
		}
		ok := payload[pos]
		pos++

		if ok == 1 {
			if pos+2 > len(payload) {
				return nil, fmt.Errorf("enqueue response: truncated msg_id_len")
			}
			idLen := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
			pos += 2
			if pos+idLen > len(payload) {
				return nil, fmt.Errorf("enqueue response: truncated msg_id")
			}
			msgID := string(payload[pos : pos+idLen])
			pos += idLen
			results = append(results, EnqueueManyResult{MessageID: msgID})
		} else {
			if pos+4 > len(payload) {
				return nil, fmt.Errorf("enqueue response: truncated error")
			}
			errCode := binary.BigEndian.Uint16(payload[pos : pos+2])
			errLen := int(binary.BigEndian.Uint16(payload[pos+2 : pos+4]))
			pos += 4
			if pos+errLen > len(payload) {
				return nil, fmt.Errorf("enqueue response: truncated error message")
			}
			errMsg := string(payload[pos : pos+errLen])
			pos += errLen
			ie := decodeEnqueueItemError(errCode, errMsg)
			results = append(results, EnqueueManyResult{Err: ie})
		}
	}
	return results, nil
}

// decodeEnqueueItemError converts a wire error code into an ItemError,
// wiring up sentinel errors where appropriate.
func decodeEnqueueItemError(code uint16, msg string) *ItemError {
	ie := &ItemError{Message: msg}
	switch code {
	case 1: // QUEUE_NOT_FOUND
		ie.Code = "queue_not_found"
		ie.cause = ErrQueueNotFound
	case 2: // STORAGE
		ie.Code = "storage"
	case 3: // LUA
		ie.Code = "lua"
	case 4: // PERMISSION_DENIED
		ie.Code = "permission_denied"
	default:
		ie.Code = "unspecified"
	}
	return ie
}

// writeU16 writes a big-endian uint16 to buf.
func writeU16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

// writeU32 writes a big-endian uint32 to buf.
func writeU32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}
