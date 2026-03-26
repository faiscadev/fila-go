package fila

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// Ack acknowledges a successfully processed message.
//
// The message is permanently removed from the queue.
func (c *Client) Ack(ctx context.Context, queue string, msgID string) error {
	payload := encodeAckRequest(queue, msgID)

	resp, err := c.conn.send(0, opAck, payload)
	if err != nil {
		return err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return &ProtocolError{Code: code, Message: msg}
	}
	return decodeAckResponse(resp.payload)
}

// encodeAckRequest builds the FIBP ack request payload.
//
// Wire format: item_count:u16 | items...
// Each item: queue_len:u16 | queue:utf8 | msg_id_len:u16 | msg_id:utf8
func encodeAckRequest(queue, msgID string) []byte {
	var buf bytes.Buffer
	writeU16(&buf, 1) // item_count
	writeU16(&buf, uint16(len(queue)))
	buf.WriteString(queue)
	writeU16(&buf, uint16(len(msgID)))
	buf.WriteString(msgID)
	return buf.Bytes()
}

// decodeAckResponse parses the FIBP ack response.
//
// Wire format:
//
//	item_count:u16
//	for each result:
//	  ok:u8 (0=err, 1=ok)
//	  if err: err_code:u16 | err_len:u16 | err_msg:utf8
func decodeAckResponse(payload []byte) error {
	if len(payload) < 2 {
		return fmt.Errorf("ack response: payload too short")
	}
	count := int(binary.BigEndian.Uint16(payload[0:2]))
	if count == 0 {
		return nil
	}
	pos := 2
	if pos >= len(payload) {
		return fmt.Errorf("ack response: truncated")
	}
	ok := payload[pos]
	pos++
	if ok == 1 {
		return nil
	}
	if pos+4 > len(payload) {
		return fmt.Errorf("ack response: truncated error")
	}
	errCode := binary.BigEndian.Uint16(payload[pos : pos+2])
	errLen := int(binary.BigEndian.Uint16(payload[pos+2 : pos+4]))
	pos += 4
	if pos+errLen > len(payload) {
		return fmt.Errorf("ack response: truncated error message")
	}
	errMsg := string(payload[pos : pos+errLen])
	return decodeAckItemError(errCode, errMsg)
}

// decodeAckItemError converts a wire ack error code into an ItemError.
func decodeAckItemError(code uint16, msg string) *ItemError {
	ie := &ItemError{Message: msg}
	switch code {
	case 1: // MESSAGE_NOT_FOUND
		ie.Code = "message_not_found"
		ie.cause = ErrMessageNotFound
	case 2: // STORAGE
		ie.Code = "storage"
	case 3: // PERMISSION_DENIED
		ie.Code = "permission_denied"
	default:
		ie.Code = "unspecified"
	}
	return ie
}
