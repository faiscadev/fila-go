package fila

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// Nack negatively acknowledges a message that failed processing.
//
// The message is requeued for retry or routed to the dead-letter queue
// based on the queue's on_failure Lua hook configuration.
func (c *Client) Nack(ctx context.Context, queue string, msgID string, errMsg string) error {
	payload := encodeNackRequest(queue, msgID, errMsg)

	resp, err := c.conn.send(ctx, 0, opNack, payload)
	if err != nil {
		return err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return &ProtocolError{Code: code, Message: msg}
	}
	return decodeNackResponse(resp.payload)
}

// encodeNackRequest builds the FIBP nack request payload.
//
// Wire format: item_count:u16 | items...
// Each item: queue_len:u16 | queue:utf8 | msg_id_len:u16 | msg_id:utf8 | err_len:u16 | err_msg:utf8
func encodeNackRequest(queue, msgID, errMsg string) []byte {
	var buf bytes.Buffer
	writeU16(&buf, 1) // item_count
	writeU16(&buf, uint16(len(queue)))
	buf.WriteString(queue)
	writeU16(&buf, uint16(len(msgID)))
	buf.WriteString(msgID)
	writeU16(&buf, uint16(len(errMsg)))
	buf.WriteString(errMsg)
	return buf.Bytes()
}

// decodeNackResponse parses the FIBP nack response.
//
// Wire format:
//
//	item_count:u16
//	for each result:
//	  ok:u8 (0=err, 1=ok)
//	  if err: err_code:u16 | err_len:u16 | err_msg:utf8
func decodeNackResponse(payload []byte) error {
	if len(payload) < 2 {
		return fmt.Errorf("nack response: payload too short")
	}
	count := int(binary.BigEndian.Uint16(payload[0:2]))
	if count == 0 {
		return nil
	}
	pos := 2
	if pos >= len(payload) {
		return fmt.Errorf("nack response: truncated")
	}
	ok := payload[pos]
	pos++
	if ok == 1 {
		return nil
	}
	if pos+4 > len(payload) {
		return fmt.Errorf("nack response: truncated error")
	}
	errCode := binary.BigEndian.Uint16(payload[pos : pos+2])
	errLen := int(binary.BigEndian.Uint16(payload[pos+2 : pos+4]))
	pos += 4
	if pos+errLen > len(payload) {
		return fmt.Errorf("nack response: truncated error message")
	}
	msg := string(payload[pos : pos+errLen])
	return decodeNackItemError(errCode, msg)
}

// decodeNackItemError converts a wire nack error code into an ItemError.
func decodeNackItemError(code uint16, msg string) *ItemError {
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
