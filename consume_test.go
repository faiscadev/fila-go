package fila

import (
	"encoding/binary"
	"testing"
)

// buildConsumeFrame constructs a raw FIBP consume push payload for testing.
// Matches the server wire format (fila-core encode_consume_push):
// each entry: msg_id, fairness_key, attempt_count, headers, payload.
// There is no queue field in the push frame.
func buildConsumeFrame(msgs []testConsumeMsg) []byte {
	var buf []byte
	buf = appendU16(buf, uint16(len(msgs)))
	for _, m := range msgs {
		buf = appendU16(buf, uint16(len(m.id)))
		buf = append(buf, m.id...)
		buf = appendU16(buf, uint16(len(m.fairnessKey)))
		buf = append(buf, m.fairnessKey...)
		buf = appendU32(buf, m.attemptCount)
		buf = append(buf, uint8(len(m.headers)))
		for k, v := range m.headers {
			buf = appendU16(buf, uint16(len(k)))
			buf = append(buf, k...)
			buf = appendU16(buf, uint16(len(v)))
			buf = append(buf, v...)
		}
		buf = appendU32(buf, uint32(len(m.payload)))
		buf = append(buf, m.payload...)
	}
	return buf
}

type testConsumeMsg struct {
	id           string
	fairnessKey  string
	attemptCount uint32
	headers      map[string]string
	payload      []byte
}

func appendU16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}

func appendU32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func TestDecodeConsumeFrameSingle(t *testing.T) {
	raw := buildConsumeFrame([]testConsumeMsg{
		{
			id:           "msg-1",
			fairnessKey:  "key-1",
			attemptCount: 0,
			headers:      nil,
			payload:      []byte("payload-1"),
		},
	})

	msgs, err := decodeConsumeFrame(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ID != "msg-1" {
		t.Errorf("expected ID msg-1, got %s", msgs[0].ID)
	}
	if string(msgs[0].Payload) != "payload-1" {
		t.Errorf("expected payload payload-1, got %s", string(msgs[0].Payload))
	}
	if msgs[0].FairnessKey != "key-1" {
		t.Errorf("expected fairness key key-1, got %s", msgs[0].FairnessKey)
	}
}

func TestDecodeConsumeFrameMultiple(t *testing.T) {
	raw := buildConsumeFrame([]testConsumeMsg{
		{id: "msg-1", payload: []byte("payload-1")},
		{id: "msg-2", payload: []byte("payload-2")},
		{id: "msg-3", payload: []byte("payload-3")},
	})

	msgs, err := decodeConsumeFrame(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	for i, msg := range msgs {
		expectedID := "msg-" + string(rune('1'+i))
		if msg.ID != expectedID {
			t.Errorf("message %d: expected ID %s, got %s", i, expectedID, msg.ID)
		}
	}
}

func TestDecodeConsumeFrameKeepalive(t *testing.T) {
	// An empty payload (or zero-count frame) is a keepalive — not an error.
	msgs, err := decodeConsumeFrame(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil payload: %v", err)
	}
	if msgs != nil {
		t.Errorf("expected nil for empty payload, got %v", msgs)
	}

	// Zero-count frame.
	raw := []byte{0x00, 0x00}
	msgs, err = decodeConsumeFrame(raw)
	if err != nil {
		t.Fatalf("unexpected error for zero-count frame: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for zero-count frame, got %d", len(msgs))
	}
}

func TestDecodeConsumeFrameWithHeaders(t *testing.T) {
	raw := buildConsumeFrame([]testConsumeMsg{
		{
			id: "msg-hdr",
			headers: map[string]string{
				"tenant": "acme",
			},
			payload: []byte("with-headers"),
		},
	})

	msgs, err := decodeConsumeFrame(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Headers["tenant"] != "acme" {
		t.Errorf("expected header tenant=acme, got %v", msgs[0].Headers)
	}
}

func TestDecodeConsumeFrameAttemptCount(t *testing.T) {
	raw := buildConsumeFrame([]testConsumeMsg{
		{id: "msg-retry", attemptCount: 3, payload: []byte("retry")},
	})

	msgs, err := decodeConsumeFrame(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].AttemptCount != 3 {
		t.Errorf("expected attempt count 3, got %d", msgs[0].AttemptCount)
	}
}

func TestDecodeConsumeFrameTruncated(t *testing.T) {
	raw := buildConsumeFrame([]testConsumeMsg{
		{id: "msg-1", payload: []byte("data")},
	})
	// Truncate payload.
	_, err := decodeConsumeFrame(raw[:len(raw)-2])
	if err == nil {
		t.Fatal("expected error for truncated frame, got nil")
	}
}

func TestDecodeErrorPayload(t *testing.T) {
	// Build an error payload: err_code:u16 | err_len:u16 | err_msg
	msg := "queue not found"
	payload := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint16(payload[0:2], 1) // code = 1
	binary.BigEndian.PutUint16(payload[2:4], uint16(len(msg)))
	copy(payload[4:], msg)

	gotMsg, gotCode := decodeErrorPayload(payload)
	if gotCode != 1 {
		t.Errorf("expected code 1, got %d", gotCode)
	}
	if gotMsg != msg {
		t.Errorf("expected message %q, got %q", msg, gotMsg)
	}
}

func TestDecodeErrorPayloadShort(t *testing.T) {
	// Too-short payload should return a safe fallback.
	msg, code := decodeErrorPayload([]byte{0x00})
	if msg == "" {
		t.Error("expected non-empty fallback message")
	}
	_ = code
}
