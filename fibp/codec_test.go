package fibp

import (
	"bytes"
	"testing"
)

func TestEnqueueRoundTrip(t *testing.T) {
	msgs := []EnqueueMessageReq{
		{Queue: "orders", Headers: map[string]string{"x": "1"}, Payload: []byte("hello")},
		{Queue: "events", Headers: nil, Payload: []byte("world")},
	}
	data, err := EncodeEnqueue(msgs)
	assertNoError(t, err)

	// Simulate server response
	rw := NewWriter(64)
	rw.WriteU32(2)
	rw.WriteU8(0x00) // Ok
	assertNoError(t, rw.WriteString("msg-id-1"))
	rw.WriteU8(0x01) // QueueNotFound
	assertNoError(t, rw.WriteString(""))

	results, err := DecodeEnqueueResult(rw.Bytes())
	assertNoError(t, err)
	assertEqual(t, 2, len(results))
	assertEqual(t, ErrorOk, results[0].ErrorCode)
	assertEqual(t, "msg-id-1", results[0].MessageID)
	assertEqual(t, ErrorQueueNotFound, results[1].ErrorCode)

	// Verify encode produces valid data
	r := NewReader(data)
	count, err := r.ReadU32()
	assertNoError(t, err)
	assertEqual(t, uint32(2), count)
}

func TestConsumeRoundTrip(t *testing.T) {
	data, err := EncodeConsume("test-queue")
	assertNoError(t, err)

	r := NewReader(data)
	queue, err := r.ReadString()
	assertNoError(t, err)
	assertEqual(t, "test-queue", queue)
}

func TestAckRoundTrip(t *testing.T) {
	items := []AckItem{
		{Queue: "q1", MessageID: "id1"},
		{Queue: "q2", MessageID: "id2"},
	}
	data, err := EncodeAck(items)
	assertNoError(t, err)

	r := NewReader(data)
	count, err := r.ReadU32()
	assertNoError(t, err)
	assertEqual(t, uint32(2), count)

	q, _ := r.ReadString()
	assertEqual(t, "q1", q)
	id, _ := r.ReadString()
	assertEqual(t, "id1", id)
}

func TestNackRoundTrip(t *testing.T) {
	items := []NackItem{
		{Queue: "q1", MessageID: "id1", Error: "processing failed"},
	}
	data, err := EncodeNack(items)
	assertNoError(t, err)

	r := NewReader(data)
	count, err := r.ReadU32()
	assertNoError(t, err)
	assertEqual(t, uint32(1), count)
}

func TestHandshakeRoundTrip(t *testing.T) {
	key := "my-api-key"
	data, err := EncodeHandshake(ProtocolVersion, &key)
	assertNoError(t, err)

	r := NewReader(data)
	ver, err := r.ReadU16()
	assertNoError(t, err)
	assertEqual(t, ProtocolVersion, ver)

	optKey, err := r.ReadOptionalString()
	assertNoError(t, err)
	if optKey == nil || *optKey != "my-api-key" {
		t.Fatal("expected api key")
	}

	// Without key
	data2, err := EncodeHandshake(ProtocolVersion, nil)
	assertNoError(t, err)
	r2 := NewReader(data2)
	_, _ = r2.ReadU16()
	optKey2, _ := r2.ReadOptionalString()
	if optKey2 != nil {
		t.Fatal("expected nil api key")
	}
}

func TestHandshakeOkDecode(t *testing.T) {
	w := NewWriter(32)
	w.WriteU16(1)
	w.WriteU64(42)
	w.WriteU32(16 * 1024 * 1024)

	resp, err := DecodeHandshakeOk(w.Bytes())
	assertNoError(t, err)
	assertEqual(t, uint16(1), resp.NegotiatedVersion)
	assertEqual(t, uint64(42), resp.NodeID)
	assertEqual(t, uint32(16*1024*1024), resp.MaxFrameSize)
}

func TestDeliveryDecode(t *testing.T) {
	w := NewWriter(256)
	w.WriteU32(1) // message_count
	assertNoError(t, w.WriteString("msg-1"))
	assertNoError(t, w.WriteString("orders"))
	assertNoError(t, w.WriteStringMap(map[string]string{"h": "v"}))
	w.WriteBytes([]byte("payload"))
	assertNoError(t, w.WriteString("key1"))
	w.WriteU32(10)
	assertNoError(t, w.WriteStringSlice([]string{"throttle1"}))
	w.WriteU32(3)
	w.WriteU64(1000)
	w.WriteU64(2000)

	msgs, err := DecodeDelivery(w.Bytes())
	assertNoError(t, err)
	assertEqual(t, 1, len(msgs))
	assertEqual(t, "msg-1", msgs[0].MessageID)
	assertEqual(t, "orders", msgs[0].Queue)
	assertEqual(t, "v", msgs[0].Headers["h"])
	if !bytes.Equal([]byte("payload"), msgs[0].Payload) {
		t.Fatal("payload mismatch")
	}
	assertEqual(t, "key1", msgs[0].FairnessKey)
	assertEqual(t, uint32(10), msgs[0].Weight)
	assertEqual(t, 1, len(msgs[0].ThrottleKeys))
	assertEqual(t, "throttle1", msgs[0].ThrottleKeys[0])
	assertEqual(t, uint32(3), msgs[0].AttemptCount)
	assertEqual(t, uint64(1000), msgs[0].EnqueuedAt)
	assertEqual(t, uint64(2000), msgs[0].LeasedAt)
}

func TestErrorDecode(t *testing.T) {
	w := NewWriter(64)
	w.WriteU8(0x0C) // NotLeader
	assertNoError(t, w.WriteString("not the leader"))
	assertNoError(t, w.WriteStringMap(map[string]string{"leader_addr": "host:5555"}))

	resp, err := DecodeError(w.Bytes())
	assertNoError(t, err)
	assertEqual(t, ErrorNotLeader, resp.Code)
	assertEqual(t, "not the leader", resp.Message)
	assertEqual(t, "host:5555", resp.Metadata["leader_addr"])
}

func TestCreateQueueRoundTrip(t *testing.T) {
	script := "return true"
	data, err := EncodeCreateQueue("myqueue", &script, nil, 30000)
	assertNoError(t, err)

	r := NewReader(data)
	name, _ := r.ReadString()
	assertEqual(t, "myqueue", name)
	onEnqueue, _ := r.ReadOptionalString()
	if onEnqueue == nil || *onEnqueue != "return true" {
		t.Fatal("expected on_enqueue_script")
	}
	onFailure, _ := r.ReadOptionalString()
	if onFailure != nil {
		t.Fatal("expected nil on_failure_script")
	}
	timeout, _ := r.ReadU64()
	assertEqual(t, uint64(30000), timeout)
}

func TestFrameWriterContinuation(t *testing.T) {
	var buf bytes.Buffer
	fw := NewFrameWriter(&buf, 32) // Very small max frame size

	body := make([]byte, 100)
	for i := range body {
		body[i] = byte(i)
	}

	err := fw.WriteFrame(OpcodeEnqueue, 1, body)
	assertNoError(t, err)

	// Read back with FrameReader
	fr := NewFrameReader(&buf, 32)
	hdr, data, err := fr.ReadFrame()
	assertNoError(t, err)
	assertEqual(t, OpcodeEnqueue, hdr.Opcode)
	assertEqual(t, uint32(1), hdr.RequestID)
	if !bytes.Equal(body, data) {
		t.Fatalf("reassembled body mismatch: got %d bytes, expected %d", len(data), len(body))
	}
}

func TestAdminResultDecoders(t *testing.T) {
	// CreateQueueResult
	{
		w := NewWriter(32)
		w.WriteU8(0x00)
		assertNoError(t, w.WriteString("queue-id-1"))
		resp, err := DecodeCreateQueueResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorOk, resp.ErrorCode)
		assertEqual(t, "queue-id-1", resp.QueueID)
	}

	// DeleteQueueResult
	{
		w := NewWriter(8)
		w.WriteU8(0x01) // QueueNotFound
		resp, err := DecodeDeleteQueueResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorQueueNotFound, resp.ErrorCode)
	}

	// SetConfigResult
	{
		w := NewWriter(8)
		w.WriteU8(0x00)
		resp, err := DecodeSetConfigResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorOk, resp.ErrorCode)
	}

	// GetConfigResult
	{
		w := NewWriter(32)
		w.WriteU8(0x00)
		assertNoError(t, w.WriteString("some-value"))
		resp, err := DecodeGetConfigResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorOk, resp.ErrorCode)
		assertEqual(t, "some-value", resp.Value)
	}

	// RedriveResult
	{
		w := NewWriter(16)
		w.WriteU8(0x00)
		w.WriteU64(42)
		resp, err := DecodeRedriveResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorOk, resp.ErrorCode)
		assertEqual(t, uint64(42), resp.Redriven)
	}
}

func TestAuthResultDecoders(t *testing.T) {
	// CreateApiKeyResult
	{
		w := NewWriter(64)
		w.WriteU8(0x00)
		assertNoError(t, w.WriteString("key-id-1"))
		assertNoError(t, w.WriteString("raw-key-abc"))
		w.WriteBool(true)
		resp, err := DecodeCreateApiKeyResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorOk, resp.ErrorCode)
		assertEqual(t, "key-id-1", resp.KeyID)
		assertEqual(t, "raw-key-abc", resp.Key)
		assertEqual(t, true, resp.IsSuperadmin)
	}

	// RevokeApiKeyResult
	{
		w := NewWriter(8)
		w.WriteU8(0x0F) // ApiKeyNotFound
		resp, err := DecodeRevokeApiKeyResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorApiKeyNotFound, resp.ErrorCode)
	}

	// SetAclResult
	{
		w := NewWriter(8)
		w.WriteU8(0x00)
		resp, err := DecodeSetAclResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorOk, resp.ErrorCode)
	}

	// GetAclResult
	{
		w := NewWriter(64)
		w.WriteU8(0x00)
		assertNoError(t, w.WriteString("key-id-1"))
		w.WriteBool(false)
		w.WriteU16(2)
		assertNoError(t, w.WriteString("produce"))
		assertNoError(t, w.WriteString("orders.*"))
		assertNoError(t, w.WriteString("consume"))
		assertNoError(t, w.WriteString("*"))
		resp, err := DecodeGetAclResult(w.Bytes())
		assertNoError(t, err)
		assertEqual(t, ErrorOk, resp.ErrorCode)
		assertEqual(t, "key-id-1", resp.KeyID)
		assertEqual(t, false, resp.IsSuperadmin)
		assertEqual(t, 2, len(resp.Permissions))
		assertEqual(t, "produce", resp.Permissions[0].Kind)
		assertEqual(t, "orders.*", resp.Permissions[0].Pattern)
	}
}

func TestErrorCodeString(t *testing.T) {
	assertEqual(t, "ok", ErrorOk.String())
	assertEqual(t, "queue_not_found", ErrorQueueNotFound.String())
	assertEqual(t, "not_leader", ErrorNotLeader.String())
	assertEqual(t, "internal_error", ErrorInternal.String())
	assertEqual(t, "unknown", ErrorCode(0xAA).String())
}
