package fila

import (
	"testing"
)

func TestEncodeDecodeEnqueueRoundtrip(t *testing.T) {
	msgs := []EnqueueMessage{
		{Queue: "q1", Headers: map[string]string{"k": "v"}, Payload: []byte("hello")},
		{Queue: "q1", Payload: []byte("world")},
	}
	payload := encodeEnqueueRequest("q1", msgs)
	if len(payload) == 0 {
		t.Fatal("expected non-empty payload")
	}

	// Simulate a successful response.
	// Build a fake response: 2 ok results with msg IDs.
	id1 := "id-001"
	id2 := "id-002"
	var resp []byte
	// count
	resp = appendU16(resp, 2)
	// result 0: ok
	resp = append(resp, 1)
	resp = appendU16(resp, uint16(len(id1)))
	resp = append(resp, id1...)
	// result 1: ok
	resp = append(resp, 1)
	resp = appendU16(resp, uint16(len(id2)))
	resp = append(resp, id2...)

	results, err := decodeEnqueueResponse(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].MessageID != id1 {
		t.Errorf("expected %s, got %s", id1, results[0].MessageID)
	}
	if results[1].MessageID != id2 {
		t.Errorf("expected %s, got %s", id2, results[1].MessageID)
	}
}

func TestDecodeEnqueueResponseError(t *testing.T) {
	errMsg := "queue not found"
	var resp []byte
	resp = appendU16(resp, 1) // count
	resp = append(resp, 0)    // not ok
	resp = appendU16(resp, 1) // err_code = 1 (QUEUE_NOT_FOUND)
	resp = appendU16(resp, uint16(len(errMsg)))
	resp = append(resp, errMsg...)

	results, err := decodeEnqueueResponse(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Fatal("expected error result, got nil")
	}
	ie, ok := results[0].Err.(*ItemError)
	if !ok {
		t.Fatalf("expected *ItemError, got %T", results[0].Err)
	}
	if ie.Code != "queue_not_found" {
		t.Errorf("expected code queue_not_found, got %s", ie.Code)
	}
}
