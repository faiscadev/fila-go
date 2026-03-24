package fila

import (
	"testing"

	filav1 "github.com/faisca/fila-go/filav1"
)

func TestExtractMessagesSingular(t *testing.T) {
	resp := &filav1.ConsumeResponse{
		Message: &filav1.Message{
			Id:      "msg-1",
			Payload: []byte("payload-1"),
			Metadata: &filav1.MessageMetadata{
				FairnessKey: "key-1",
				QueueId:     "q1",
			},
		},
	}

	msgs := extractMessages(resp)
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

func TestExtractMessagesBatched(t *testing.T) {
	resp := &filav1.ConsumeResponse{
		Messages: []*filav1.Message{
			{
				Id:      "msg-1",
				Payload: []byte("payload-1"),
				Metadata: &filav1.MessageMetadata{
					FairnessKey: "key-1",
					QueueId:     "q1",
				},
			},
			{
				Id:      "msg-2",
				Payload: []byte("payload-2"),
				Metadata: &filav1.MessageMetadata{
					FairnessKey: "key-2",
					QueueId:     "q1",
				},
			},
			{
				Id:      "msg-3",
				Payload: []byte("payload-3"),
				Metadata: &filav1.MessageMetadata{
					FairnessKey: "key-3",
					QueueId:     "q1",
				},
			},
		},
	}

	msgs := extractMessages(resp)
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

func TestExtractMessagesBatchedOverridesSingular(t *testing.T) {
	// When both fields are populated, the repeated messages field takes
	// precedence.
	resp := &filav1.ConsumeResponse{
		Message: &filav1.Message{
			Id:      "singular-msg",
			Payload: []byte("singular"),
		},
		Messages: []*filav1.Message{
			{
				Id:      "batch-msg-1",
				Payload: []byte("batch-1"),
			},
			{
				Id:      "batch-msg-2",
				Payload: []byte("batch-2"),
			},
		},
	}

	msgs := extractMessages(resp)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (batched takes precedence), got %d", len(msgs))
	}
	if msgs[0].ID != "batch-msg-1" {
		t.Errorf("expected first message ID batch-msg-1, got %s", msgs[0].ID)
	}
}

func TestExtractMessagesKeepalive(t *testing.T) {
	// A keepalive frame has neither singular nor batched messages.
	resp := &filav1.ConsumeResponse{}

	msgs := extractMessages(resp)
	if msgs != nil {
		t.Errorf("expected nil for keepalive frame, got %v", msgs)
	}
}

func TestExtractMessagesNilMetadata(t *testing.T) {
	// Messages without metadata should still work (empty metadata).
	resp := &filav1.ConsumeResponse{
		Message: &filav1.Message{
			Id:      "no-meta",
			Payload: []byte("test"),
		},
	}

	msgs := extractMessages(resp)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].FairnessKey != "" {
		t.Errorf("expected empty fairness key, got %s", msgs[0].FairnessKey)
	}
	if msgs[0].AttemptCount != 0 {
		t.Errorf("expected 0 attempt count, got %d", msgs[0].AttemptCount)
	}
}

func TestProtoToConsumeMessageNil(t *testing.T) {
	msg := protoToConsumeMessage(nil)
	if msg != nil {
		t.Error("expected nil for nil proto message")
	}
}
