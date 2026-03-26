package fila

import (
	"fmt"

	filav1 "github.com/faisca/fila-go/filav1"
	"google.golang.org/protobuf/proto"
)

// Admin ops use FIBP frames with protobuf-encoded payloads.
const (
	opCreateQueue  uint8 = 0x10
	opDeleteQueue  uint8 = 0x11
	opQueueStats   uint8 = 0x12
	opListQueues   uint8 = 0x13
	opPauseQueue   uint8 = 0x14
	opResumeQueue  uint8 = 0x15
	opRedrive      uint8 = 0x16
)

// AdminClient provides administrative operations on a Fila broker.
//
// Create one via Client.Admin().
type AdminClient struct {
	c *conn
}

// Admin returns an AdminClient backed by the same FIBP connection.
func (c *Client) Admin() *AdminClient {
	return &AdminClient{c: c.conn}
}

// CreateQueue creates a new queue with the given name and configuration.
func (a *AdminClient) CreateQueue(name string, config *filav1.QueueConfig) error {
	if config == nil {
		config = &filav1.QueueConfig{}
	}
	req := &filav1.CreateQueueRequest{
		Name:   name,
		Config: config,
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("create queue: marshal: %w", err)
	}

	resp, err := a.c.send(0, opCreateQueue, payload)
	if err != nil {
		return err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return &ProtocolError{Code: code, Message: msg}
	}
	return nil
}

// ListQueues returns the names of all queues on the broker.
func (a *AdminClient) ListQueues() ([]string, error) {
	req := &filav1.ListQueuesRequest{}
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("list queues: marshal: %w", err)
	}

	resp, err := a.c.send(0, opListQueues, payload)
	if err != nil {
		return nil, err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return nil, &ProtocolError{Code: code, Message: msg}
	}

	var listResp filav1.ListQueuesResponse
	if err := proto.Unmarshal(resp.payload, &listResp); err != nil {
		return nil, fmt.Errorf("list queues: unmarshal: %w", err)
	}
	names := make([]string, 0, len(listResp.Queues))
	for _, q := range listResp.Queues {
		names = append(names, q.Name)
	}
	return names, nil
}

// DeleteQueue deletes the named queue.
func (a *AdminClient) DeleteQueue(name string) error {
	req := &filav1.DeleteQueueRequest{Queue: name}
	payload, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("delete queue: marshal: %w", err)
	}

	resp, err := a.c.send(0, opDeleteQueue, payload)
	if err != nil {
		return err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return &ProtocolError{Code: code, Message: msg}
	}
	return nil
}
