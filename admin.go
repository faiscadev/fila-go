package fila

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// Admin ops use FIBP frames with binary-encoded payloads (same encoding as data ops).
const (
	opCreateQueue uint8 = 0x10
	opDeleteQueue uint8 = 0x11
	opQueueStats  uint8 = 0x12
	opListQueues  uint8 = 0x13
	opRedrive     uint8 = 0x14
	opConfigSet   uint8 = 0x15
	opConfigGet   uint8 = 0x16
	opConfigList  uint8 = 0x17
)

// QueueConfig holds optional configuration for creating a queue.
type QueueConfig struct {
	OnEnqueue         string
	OnFailure         string
	VisibilityTimeout uint32
}

// QueueStats holds statistics for a queue.
type QueueStats struct {
	Depth            uint64
	InFlight         uint64
	ActiveFairness   uint32
	ActiveConsumers  uint32
	Quantum          uint64
	LeaderNodeID     uint64
	ReplicationCount uint32
	ClusterNodeCount uint32
}

// QueueInfo holds summary information for a queue in a list.
type QueueInfo struct {
	Name            string
	Depth           uint64
	InFlight        uint64
	ActiveConsumers uint32
}

// RedriveResult holds the result of a redrive operation.
type RedriveResult struct {
	MovedCount uint32
}

// ConfigEntry holds a key-value configuration pair.
type ConfigEntry struct {
	Key   string
	Value string
}

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
//
// Wire format: queue_len:u16 + queue:utf8 + on_enqueue_len:u16 + on_enqueue:utf8
//
//	+ on_failure_len:u16 + on_failure:utf8 + visibility_timeout_ms:u32
func (a *AdminClient) CreateQueue(name string, config *QueueConfig) error {
	var buf bytes.Buffer
	writeU16(&buf, uint16(len(name)))
	buf.WriteString(name)

	onEnqueue := ""
	onFailure := ""
	var visTimeout uint32
	if config != nil {
		onEnqueue = config.OnEnqueue
		onFailure = config.OnFailure
		visTimeout = config.VisibilityTimeout
	}
	writeU16(&buf, uint16(len(onEnqueue)))
	buf.WriteString(onEnqueue)
	writeU16(&buf, uint16(len(onFailure)))
	buf.WriteString(onFailure)
	writeU32(&buf, visTimeout)

	resp, err := a.c.send(context.Background(), 0, opCreateQueue, buf.Bytes())
	if err != nil {
		return err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return &ProtocolError{Code: code, Message: msg}
	}
	return nil
}

// DeleteQueue deletes the named queue.
//
// Wire format: queue_len:u16 + queue:utf8
func (a *AdminClient) DeleteQueue(name string) error {
	var buf bytes.Buffer
	writeU16(&buf, uint16(len(name)))
	buf.WriteString(name)

	resp, err := a.c.send(context.Background(), 0, opDeleteQueue, buf.Bytes())
	if err != nil {
		return err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return &ProtocolError{Code: code, Message: msg}
	}
	return nil
}

// GetQueueStats returns statistics for the named queue.
//
// Request: queue_len:u16 + queue:utf8
// Response: depth:u64 + in_flight:u64 + active_fairness_keys:u32 + active_consumers:u32
//
//	+ quantum:u64 + leader_node_id:u64 + replication_count:u32 + cluster_node_count:u32
func (a *AdminClient) GetQueueStats(name string) (*QueueStats, error) {
	var buf bytes.Buffer
	writeU16(&buf, uint16(len(name)))
	buf.WriteString(name)

	resp, err := a.c.send(context.Background(), 0, opQueueStats, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return nil, &ProtocolError{Code: code, Message: msg}
	}

	p := resp.payload
	if len(p) < 48 {
		return nil, fmt.Errorf("queue stats: response too short (%d bytes)", len(p))
	}

	return &QueueStats{
		Depth:            binary.BigEndian.Uint64(p[0:8]),
		InFlight:         binary.BigEndian.Uint64(p[8:16]),
		ActiveFairness:   binary.BigEndian.Uint32(p[16:20]),
		ActiveConsumers:  binary.BigEndian.Uint32(p[20:24]),
		Quantum:          binary.BigEndian.Uint64(p[24:32]),
		LeaderNodeID:     binary.BigEndian.Uint64(p[32:40]),
		ReplicationCount: binary.BigEndian.Uint32(p[40:44]),
		ClusterNodeCount: binary.BigEndian.Uint32(p[44:48]),
	}, nil
}

// ListQueues returns information about all queues on the broker.
//
// Response: count:u16 + repeated(name_len:u16 + name:utf8 + depth:u64 + in_flight:u64 + active_consumers:u32)
func (a *AdminClient) ListQueues() ([]QueueInfo, error) {
	resp, err := a.c.send(context.Background(), 0, opListQueues, nil)
	if err != nil {
		return nil, err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return nil, &ProtocolError{Code: code, Message: msg}
	}

	p := resp.payload
	if len(p) < 2 {
		return nil, fmt.Errorf("list queues: response too short")
	}
	count := int(binary.BigEndian.Uint16(p[0:2]))
	pos := 2

	queues := make([]QueueInfo, 0, count)
	for i := 0; i < count; i++ {
		if pos+2 > len(p) {
			return nil, fmt.Errorf("list queues: truncated at entry %d", i)
		}
		nameLen := int(binary.BigEndian.Uint16(p[pos : pos+2]))
		pos += 2
		if pos+nameLen > len(p) {
			return nil, fmt.Errorf("list queues: truncated name at entry %d", i)
		}
		name := string(p[pos : pos+nameLen])
		pos += nameLen
		if pos+20 > len(p) {
			return nil, fmt.Errorf("list queues: truncated stats at entry %d", i)
		}
		depth := binary.BigEndian.Uint64(p[pos : pos+8])
		pos += 8
		inFlight := binary.BigEndian.Uint64(p[pos : pos+8])
		pos += 8
		activeCons := binary.BigEndian.Uint32(p[pos : pos+4])
		pos += 4

		queues = append(queues, QueueInfo{
			Name:            name,
			Depth:           depth,
			InFlight:        inFlight,
			ActiveConsumers: activeCons,
		})
	}

	return queues, nil
}

// Redrive moves messages from a DLQ back to the source queue.
//
// Request: source_queue_len:u16 + source_queue:utf8 + max_messages:u32
// Response: moved_count:u32
func (a *AdminClient) Redrive(sourceQueue string, maxMessages uint32) (*RedriveResult, error) {
	var buf bytes.Buffer
	writeU16(&buf, uint16(len(sourceQueue)))
	buf.WriteString(sourceQueue)
	writeU32(&buf, maxMessages)

	resp, err := a.c.send(context.Background(), 0, opRedrive, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return nil, &ProtocolError{Code: code, Message: msg}
	}

	if len(resp.payload) < 4 {
		return nil, fmt.Errorf("redrive: response too short")
	}
	return &RedriveResult{
		MovedCount: binary.BigEndian.Uint32(resp.payload[0:4]),
	}, nil
}

// SetConfig sets a configuration key-value pair.
//
// Wire format: key_len:u16 + key:utf8 + value_len:u16 + value:utf8
func (a *AdminClient) SetConfig(key, value string) error {
	var buf bytes.Buffer
	writeU16(&buf, uint16(len(key)))
	buf.WriteString(key)
	writeU16(&buf, uint16(len(value)))
	buf.WriteString(value)

	resp, err := a.c.send(context.Background(), 0, opConfigSet, buf.Bytes())
	if err != nil {
		return err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return &ProtocolError{Code: code, Message: msg}
	}
	return nil
}

// GetConfig retrieves a configuration value by key.
//
// Request: key_len:u16 + key:utf8
// Response: found:u8 + if found: value_len:u16 + value:utf8
func (a *AdminClient) GetConfig(key string) (string, bool, error) {
	var buf bytes.Buffer
	writeU16(&buf, uint16(len(key)))
	buf.WriteString(key)

	resp, err := a.c.send(context.Background(), 0, opConfigGet, buf.Bytes())
	if err != nil {
		return "", false, err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return "", false, &ProtocolError{Code: code, Message: msg}
	}

	p := resp.payload
	if len(p) < 1 {
		return "", false, fmt.Errorf("get config: response too short")
	}
	if p[0] == 0 {
		return "", false, nil
	}
	if len(p) < 3 {
		return "", false, fmt.Errorf("get config: truncated value")
	}
	valLen := int(binary.BigEndian.Uint16(p[1:3]))
	if len(p) < 3+valLen {
		return "", false, fmt.Errorf("get config: truncated value bytes")
	}
	return string(p[3 : 3+valLen]), true, nil
}

// ListConfig lists configuration entries matching the given prefix.
//
// Request: prefix_len:u16 + prefix:utf8
// Response: count:u16 + repeated(key_len:u16 + key:utf8 + value_len:u16 + value:utf8)
func (a *AdminClient) ListConfig(prefix string) ([]ConfigEntry, error) {
	var buf bytes.Buffer
	writeU16(&buf, uint16(len(prefix)))
	buf.WriteString(prefix)

	resp, err := a.c.send(context.Background(), 0, opConfigList, buf.Bytes())
	if err != nil {
		return nil, err
	}
	if resp.op == opError {
		msg, code := decodeErrorPayload(resp.payload)
		return nil, &ProtocolError{Code: code, Message: msg}
	}

	p := resp.payload
	if len(p) < 2 {
		return nil, fmt.Errorf("list config: response too short")
	}
	count := int(binary.BigEndian.Uint16(p[0:2]))
	pos := 2

	entries := make([]ConfigEntry, 0, count)
	for i := 0; i < count; i++ {
		if pos+2 > len(p) {
			return nil, fmt.Errorf("list config: truncated at entry %d", i)
		}
		keyLen := int(binary.BigEndian.Uint16(p[pos : pos+2]))
		pos += 2
		if pos+keyLen > len(p) {
			return nil, fmt.Errorf("list config: truncated key at entry %d", i)
		}
		k := string(p[pos : pos+keyLen])
		pos += keyLen
		if pos+2 > len(p) {
			return nil, fmt.Errorf("list config: truncated value length at entry %d", i)
		}
		valLen := int(binary.BigEndian.Uint16(p[pos : pos+2]))
		pos += 2
		if pos+valLen > len(p) {
			return nil, fmt.Errorf("list config: truncated value at entry %d", i)
		}
		v := string(p[pos : pos+valLen])
		pos += valLen
		entries = append(entries, ConfigEntry{Key: k, Value: v})
	}

	return entries, nil
}
