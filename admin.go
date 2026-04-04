package fila

import (
	"context"

	"github.com/faisca/fila-go/fibp"
)

// CreateQueueOpts holds optional parameters for CreateQueue.
type CreateQueueOpts struct {
	OnEnqueueScript     *string
	OnFailureScript     *string
	VisibilityTimeoutMs uint64
}

// CreateQueue creates a new queue with the given name and options.
func (c *Client) CreateQueue(ctx context.Context, name string, opts *CreateQueueOpts) (string, error) {
	var onEnqueue, onFailure *string
	var timeout uint64
	if opts != nil {
		onEnqueue = opts.OnEnqueueScript
		onFailure = opts.OnFailureScript
		timeout = opts.VisibilityTimeoutMs
	}
	body, err := fibp.EncodeCreateQueue(name, onEnqueue, onFailure, timeout)
	if err != nil {
		return "", err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeCreateQueue, body)
	if err != nil {
		return "", err
	}
	resp, err := fibp.DecodeCreateQueueResult(respBody)
	if err != nil {
		return "", err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return "", errorCodeToErr(resp.ErrorCode, "", nil)
	}
	return resp.QueueID, nil
}

// DeleteQueue deletes a queue by name.
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	body, err := fibp.EncodeDeleteQueue(name)
	if err != nil {
		return err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeDeleteQueue, body)
	if err != nil {
		return err
	}
	resp, err := fibp.DecodeDeleteQueueResult(respBody)
	if err != nil {
		return err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return errorCodeToErr(resp.ErrorCode, "", nil)
	}
	return nil
}

// QueueStats holds the statistics for a queue.
type QueueStats struct {
	Depth              uint64
	InFlight           uint64
	ActiveFairnessKeys uint64
	ActiveConsumers    uint32
	Quantum            uint32
	LeaderNodeID       uint64
	ReplicationCount   uint32
	FairnessKeyStats   []FairnessKeyStat
	ThrottleKeyStats   []ThrottleKeyStat
}

// FairnessKeyStat holds per-fairness-key statistics.
type FairnessKeyStat struct {
	Key            string
	PendingCount   uint64
	CurrentDeficit int64
	Weight         uint32
}

// ThrottleKeyStat holds per-throttle-key statistics.
type ThrottleKeyStat struct {
	Key           string
	Tokens        float64
	RatePerSecond float64
	Burst         float64
}

// GetStats returns the statistics for a queue.
func (c *Client) GetStats(ctx context.Context, queue string) (*QueueStats, error) {
	body, err := fibp.EncodeGetStats(queue)
	if err != nil {
		return nil, err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeGetStats, body)
	if err != nil {
		return nil, err
	}
	resp, err := fibp.DecodeGetStatsResult(respBody)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return nil, errorCodeToErr(resp.ErrorCode, "", nil)
	}
	stats := &QueueStats{
		Depth:              resp.Depth,
		InFlight:           resp.InFlight,
		ActiveFairnessKeys: resp.ActiveFairnessKeys,
		ActiveConsumers:    resp.ActiveConsumers,
		Quantum:            resp.Quantum,
		LeaderNodeID:       resp.LeaderNodeID,
		ReplicationCount:   resp.ReplicationCount,
	}
	for _, fk := range resp.FairnessKeyStats {
		stats.FairnessKeyStats = append(stats.FairnessKeyStats, FairnessKeyStat{
			Key:            fk.Key,
			PendingCount:   fk.PendingCount,
			CurrentDeficit: fk.CurrentDeficit,
			Weight:         fk.Weight,
		})
	}
	for _, tk := range resp.ThrottleKeyStats {
		stats.ThrottleKeyStats = append(stats.ThrottleKeyStats, ThrottleKeyStat{
			Key:           tk.Key,
			Tokens:        tk.Tokens,
			RatePerSecond: tk.RatePerSecond,
			Burst:         tk.Burst,
		})
	}
	return stats, nil
}

// QueueInfo holds summary info for a queue in a list operation.
type QueueInfo struct {
	Name            string
	Depth           uint64
	InFlight        uint64
	ActiveConsumers uint32
	LeaderNodeID    uint64
}

// ListQueuesResult holds the result of a ListQueues call.
type ListQueuesResult struct {
	ClusterNodeCount uint32
	Queues           []QueueInfo
}

// ListQueues lists all queues on the server.
func (c *Client) ListQueues(ctx context.Context) (*ListQueuesResult, error) {
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeListQueues, nil)
	if err != nil {
		return nil, err
	}
	resp, err := fibp.DecodeListQueuesResult(respBody)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return nil, errorCodeToErr(resp.ErrorCode, "", nil)
	}
	result := &ListQueuesResult{ClusterNodeCount: resp.ClusterNodeCount}
	for _, q := range resp.Queues {
		result.Queues = append(result.Queues, QueueInfo{
			Name:            q.Name,
			Depth:           q.Depth,
			InFlight:        q.InFlight,
			ActiveConsumers: q.ActiveConsumers,
			LeaderNodeID:    q.LeaderNodeID,
		})
	}
	return result, nil
}

// SetConfig sets a runtime configuration key-value pair.
func (c *Client) SetConfig(ctx context.Context, key, value string) error {
	body, err := fibp.EncodeSetConfig(key, value)
	if err != nil {
		return err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeSetConfig, body)
	if err != nil {
		return err
	}
	resp, err := fibp.DecodeSetConfigResult(respBody)
	if err != nil {
		return err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return errorCodeToErr(resp.ErrorCode, "", nil)
	}
	return nil
}

// GetConfig gets a runtime configuration value by key.
func (c *Client) GetConfig(ctx context.Context, key string) (string, error) {
	body, err := fibp.EncodeGetConfig(key)
	if err != nil {
		return "", err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeGetConfig, body)
	if err != nil {
		return "", err
	}
	resp, err := fibp.DecodeGetConfigResult(respBody)
	if err != nil {
		return "", err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return "", errorCodeToErr(resp.ErrorCode, "", nil)
	}
	return resp.Value, nil
}

// ConfigEntry holds a single config entry.
type ConfigEntry struct {
	Key   string
	Value string
}

// ListConfig lists config entries by prefix.
func (c *Client) ListConfig(ctx context.Context, prefix string) ([]ConfigEntry, error) {
	body, err := fibp.EncodeListConfig(prefix)
	if err != nil {
		return nil, err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeListConfig, body)
	if err != nil {
		return nil, err
	}
	resp, err := fibp.DecodeListConfigResult(respBody)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return nil, errorCodeToErr(resp.ErrorCode, "", nil)
	}
	entries := make([]ConfigEntry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = ConfigEntry{Key: e.Key, Value: e.Value}
	}
	return entries, nil
}

// Redrive moves messages from a dead-letter queue back to their parent queue.
func (c *Client) Redrive(ctx context.Context, dlqQueue string, count uint64) (uint64, error) {
	body, err := fibp.EncodeRedrive(dlqQueue, count)
	if err != nil {
		return 0, err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeRedrive, body)
	if err != nil {
		return 0, err
	}
	resp, err := fibp.DecodeRedriveResult(respBody)
	if err != nil {
		return 0, err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return 0, errorCodeToErr(resp.ErrorCode, "", nil)
	}
	return resp.Redriven, nil
}
