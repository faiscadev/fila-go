package fibp

import "fmt"

// maxDecodeCount is a safety limit on batch counts to prevent malformed frames
// from causing huge allocations. The server's max frame size already limits
// actual data, but this prevents degenerate allocations before we start reading.
const maxDecodeCount = 1_000_000

func validateCount(count uint32, remaining int) error {
	if count > maxDecodeCount {
		return fmt.Errorf("count %d exceeds safety limit %d", count, maxDecodeCount)
	}
	return nil
}

// --- Control response decoders ---

// HandshakeOkResp holds the decoded HandshakeOk response.
type HandshakeOkResp struct {
	NegotiatedVersion uint16
	NodeID            uint64
	MaxFrameSize      uint32
}

// DecodeHandshakeOk decodes a HandshakeOk response body.
func DecodeHandshakeOk(data []byte) (HandshakeOkResp, error) {
	r := NewReader(data)
	ver, err := r.ReadU16()
	if err != nil {
		return HandshakeOkResp{}, err
	}
	nodeID, err := r.ReadU64()
	if err != nil {
		return HandshakeOkResp{}, err
	}
	maxFrame, err := r.ReadU32()
	if err != nil {
		return HandshakeOkResp{}, err
	}
	return HandshakeOkResp{
		NegotiatedVersion: ver,
		NodeID:            nodeID,
		MaxFrameSize:      maxFrame,
	}, nil
}

// --- Hot-path response decoders ---

// EnqueueResultItem holds the result of a single enqueued message.
type EnqueueResultItem struct {
	ErrorCode ErrorCode
	MessageID string
}

// DecodeEnqueueResult decodes an EnqueueResult response body.
func DecodeEnqueueResult(data []byte) ([]EnqueueResultItem, error) {
	r := NewReader(data)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if err := validateCount(count, r.Remaining()); err != nil {
		return nil, err
	}
	results := make([]EnqueueResultItem, count)
	for i := uint32(0); i < count; i++ {
		code, err := r.ReadU8()
		if err != nil {
			return nil, err
		}
		msgID, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		results[i] = EnqueueResultItem{
			ErrorCode: ErrorCode(code),
			MessageID: msgID,
		}
	}
	return results, nil
}

// ConsumeOkResp holds the decoded ConsumeOk response.
type ConsumeOkResp struct {
	ConsumerID string
}

// DecodeConsumeOk decodes a ConsumeOk response body.
func DecodeConsumeOk(data []byte) (ConsumeOkResp, error) {
	r := NewReader(data)
	cid, err := r.ReadString()
	if err != nil {
		return ConsumeOkResp{}, err
	}
	return ConsumeOkResp{ConsumerID: cid}, nil
}

// DeliveryMessage holds a single delivered message.
type DeliveryMessage struct {
	MessageID    string
	Queue        string
	Headers      map[string]string
	Payload      []byte
	FairnessKey  string
	Weight       uint32
	ThrottleKeys []string
	AttemptCount uint32
	EnqueuedAt   uint64
	LeasedAt     uint64
}

// DecodeDelivery decodes a Delivery response body.
func DecodeDelivery(data []byte) ([]DeliveryMessage, error) {
	r := NewReader(data)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if err := validateCount(count, r.Remaining()); err != nil {
		return nil, err
	}
	msgs := make([]DeliveryMessage, count)
	for i := uint32(0); i < count; i++ {
		msgID, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		queue, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		headers, err := r.ReadStringMap()
		if err != nil {
			return nil, err
		}
		payload, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		fairnessKey, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		weight, err := r.ReadU32()
		if err != nil {
			return nil, err
		}
		throttleKeys, err := r.ReadStringSlice()
		if err != nil {
			return nil, err
		}
		attemptCount, err := r.ReadU32()
		if err != nil {
			return nil, err
		}
		enqueuedAt, err := r.ReadU64()
		if err != nil {
			return nil, err
		}
		leasedAt, err := r.ReadU64()
		if err != nil {
			return nil, err
		}
		msgs[i] = DeliveryMessage{
			MessageID:    msgID,
			Queue:        queue,
			Headers:      headers,
			Payload:      payload,
			FairnessKey:  fairnessKey,
			Weight:       weight,
			ThrottleKeys: throttleKeys,
			AttemptCount: attemptCount,
			EnqueuedAt:   enqueuedAt,
			LeasedAt:     leasedAt,
		}
	}
	return msgs, nil
}

// AckResultItem holds the result of a single ack.
type AckResultItem struct {
	ErrorCode ErrorCode
}

// DecodeAckResult decodes an AckResult response body.
func DecodeAckResult(data []byte) ([]AckResultItem, error) {
	r := NewReader(data)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if err := validateCount(count, r.Remaining()); err != nil {
		return nil, err
	}
	results := make([]AckResultItem, count)
	for i := uint32(0); i < count; i++ {
		code, err := r.ReadU8()
		if err != nil {
			return nil, err
		}
		results[i] = AckResultItem{ErrorCode: ErrorCode(code)}
	}
	return results, nil
}

// NackResultItem holds the result of a single nack.
type NackResultItem struct {
	ErrorCode ErrorCode
}

// DecodeNackResult decodes a NackResult response body.
func DecodeNackResult(data []byte) ([]NackResultItem, error) {
	r := NewReader(data)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if err := validateCount(count, r.Remaining()); err != nil {
		return nil, err
	}
	results := make([]NackResultItem, count)
	for i := uint32(0); i < count; i++ {
		code, err := r.ReadU8()
		if err != nil {
			return nil, err
		}
		results[i] = NackResultItem{ErrorCode: ErrorCode(code)}
	}
	return results, nil
}

// --- Error frame decoder ---

// ErrorResp holds the decoded Error frame.
type ErrorResp struct {
	Code     ErrorCode
	Message  string
	Metadata map[string]string
}

// DecodeError decodes an Error frame body.
func DecodeError(data []byte) (ErrorResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return ErrorResp{}, err
	}
	msg, err := r.ReadString()
	if err != nil {
		return ErrorResp{}, err
	}
	metadata, err := r.ReadStringMap()
	if err != nil {
		return ErrorResp{}, err
	}
	return ErrorResp{
		Code:     ErrorCode(code),
		Message:  msg,
		Metadata: metadata,
	}, nil
}

// --- Admin response decoders ---

// CreateQueueResultResp holds a decoded CreateQueueResult.
type CreateQueueResultResp struct {
	ErrorCode ErrorCode
	QueueID   string
}

// DecodeCreateQueueResult decodes a CreateQueueResult body.
func DecodeCreateQueueResult(data []byte) (CreateQueueResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return CreateQueueResultResp{}, err
	}
	queueID, err := r.ReadString()
	if err != nil {
		return CreateQueueResultResp{}, err
	}
	return CreateQueueResultResp{ErrorCode: ErrorCode(code), QueueID: queueID}, nil
}

// DeleteQueueResultResp holds a decoded DeleteQueueResult.
type DeleteQueueResultResp struct {
	ErrorCode ErrorCode
}

// DecodeDeleteQueueResult decodes a DeleteQueueResult body.
func DecodeDeleteQueueResult(data []byte) (DeleteQueueResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return DeleteQueueResultResp{}, err
	}
	return DeleteQueueResultResp{ErrorCode: ErrorCode(code)}, nil
}

// FairnessKeyStat holds per-key stats in GetStatsResult.
type FairnessKeyStat struct {
	Key           string
	PendingCount  uint64
	CurrentDeficit int64
	Weight        uint32
}

// ThrottleKeyStat holds per-throttle stats in GetStatsResult.
type ThrottleKeyStat struct {
	Key          string
	Tokens       float64
	RatePerSecond float64
	Burst        float64
}

// GetStatsResultResp holds a decoded GetStatsResult.
type GetStatsResultResp struct {
	ErrorCode          ErrorCode
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

// DecodeGetStatsResult decodes a GetStatsResult body.
func DecodeGetStatsResult(data []byte) (GetStatsResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	depth, err := r.ReadU64()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	inFlight, err := r.ReadU64()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	activeFairnessKeys, err := r.ReadU64()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	activeConsumers, err := r.ReadU32()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	quantum, err := r.ReadU32()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	leaderNodeID, err := r.ReadU64()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	replicationCount, err := r.ReadU32()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	fkCount, err := r.ReadU16()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	fkStats := make([]FairnessKeyStat, fkCount)
	for i := uint16(0); i < fkCount; i++ {
		key, err := r.ReadString()
		if err != nil {
			return GetStatsResultResp{}, err
		}
		pending, err := r.ReadU64()
		if err != nil {
			return GetStatsResultResp{}, err
		}
		deficit, err := r.ReadI64()
		if err != nil {
			return GetStatsResultResp{}, err
		}
		weight, err := r.ReadU32()
		if err != nil {
			return GetStatsResultResp{}, err
		}
		fkStats[i] = FairnessKeyStat{
			Key:            key,
			PendingCount:   pending,
			CurrentDeficit: deficit,
			Weight:         weight,
		}
	}
	tkCount, err := r.ReadU16()
	if err != nil {
		return GetStatsResultResp{}, err
	}
	tkStats := make([]ThrottleKeyStat, tkCount)
	for i := uint16(0); i < tkCount; i++ {
		key, err := r.ReadString()
		if err != nil {
			return GetStatsResultResp{}, err
		}
		tokens, err := r.ReadF64()
		if err != nil {
			return GetStatsResultResp{}, err
		}
		rate, err := r.ReadF64()
		if err != nil {
			return GetStatsResultResp{}, err
		}
		burst, err := r.ReadF64()
		if err != nil {
			return GetStatsResultResp{}, err
		}
		tkStats[i] = ThrottleKeyStat{
			Key:           key,
			Tokens:        tokens,
			RatePerSecond: rate,
			Burst:         burst,
		}
	}
	return GetStatsResultResp{
		ErrorCode:          ErrorCode(code),
		Depth:              depth,
		InFlight:           inFlight,
		ActiveFairnessKeys: activeFairnessKeys,
		ActiveConsumers:    activeConsumers,
		Quantum:            quantum,
		LeaderNodeID:       leaderNodeID,
		ReplicationCount:   replicationCount,
		FairnessKeyStats:   fkStats,
		ThrottleKeyStats:   tkStats,
	}, nil
}

// ListQueuesQueueInfo holds per-queue info in ListQueuesResult.
type ListQueuesQueueInfo struct {
	Name            string
	Depth           uint64
	InFlight        uint64
	ActiveConsumers uint32
	LeaderNodeID    uint64
}

// ListQueuesResultResp holds a decoded ListQueuesResult.
type ListQueuesResultResp struct {
	ErrorCode        ErrorCode
	ClusterNodeCount uint32
	Queues           []ListQueuesQueueInfo
}

// DecodeListQueuesResult decodes a ListQueuesResult body.
func DecodeListQueuesResult(data []byte) (ListQueuesResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return ListQueuesResultResp{}, err
	}
	nodeCount, err := r.ReadU32()
	if err != nil {
		return ListQueuesResultResp{}, err
	}
	queueCount, err := r.ReadU16()
	if err != nil {
		return ListQueuesResultResp{}, err
	}
	queues := make([]ListQueuesQueueInfo, queueCount)
	for i := uint16(0); i < queueCount; i++ {
		name, err := r.ReadString()
		if err != nil {
			return ListQueuesResultResp{}, err
		}
		depth, err := r.ReadU64()
		if err != nil {
			return ListQueuesResultResp{}, err
		}
		inFlight, err := r.ReadU64()
		if err != nil {
			return ListQueuesResultResp{}, err
		}
		activeConsumers, err := r.ReadU32()
		if err != nil {
			return ListQueuesResultResp{}, err
		}
		leaderNodeID, err := r.ReadU64()
		if err != nil {
			return ListQueuesResultResp{}, err
		}
		queues[i] = ListQueuesQueueInfo{
			Name:            name,
			Depth:           depth,
			InFlight:        inFlight,
			ActiveConsumers: activeConsumers,
			LeaderNodeID:    leaderNodeID,
		}
	}
	return ListQueuesResultResp{
		ErrorCode:        ErrorCode(code),
		ClusterNodeCount: nodeCount,
		Queues:           queues,
	}, nil
}

// SetConfigResultResp holds a decoded SetConfigResult.
type SetConfigResultResp struct {
	ErrorCode ErrorCode
}

// DecodeSetConfigResult decodes a SetConfigResult body.
func DecodeSetConfigResult(data []byte) (SetConfigResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return SetConfigResultResp{}, err
	}
	return SetConfigResultResp{ErrorCode: ErrorCode(code)}, nil
}

// GetConfigResultResp holds a decoded GetConfigResult.
type GetConfigResultResp struct {
	ErrorCode ErrorCode
	Value     string
}

// DecodeGetConfigResult decodes a GetConfigResult body.
func DecodeGetConfigResult(data []byte) (GetConfigResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return GetConfigResultResp{}, err
	}
	value, err := r.ReadString()
	if err != nil {
		return GetConfigResultResp{}, err
	}
	return GetConfigResultResp{ErrorCode: ErrorCode(code), Value: value}, nil
}

// ListConfigEntry holds a single config entry.
type ListConfigEntry struct {
	Key   string
	Value string
}

// ListConfigResultResp holds a decoded ListConfigResult.
type ListConfigResultResp struct {
	ErrorCode ErrorCode
	Entries   []ListConfigEntry
}

// DecodeListConfigResult decodes a ListConfigResult body.
func DecodeListConfigResult(data []byte) (ListConfigResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return ListConfigResultResp{}, err
	}
	count, err := r.ReadU16()
	if err != nil {
		return ListConfigResultResp{}, err
	}
	entries := make([]ListConfigEntry, count)
	for i := uint16(0); i < count; i++ {
		key, err := r.ReadString()
		if err != nil {
			return ListConfigResultResp{}, err
		}
		value, err := r.ReadString()
		if err != nil {
			return ListConfigResultResp{}, err
		}
		entries[i] = ListConfigEntry{Key: key, Value: value}
	}
	return ListConfigResultResp{ErrorCode: ErrorCode(code), Entries: entries}, nil
}

// RedriveResultResp holds a decoded RedriveResult.
type RedriveResultResp struct {
	ErrorCode ErrorCode
	Redriven  uint64
}

// DecodeRedriveResult decodes a RedriveResult body.
func DecodeRedriveResult(data []byte) (RedriveResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return RedriveResultResp{}, err
	}
	redriven, err := r.ReadU64()
	if err != nil {
		return RedriveResultResp{}, err
	}
	return RedriveResultResp{ErrorCode: ErrorCode(code), Redriven: redriven}, nil
}

// --- Auth response decoders ---

// CreateApiKeyResultResp holds a decoded CreateApiKeyResult.
type CreateApiKeyResultResp struct {
	ErrorCode    ErrorCode
	KeyID        string
	Key          string
	IsSuperadmin bool
}

// DecodeCreateApiKeyResult decodes a CreateApiKeyResult body.
func DecodeCreateApiKeyResult(data []byte) (CreateApiKeyResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return CreateApiKeyResultResp{}, err
	}
	keyID, err := r.ReadString()
	if err != nil {
		return CreateApiKeyResultResp{}, err
	}
	key, err := r.ReadString()
	if err != nil {
		return CreateApiKeyResultResp{}, err
	}
	isSuperadmin, err := r.ReadBool()
	if err != nil {
		return CreateApiKeyResultResp{}, err
	}
	return CreateApiKeyResultResp{
		ErrorCode:    ErrorCode(code),
		KeyID:        keyID,
		Key:          key,
		IsSuperadmin: isSuperadmin,
	}, nil
}

// RevokeApiKeyResultResp holds a decoded RevokeApiKeyResult.
type RevokeApiKeyResultResp struct {
	ErrorCode ErrorCode
}

// DecodeRevokeApiKeyResult decodes a RevokeApiKeyResult body.
func DecodeRevokeApiKeyResult(data []byte) (RevokeApiKeyResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return RevokeApiKeyResultResp{}, err
	}
	return RevokeApiKeyResultResp{ErrorCode: ErrorCode(code)}, nil
}

// ListApiKeysKeyInfo holds per-key info in ListApiKeysResult.
type ListApiKeysKeyInfo struct {
	KeyID        string
	Name         string
	CreatedAtMs  uint64
	ExpiresAtMs  uint64
	IsSuperadmin bool
}

// ListApiKeysResultResp holds a decoded ListApiKeysResult.
type ListApiKeysResultResp struct {
	ErrorCode ErrorCode
	Keys      []ListApiKeysKeyInfo
}

// DecodeListApiKeysResult decodes a ListApiKeysResult body.
func DecodeListApiKeysResult(data []byte) (ListApiKeysResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return ListApiKeysResultResp{}, err
	}
	count, err := r.ReadU16()
	if err != nil {
		return ListApiKeysResultResp{}, err
	}
	keys := make([]ListApiKeysKeyInfo, count)
	for i := uint16(0); i < count; i++ {
		keyID, err := r.ReadString()
		if err != nil {
			return ListApiKeysResultResp{}, err
		}
		name, err := r.ReadString()
		if err != nil {
			return ListApiKeysResultResp{}, err
		}
		createdAt, err := r.ReadU64()
		if err != nil {
			return ListApiKeysResultResp{}, err
		}
		expiresAt, err := r.ReadU64()
		if err != nil {
			return ListApiKeysResultResp{}, err
		}
		isSuperadmin, err := r.ReadBool()
		if err != nil {
			return ListApiKeysResultResp{}, err
		}
		keys[i] = ListApiKeysKeyInfo{
			KeyID:        keyID,
			Name:         name,
			CreatedAtMs:  createdAt,
			ExpiresAtMs:  expiresAt,
			IsSuperadmin: isSuperadmin,
		}
	}
	return ListApiKeysResultResp{ErrorCode: ErrorCode(code), Keys: keys}, nil
}

// SetAclResultResp holds a decoded SetAclResult.
type SetAclResultResp struct {
	ErrorCode ErrorCode
}

// DecodeSetAclResult decodes a SetAclResult body.
func DecodeSetAclResult(data []byte) (SetAclResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return SetAclResultResp{}, err
	}
	return SetAclResultResp{ErrorCode: ErrorCode(code)}, nil
}

// GetAclResultResp holds a decoded GetAclResult.
type GetAclResultResp struct {
	ErrorCode    ErrorCode
	KeyID        string
	IsSuperadmin bool
	Permissions  []AclPermission
}

// DecodeGetAclResult decodes a GetAclResult body.
func DecodeGetAclResult(data []byte) (GetAclResultResp, error) {
	r := NewReader(data)
	code, err := r.ReadU8()
	if err != nil {
		return GetAclResultResp{}, err
	}
	keyID, err := r.ReadString()
	if err != nil {
		return GetAclResultResp{}, err
	}
	isSuperadmin, err := r.ReadBool()
	if err != nil {
		return GetAclResultResp{}, err
	}
	count, err := r.ReadU16()
	if err != nil {
		return GetAclResultResp{}, err
	}
	perms := make([]AclPermission, count)
	for i := uint16(0); i < count; i++ {
		kind, err := r.ReadString()
		if err != nil {
			return GetAclResultResp{}, err
		}
		pattern, err := r.ReadString()
		if err != nil {
			return GetAclResultResp{}, err
		}
		perms[i] = AclPermission{Kind: kind, Pattern: pattern}
	}
	return GetAclResultResp{
		ErrorCode:    ErrorCode(code),
		KeyID:        keyID,
		IsSuperadmin: isSuperadmin,
		Permissions:  perms,
	}, nil
}
