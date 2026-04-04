package fibp

// EncodeHandshake encodes a Handshake request body.
func EncodeHandshake(version uint16, apiKey *string) ([]byte, error) {
	w := NewWriter(32)
	w.WriteU16(version)
	if err := w.WriteOptionalString(apiKey); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EnqueueMessageReq is a single message in an Enqueue request.
type EnqueueMessageReq struct {
	Queue   string
	Headers map[string]string
	Payload []byte
}

// EncodeEnqueue encodes an Enqueue request body.
func EncodeEnqueue(messages []EnqueueMessageReq) ([]byte, error) {
	w := NewWriter(256)
	w.WriteU32(uint32(len(messages)))
	for _, msg := range messages {
		if err := w.WriteString(msg.Queue); err != nil {
			return nil, err
		}
		if err := w.WriteStringMap(msg.Headers); err != nil {
			return nil, err
		}
		w.WriteBytes(msg.Payload)
	}
	return w.Bytes(), nil
}

// EncodeConsume encodes a Consume request body.
func EncodeConsume(queue string) ([]byte, error) {
	w := NewWriter(32)
	if err := w.WriteString(queue); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EncodeCancelConsume encodes a CancelConsume request body (empty body).
func EncodeCancelConsume() []byte {
	return nil
}

// AckItem is a single item in an Ack request.
type AckItem struct {
	Queue     string
	MessageID string
}

// EncodeAck encodes an Ack request body.
func EncodeAck(items []AckItem) ([]byte, error) {
	w := NewWriter(128)
	w.WriteU32(uint32(len(items)))
	for _, item := range items {
		if err := w.WriteString(item.Queue); err != nil {
			return nil, err
		}
		if err := w.WriteString(item.MessageID); err != nil {
			return nil, err
		}
	}
	return w.Bytes(), nil
}

// NackItem is a single item in a Nack request.
type NackItem struct {
	Queue     string
	MessageID string
	Error     string
}

// EncodeNack encodes a Nack request body.
func EncodeNack(items []NackItem) ([]byte, error) {
	w := NewWriter(128)
	w.WriteU32(uint32(len(items)))
	for _, item := range items {
		if err := w.WriteString(item.Queue); err != nil {
			return nil, err
		}
		if err := w.WriteString(item.MessageID); err != nil {
			return nil, err
		}
		if err := w.WriteString(item.Error); err != nil {
			return nil, err
		}
	}
	return w.Bytes(), nil
}

// EncodePing encodes a Ping frame body (empty).
func EncodePing() []byte { return nil }

// EncodePong encodes a Pong frame body (empty).
func EncodePong() []byte { return nil }

// EncodeDisconnect encodes a Disconnect frame body (empty).
func EncodeDisconnect() []byte { return nil }

// --- Admin request encoders ---

// EncodeCreateQueue encodes a CreateQueue request body.
func EncodeCreateQueue(name string, onEnqueueScript, onFailureScript *string, visibilityTimeoutMs uint64) ([]byte, error) {
	w := NewWriter(128)
	if err := w.WriteString(name); err != nil {
		return nil, err
	}
	if err := w.WriteOptionalString(onEnqueueScript); err != nil {
		return nil, err
	}
	if err := w.WriteOptionalString(onFailureScript); err != nil {
		return nil, err
	}
	w.WriteU64(visibilityTimeoutMs)
	return w.Bytes(), nil
}

// EncodeDeleteQueue encodes a DeleteQueue request body.
func EncodeDeleteQueue(queue string) ([]byte, error) {
	w := NewWriter(32)
	if err := w.WriteString(queue); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EncodeGetStats encodes a GetStats request body.
func EncodeGetStats(queue string) ([]byte, error) {
	w := NewWriter(32)
	if err := w.WriteString(queue); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EncodeListQueues encodes a ListQueues request body (empty).
func EncodeListQueues() []byte { return nil }

// EncodeSetConfig encodes a SetConfig request body.
func EncodeSetConfig(key, value string) ([]byte, error) {
	w := NewWriter(64)
	if err := w.WriteString(key); err != nil {
		return nil, err
	}
	if err := w.WriteString(value); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EncodeGetConfig encodes a GetConfig request body.
func EncodeGetConfig(key string) ([]byte, error) {
	w := NewWriter(32)
	if err := w.WriteString(key); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EncodeListConfig encodes a ListConfig request body.
func EncodeListConfig(prefix string) ([]byte, error) {
	w := NewWriter(32)
	if err := w.WriteString(prefix); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EncodeRedrive encodes a Redrive request body.
func EncodeRedrive(dlqQueue string, count uint64) ([]byte, error) {
	w := NewWriter(32)
	if err := w.WriteString(dlqQueue); err != nil {
		return nil, err
	}
	w.WriteU64(count)
	return w.Bytes(), nil
}

// --- Auth request encoders ---

// EncodeCreateApiKey encodes a CreateApiKey request body.
func EncodeCreateApiKey(name string, expiresAtMs uint64, isSuperadmin bool) ([]byte, error) {
	w := NewWriter(64)
	if err := w.WriteString(name); err != nil {
		return nil, err
	}
	w.WriteU64(expiresAtMs)
	w.WriteBool(isSuperadmin)
	return w.Bytes(), nil
}

// EncodeRevokeApiKey encodes a RevokeApiKey request body.
func EncodeRevokeApiKey(keyID string) ([]byte, error) {
	w := NewWriter(32)
	if err := w.WriteString(keyID); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EncodeListApiKeys encodes a ListApiKeys request body (empty).
func EncodeListApiKeys() []byte { return nil }

// AclPermission is a single permission in SetAcl/GetAcl.
type AclPermission struct {
	Kind    string // "produce", "consume", or "admin"
	Pattern string
}

// EncodeSetAcl encodes a SetAcl request body.
func EncodeSetAcl(keyID string, permissions []AclPermission) ([]byte, error) {
	w := NewWriter(128)
	if err := w.WriteString(keyID); err != nil {
		return nil, err
	}
	w.WriteU16(uint16(len(permissions)))
	for _, p := range permissions {
		if err := w.WriteString(p.Kind); err != nil {
			return nil, err
		}
		if err := w.WriteString(p.Pattern); err != nil {
			return nil, err
		}
	}
	return w.Bytes(), nil
}

// EncodeGetAcl encodes a GetAcl request body.
func EncodeGetAcl(keyID string) ([]byte, error) {
	w := NewWriter(32)
	if err := w.WriteString(keyID); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}
