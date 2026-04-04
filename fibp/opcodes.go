package fibp

// Opcode identifies the operation in a frame header.
type Opcode uint8

// Control opcodes (0x00-0x0F).
const (
	OpcodeHandshake   Opcode = 0x01
	OpcodeHandshakeOk Opcode = 0x02
	OpcodePing        Opcode = 0x03
	OpcodePong        Opcode = 0x04
	OpcodeDisconnect  Opcode = 0x05
)

// Hot-path opcodes (0x10-0x1F).
const (
	OpcodeEnqueue       Opcode = 0x10
	OpcodeEnqueueResult Opcode = 0x11
	OpcodeConsume       Opcode = 0x12
	OpcodeDelivery      Opcode = 0x13
	OpcodeCancelConsume Opcode = 0x14
	OpcodeAck           Opcode = 0x15
	OpcodeAckResult     Opcode = 0x16
	OpcodeNack          Opcode = 0x17
	OpcodeNackResult    Opcode = 0x18
	OpcodeConsumeOk     Opcode = 0x19
)

// Error opcode.
const OpcodeError Opcode = 0xFE

// Admin opcodes (0xFD downward).
const (
	OpcodeCreateQueue       Opcode = 0xFD
	OpcodeCreateQueueResult Opcode = 0xFC
	OpcodeDeleteQueue       Opcode = 0xFB
	OpcodeDeleteQueueResult Opcode = 0xFA
	OpcodeGetStats          Opcode = 0xF9
	OpcodeGetStatsResult    Opcode = 0xF8
	OpcodeListQueues        Opcode = 0xF7
	OpcodeListQueuesResult  Opcode = 0xF6
	OpcodeSetConfig         Opcode = 0xF5
	OpcodeSetConfigResult   Opcode = 0xF4
	OpcodeGetConfig         Opcode = 0xF3
	OpcodeGetConfigResult   Opcode = 0xF2
	OpcodeListConfig        Opcode = 0xF1
	OpcodeListConfigResult  Opcode = 0xF0
	OpcodeRedrive           Opcode = 0xEF
	OpcodeRedriveResult     Opcode = 0xEE
	OpcodeCreateApiKey       Opcode = 0xED
	OpcodeCreateApiKeyResult Opcode = 0xEC
	OpcodeRevokeApiKey       Opcode = 0xEB
	OpcodeRevokeApiKeyResult Opcode = 0xEA
	OpcodeListApiKeys        Opcode = 0xE9
	OpcodeListApiKeysResult  Opcode = 0xE8
	OpcodeSetAcl             Opcode = 0xE7
	OpcodeSetAclResult       Opcode = 0xE6
	OpcodeGetAcl             Opcode = 0xE5
	OpcodeGetAclResult       Opcode = 0xE4
)

// ErrorCode identifies the type of error in Error frames and per-item results.
type ErrorCode uint8

const (
	ErrorOk                ErrorCode = 0x00
	ErrorQueueNotFound     ErrorCode = 0x01
	ErrorMessageNotFound   ErrorCode = 0x02
	ErrorQueueAlreadyExists ErrorCode = 0x03
	ErrorLuaCompilation    ErrorCode = 0x04
	ErrorStorage           ErrorCode = 0x05
	ErrorNotADLQ           ErrorCode = 0x06
	ErrorParentQueueNotFound ErrorCode = 0x07
	ErrorInvalidConfigValue ErrorCode = 0x08
	ErrorChannelFull       ErrorCode = 0x09
	ErrorUnauthorized      ErrorCode = 0x0A
	ErrorForbidden         ErrorCode = 0x0B
	ErrorNotLeader         ErrorCode = 0x0C
	ErrorUnsupportedVersion ErrorCode = 0x0D
	ErrorInvalidFrame      ErrorCode = 0x0E
	ErrorApiKeyNotFound    ErrorCode = 0x0F
	ErrorNodeNotReady      ErrorCode = 0x10
	ErrorInternal          ErrorCode = 0xFF
)

// String returns the human-readable name of an error code.
func (c ErrorCode) String() string {
	switch c {
	case ErrorOk:
		return "ok"
	case ErrorQueueNotFound:
		return "queue_not_found"
	case ErrorMessageNotFound:
		return "message_not_found"
	case ErrorQueueAlreadyExists:
		return "queue_already_exists"
	case ErrorLuaCompilation:
		return "lua_compilation_error"
	case ErrorStorage:
		return "storage_error"
	case ErrorNotADLQ:
		return "not_a_dlq"
	case ErrorParentQueueNotFound:
		return "parent_queue_not_found"
	case ErrorInvalidConfigValue:
		return "invalid_config_value"
	case ErrorChannelFull:
		return "channel_full"
	case ErrorUnauthorized:
		return "unauthorized"
	case ErrorForbidden:
		return "forbidden"
	case ErrorNotLeader:
		return "not_leader"
	case ErrorUnsupportedVersion:
		return "unsupported_version"
	case ErrorInvalidFrame:
		return "invalid_frame"
	case ErrorApiKeyNotFound:
		return "api_key_not_found"
	case ErrorNodeNotReady:
		return "node_not_ready"
	case ErrorInternal:
		return "internal_error"
	default:
		return "unknown"
	}
}

// Frame header constants.
const (
	HeaderSize       = 6 // opcode(1) + flags(1) + request_id(4)
	FlagContinuation = 0x01

	DefaultMaxFrameSize uint32 = 16 * 1024 * 1024 // 16 MiB
	ProtocolVersion     uint16 = 1
)

// FrameHeader is the 6-byte header present in every frame body.
type FrameHeader struct {
	Opcode    Opcode
	Flags     uint8
	RequestID uint32
}

// IsContinuation returns true if the CONTINUATION flag is set.
func (h FrameHeader) IsContinuation() bool {
	return h.Flags&FlagContinuation != 0
}

// EncodeHeader writes the 6-byte header into the writer.
func EncodeHeader(w *Writer, h FrameHeader) {
	w.WriteU8(uint8(h.Opcode))
	w.WriteU8(h.Flags)
	w.WriteU32(h.RequestID)
}

// DecodeHeader reads the 6-byte header from the reader.
func DecodeHeader(r *Reader) (FrameHeader, error) {
	opcode, err := r.ReadU8()
	if err != nil {
		return FrameHeader{}, err
	}
	flags, err := r.ReadU8()
	if err != nil {
		return FrameHeader{}, err
	}
	reqID, err := r.ReadU32()
	if err != nil {
		return FrameHeader{}, err
	}
	return FrameHeader{
		Opcode:    Opcode(opcode),
		Flags:     flags,
		RequestID: reqID,
	}, nil
}
