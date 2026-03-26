package fila

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// FIBP op codes.
const (
	opEnqueue   uint8 = 0x01
	opConsume   uint8 = 0x02
	opAck       uint8 = 0x03
	opNack      uint8 = 0x04
	opFlow      uint8 = 0x20
	opHeartbeat uint8 = 0x21
	opAuth      uint8 = 0x30
	opError     uint8 = 0xFE
	opGoAway    uint8 = 0xFF
)

// flagServerPush is set on frames the server pushes (consume delivery).
const flagServerPush uint8 = 0x04

// fibpHandshake is the 6-byte magic + version sent at connection start.
var fibpHandshake = [6]byte{'F', 'I', 'B', 'P', 0x01, 0x00}

// frame is a decoded FIBP frame.
type frame struct {
	flags   uint8
	op      uint8
	corrID  uint32
	payload []byte
}

// conn wraps a net.Conn providing FIBP framing and request/response
// multiplexing over a single TCP connection.
//
// The connection is safe for concurrent use: multiple goroutines may call
// send() simultaneously, and each will receive its response via the channel
// registered under its correlation ID.
//
// Consume streams are handled separately: incoming server-push frames
// (flagServerPush set) are routed to a per-corrID push channel rather
// than a one-shot response channel.
type conn struct {
	nc net.Conn

	// Guards writes to nc. Reads happen exclusively in the read loop.
	writeMu sync.Mutex

	// nextCorrID provides monotonically increasing correlation IDs.
	nextCorrID atomic.Uint32

	// pending maps corrID → response channel for one-shot request/response.
	pendingMu sync.Mutex
	pending   map[uint32]chan frame

	// streams maps corrID → push channel for long-lived consume streams.
	streamsMu sync.Mutex
	streams   map[uint32]chan frame

	// closeOnce ensures the connection is closed exactly once.
	closeOnce sync.Once
	closed    chan struct{}
}

// newConn dials addr, performs the FIBP handshake, starts the read loop,
// and returns the ready connection. If apiKey is non-empty an AUTH frame
// is sent immediately after the handshake.
func newConn(nc net.Conn, apiKey string) (*conn, error) {
	c := &conn{
		nc:      nc,
		pending: make(map[uint32]chan frame),
		streams: make(map[uint32]chan frame),
		closed:  make(chan struct{}),
	}

	// Handshake: send magic, expect echo.
	if _, err := nc.Write(fibpHandshake[:]); err != nil {
		nc.Close()
		return nil, fmt.Errorf("fibp handshake write: %w", err)
	}
	var echo [6]byte
	if _, err := io.ReadFull(nc, echo[:]); err != nil {
		nc.Close()
		return nil, fmt.Errorf("fibp handshake read: %w", err)
	}
	if echo != fibpHandshake {
		nc.Close()
		return nil, fmt.Errorf("fibp handshake: unexpected echo %x", echo)
	}

	// Authenticate if an API key was provided.
	if apiKey != "" {
		key := []byte(apiKey)
		payload := make([]byte, 2+len(key))
		binary.BigEndian.PutUint16(payload[0:2], uint16(len(key)))
		copy(payload[2:], key)
		corrID := c.nextCorrID.Add(1)
		if err := c.writeFrame(0, opAuth, corrID, payload); err != nil {
			nc.Close()
			return nil, fmt.Errorf("fibp auth: %w", err)
		}
		// Read auth response inline (before starting the read loop so we
		// don't need to register a pending channel yet).
		resp, err := readFrame(nc)
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("fibp auth response: %w", err)
		}
		if resp.op == opError {
			msg, _ := decodeErrorPayload(resp.payload)
			nc.Close()
			return nil, fmt.Errorf("fibp auth rejected: %s", msg)
		}
	}

	// Start background read loop.
	go c.readLoop()

	return c, nil
}

// readLoop drains incoming frames and routes them to waiting callers.
func (c *conn) readLoop() {
	defer func() {
		close(c.closed)
		c.nc.Close()

		// Drain all pending channels so callers unblock.
		c.pendingMu.Lock()
		for id, ch := range c.pending {
			close(ch)
			delete(c.pending, id)
		}
		c.pendingMu.Unlock()

		c.streamsMu.Lock()
		for id, ch := range c.streams {
			close(ch)
			delete(c.streams, id)
		}
		c.streamsMu.Unlock()
	}()

	for {
		f, err := readFrame(c.nc)
		if err != nil {
			return
		}

		if f.flags&flagServerPush != 0 {
			// Route to a long-lived stream channel.
			c.streamsMu.Lock()
			ch, ok := c.streams[f.corrID]
			c.streamsMu.Unlock()
			if ok {
				// Non-blocking send: if the consumer is slow, drop the frame
				// rather than blocking the read loop. The channel is buffered.
				select {
				case ch <- f:
				default:
				}
			}
		} else {
			// Route to a one-shot pending channel.
			c.pendingMu.Lock()
			ch, ok := c.pending[f.corrID]
			if ok {
				delete(c.pending, f.corrID)
			}
			c.pendingMu.Unlock()
			if ok {
				ch <- f
			}
		}
	}
}

// send writes a frame and waits for the correlated response.
func (c *conn) send(flags, op uint8, payload []byte) (frame, error) {
	corrID := c.nextCorrID.Add(1)

	ch := make(chan frame, 1)
	c.pendingMu.Lock()
	c.pending[corrID] = ch
	c.pendingMu.Unlock()

	if err := c.writeFrame(flags, op, corrID, payload); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, corrID)
		c.pendingMu.Unlock()
		return frame{}, err
	}

	resp, ok := <-ch
	if !ok {
		return frame{}, ErrConnectionClosed
	}
	return resp, nil
}

// openStream registers a push channel for a long-lived consume stream and
// sends the initial frame. The caller is responsible for calling
// closeStream when done.
func (c *conn) openStream(flags, op uint8, payload []byte) (uint32, <-chan frame, error) {
	corrID := c.nextCorrID.Add(1)

	ch := make(chan frame, 64)
	c.streamsMu.Lock()
	c.streams[corrID] = ch
	c.streamsMu.Unlock()

	if err := c.writeFrame(flags, op, corrID, payload); err != nil {
		c.streamsMu.Lock()
		delete(c.streams, corrID)
		c.streamsMu.Unlock()
		return 0, nil, err
	}
	return corrID, ch, nil
}

// closeStream removes a push channel and closes it.
func (c *conn) closeStream(corrID uint32) {
	c.streamsMu.Lock()
	ch, ok := c.streams[corrID]
	if ok {
		delete(c.streams, corrID)
	}
	c.streamsMu.Unlock()
	if ok {
		close(ch)
	}
}

// writeFrame encodes and writes a single FIBP frame.
// Frame layout: [4-byte length][flags:u8][op:u8][corrID:u32][payload]
// The 4-byte length covers everything after the length field itself:
// 1 (flags) + 1 (op) + 4 (corrID) + len(payload) = 6 + len(payload).
func (c *conn) writeFrame(flags, op uint8, corrID uint32, payload []byte) error {
	headerLen := 6 // flags(1) + op(1) + corrID(4)
	totalLen := headerLen + len(payload)

	buf := make([]byte, 4+totalLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))
	buf[4] = flags
	buf[5] = op
	binary.BigEndian.PutUint32(buf[6:10], corrID)
	copy(buf[10:], payload)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.nc.Write(buf)
	return err
}

// readFrame reads the next frame from r.
func readFrame(r io.Reader) (frame, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return frame{}, err
	}
	totalLen := binary.BigEndian.Uint32(lenBuf[:])
	if totalLen < 6 {
		return frame{}, fmt.Errorf("fibp: frame too short (%d bytes)", totalLen)
	}

	body := make([]byte, totalLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return frame{}, err
	}

	return frame{
		flags:   body[0],
		op:      body[1],
		corrID:  binary.BigEndian.Uint32(body[2:6]),
		payload: body[6:],
	}, nil
}

// decodeErrorPayload extracts the error message from an ERROR frame payload.
// Format: err_code:u16 | err_len:u16 | err_msg:utf8
func decodeErrorPayload(payload []byte) (string, uint16) {
	if len(payload) < 4 {
		return "unknown error", 0
	}
	code := binary.BigEndian.Uint16(payload[0:2])
	msgLen := binary.BigEndian.Uint16(payload[2:4])
	if len(payload) < 4+int(msgLen) {
		return "malformed error frame", code
	}
	return string(payload[4 : 4+msgLen]), code
}

// close shuts down the connection.
func (c *conn) close() {
	c.closeOnce.Do(func() {
		c.nc.Close()
	})
}
