package fila

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faisca/fila-go/fibp"
)

// conn manages a TCP connection to a Fila server using the FIBP protocol.
// It handles request/response multiplexing, server-push delivery routing,
// and keepalive pings.
//
// Cleanup design:
// - Close() sends Disconnect, cancels the read loop, and closes the TCP conn.
// - The read loop goroutine detects conn close and exits.
// - All pending request waiters receive an error on conn close.
// - Delivery channels are closed when the read loop exits.
type conn struct {
	netConn  net.Conn
	writer   *fibp.FrameWriter
	reader   *fibp.FrameReader
	maxFrame uint32
	nodeID   uint64

	// Request ID counter.
	nextReqID atomic.Uint32

	// Pending request-response waiters.
	mu       sync.Mutex
	waiters  map[uint32]chan frameResult
	// Delivery subscribers keyed by consume request ID.
	delivery map[uint32]chan *fibp.DeliveryMessage
	closed   bool

	// Read loop management.
	done   chan struct{}
	cancel context.CancelFunc
}

type frameResult struct {
	header fibp.FrameHeader
	body   []byte
	err    error
}

// dialConn establishes a FIBP connection to the server.
func dialConn(ctx context.Context, addr string, opts *dialOptions) (*conn, error) {
	var netConn net.Conn
	var err error

	dialer := &net.Dialer{Timeout: 10 * time.Second}

	if opts.hasTLS {
		tlsConfig, tlsErr := buildTLSConfig(opts)
		if tlsErr != nil {
			return nil, fmt.Errorf("tls config: %w", tlsErr)
		}
		netConn, err = dialTLS(ctx, dialer, addr, tlsConfig)
	} else {
		netConn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	c := &conn{
		netConn:  netConn,
		writer:   fibp.NewFrameWriter(netConn, fibp.DefaultMaxFrameSize),
		reader:   fibp.NewFrameReader(netConn, fibp.DefaultMaxFrameSize),
		maxFrame: fibp.DefaultMaxFrameSize,
		waiters:  make(map[uint32]chan frameResult),
		delivery: make(map[uint32]chan *fibp.DeliveryMessage),
		done:     make(chan struct{}),
	}

	// Perform handshake.
	if err := c.handshake(opts); err != nil {
		netConn.Close()
		return nil, err
	}

	// Start read loop.
	readCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.readLoop(readCtx)

	return c, nil
}

func (c *conn) handshake(opts *dialOptions) error {
	var apiKey *string
	if opts.hasAPIKey {
		apiKey = &opts.apiKey
	}

	body, err := fibp.EncodeHandshake(fibp.ProtocolVersion, apiKey)
	if err != nil {
		return fmt.Errorf("encode handshake: %w", err)
	}

	if err := c.writer.WriteFrame(fibp.OpcodeHandshake, 0, body); err != nil {
		return fmt.Errorf("write handshake: %w", err)
	}

	hdr, respBody, err := c.reader.ReadFrame()
	if err != nil {
		return fmt.Errorf("read handshake response: %w", err)
	}

	if hdr.Opcode == fibp.OpcodeError {
		errResp, _ := fibp.DecodeError(respBody)
		return errorCodeToErr(errResp.Code, errResp.Message, errResp.Metadata)
	}

	if hdr.Opcode != fibp.OpcodeHandshakeOk {
		return fmt.Errorf("unexpected handshake response opcode: 0x%02x", hdr.Opcode)
	}

	resp, err := fibp.DecodeHandshakeOk(respBody)
	if err != nil {
		return fmt.Errorf("decode handshake ok: %w", err)
	}

	if resp.MaxFrameSize > 0 {
		c.maxFrame = resp.MaxFrameSize
		c.writer = fibp.NewFrameWriter(c.netConn, resp.MaxFrameSize)
		c.reader = fibp.NewFrameReader(c.netConn, resp.MaxFrameSize)
	}
	c.nodeID = resp.NodeID

	return nil
}

// readLoop reads frames from the connection and dispatches them.
func (c *conn) readLoop(ctx context.Context) {
	defer close(c.done)
	for {
		select {
		case <-ctx.Done():
			c.closeWaiters(fmt.Errorf("connection closed"))
			return
		default:
		}

		hdr, body, err := c.reader.ReadFrame()
		if err != nil {
			c.closeWaiters(fmt.Errorf("read error: %w", err))
			return
		}

		switch hdr.Opcode {
		case fibp.OpcodePing:
			// Respond with Pong using same request ID.
			_ = c.writer.WriteFrame(fibp.OpcodePong, hdr.RequestID, nil)

		case fibp.OpcodeDelivery:
			// Route to delivery subscriber.
			c.mu.Lock()
			ch, ok := c.delivery[hdr.RequestID]
			c.mu.Unlock()
			if ok {
				msgs, err := fibp.DecodeDelivery(body)
				if err == nil {
					for i := range msgs {
						select {
						case ch <- &msgs[i]:
						case <-ctx.Done():
							return
						}
					}
				}
			}

		case fibp.OpcodeConsumeOk:
			// Route to the waiter for the consume request.
			c.mu.Lock()
			ch, ok := c.waiters[hdr.RequestID]
			c.mu.Unlock()
			if ok {
				ch <- frameResult{header: hdr, body: body}
			}

		default:
			// Route to request waiter.
			c.mu.Lock()
			ch, ok := c.waiters[hdr.RequestID]
			c.mu.Unlock()
			if ok {
				ch <- frameResult{header: hdr, body: body}
			}
		}
	}
}

// closeWaiters notifies all pending waiters of an error and closes delivery channels.
func (c *conn) closeWaiters(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for id, ch := range c.waiters {
		ch <- frameResult{err: err}
		delete(c.waiters, id)
	}
	for id, ch := range c.delivery {
		close(ch)
		delete(c.delivery, id)
	}
}

// request sends a frame and waits for the response.
func (c *conn) request(ctx context.Context, opcode fibp.Opcode, body []byte) (fibp.FrameHeader, []byte, error) {
	reqID := c.nextReqID.Add(1)
	ch := make(chan frameResult, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fibp.FrameHeader{}, nil, fmt.Errorf("connection closed")
	}
	c.waiters[reqID] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.waiters, reqID)
		c.mu.Unlock()
	}()

	if err := c.writer.WriteFrame(opcode, reqID, body); err != nil {
		return fibp.FrameHeader{}, nil, fmt.Errorf("write frame: %w", err)
	}

	select {
	case res := <-ch:
		if res.err != nil {
			return fibp.FrameHeader{}, nil, res.err
		}
		// Check for Error frame response.
		if res.header.Opcode == fibp.OpcodeError {
			errResp, _ := fibp.DecodeError(res.body)
			return res.header, res.body, errorCodeToErr(errResp.Code, errResp.Message, errResp.Metadata)
		}
		return res.header, res.body, nil
	case <-ctx.Done():
		return fibp.FrameHeader{}, nil, ctx.Err()
	}
}

// subscribe sends a Consume frame and registers a delivery channel.
// Returns the consume request ID and the delivery channel.
func (c *conn) subscribe(ctx context.Context, queue string) (uint32, string, <-chan *fibp.DeliveryMessage, error) {
	reqID := c.nextReqID.Add(1)
	waitCh := make(chan frameResult, 1)
	deliveryCh := make(chan *fibp.DeliveryMessage, 64)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, "", nil, fmt.Errorf("connection closed")
	}
	c.waiters[reqID] = waitCh
	c.delivery[reqID] = deliveryCh
	c.mu.Unlock()

	body, err := fibp.EncodeConsume(queue)
	if err != nil {
		c.mu.Lock()
		delete(c.waiters, reqID)
		delete(c.delivery, reqID)
		c.mu.Unlock()
		return 0, "", nil, err
	}

	if err := c.writer.WriteFrame(fibp.OpcodeConsume, reqID, body); err != nil {
		c.mu.Lock()
		delete(c.waiters, reqID)
		delete(c.delivery, reqID)
		c.mu.Unlock()
		return 0, "", nil, fmt.Errorf("write consume: %w", err)
	}

	// Wait for ConsumeOk or Error.
	select {
	case res := <-waitCh:
		c.mu.Lock()
		delete(c.waiters, reqID)
		c.mu.Unlock()

		if res.err != nil {
			c.mu.Lock()
			delete(c.delivery, reqID)
			c.mu.Unlock()
			return 0, "", nil, res.err
		}
		if res.header.Opcode == fibp.OpcodeError {
			c.mu.Lock()
			delete(c.delivery, reqID)
			c.mu.Unlock()
			errResp, _ := fibp.DecodeError(res.body)
			return 0, "", nil, errorCodeToErr(errResp.Code, errResp.Message, errResp.Metadata)
		}
		consumeOk, err := fibp.DecodeConsumeOk(res.body)
		if err != nil {
			c.mu.Lock()
			delete(c.delivery, reqID)
			c.mu.Unlock()
			return 0, "", nil, err
		}
		return reqID, consumeOk.ConsumerID, deliveryCh, nil

	case <-ctx.Done():
		c.mu.Lock()
		delete(c.waiters, reqID)
		delete(c.delivery, reqID)
		c.mu.Unlock()
		return 0, "", nil, ctx.Err()
	}
}

// cancelConsume sends a CancelConsume frame and unregisters the delivery channel.
func (c *conn) cancelConsume(reqID uint32) {
	_ = c.writer.WriteFrame(fibp.OpcodeCancelConsume, reqID, nil)
	c.mu.Lock()
	if ch, ok := c.delivery[reqID]; ok {
		close(ch)
		delete(c.delivery, reqID)
	}
	c.mu.Unlock()
}

// close gracefully disconnects.
func (c *conn) close() error {
	_ = c.writer.WriteFrame(fibp.OpcodeDisconnect, 0, nil)
	c.cancel()
	c.netConn.Close()
	<-c.done
	return nil
}
