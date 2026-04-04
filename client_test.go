package fila

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/faisca/fila-go/fibp"
)

// mockServer is a test helper that speaks the FIBP server side over a net.Conn pair.
type mockServer struct {
	listener net.Listener
	addr     string
	handler  func(fr *fibp.FrameReader, fw *fibp.FrameWriter)
	wg       sync.WaitGroup
}

func newMockServer(t *testing.T, handler func(fr *fibp.FrameReader, fw *fibp.FrameWriter)) *mockServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &mockServer{
		listener: l,
		addr:     l.Addr().String(),
		handler:  handler,
	}
	s.wg.Add(1)
	go s.accept()
	return s
}

func (s *mockServer) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			fr := fibp.NewFrameReader(conn, fibp.DefaultMaxFrameSize)
			fw := fibp.NewFrameWriter(conn, fibp.DefaultMaxFrameSize)

			// Handle handshake.
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode != fibp.OpcodeHandshake {
				return
			}

			// Send HandshakeOk.
			w := fibp.NewWriter(32)
			w.WriteU16(fibp.ProtocolVersion)
			w.WriteU64(1) // nodeID
			w.WriteU32(0) // default max frame
			_ = fw.WriteFrame(fibp.OpcodeHandshakeOk, 0, w.Bytes())

			s.handler(fr, fw)
		}()
	}
}

func (s *mockServer) close() {
	s.listener.Close()
	s.wg.Wait()
}

func TestDialAndClose(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		// Just accept and wait for disconnect.
		for {
			_, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueDirect(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeEnqueue {
				w := fibp.NewWriter(32)
				w.WriteU32(1) // result_count
				w.WriteU8(0x00)
				_ = w.WriteString("msg-uuid-1")
				_ = fw.WriteFrame(fibp.OpcodeEnqueueResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msgID, err := c.Enqueue(ctx, "test-queue", map[string]string{"k": "v"}, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if msgID != "msg-uuid-1" {
		t.Fatalf("expected msg-uuid-1, got %s", msgID)
	}
}

func TestEnqueueWithAccumulator(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, body, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeEnqueue {
				r := fibp.NewReader(body)
				count, _ := r.ReadU32()
				w := fibp.NewWriter(int(count) * 20)
				w.WriteU32(count)
				for i := uint32(0); i < count; i++ {
					w.WriteU8(0x00)
					_ = w.WriteString("id-" + string(rune('a'+i)))
				}
				_ = fw.WriteFrame(fibp.OpcodeEnqueueResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr) // Default accumulator
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msgID, err := c.Enqueue(ctx, "q", nil, []byte("msg"))
	if err != nil {
		t.Fatal(err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}
}

func TestEnqueueMany(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeEnqueue {
				w := fibp.NewWriter(64)
				w.WriteU32(2)
				w.WriteU8(0x00) // success
				_ = w.WriteString("id-1")
				w.WriteU8(0x01) // QueueNotFound
				_ = w.WriteString("")
				_ = fw.WriteFrame(fibp.OpcodeEnqueueResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	results, err := c.EnqueueMany(ctx, []EnqueueMessage{
		{Queue: "q1", Payload: []byte("a")},
		{Queue: "bad-q", Payload: []byte("b")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].MessageID != "id-1" {
		t.Fatalf("expected id-1, got %s", results[0].MessageID)
	}
	if results[1].Err == nil {
		t.Fatal("expected error for second message")
	}
	if !errors.Is(results[1].Err, ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got %v", results[1].Err)
	}
}

func TestAck(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeAck {
				w := fibp.NewWriter(16)
				w.WriteU32(1)
				w.WriteU8(0x00)
				_ = fw.WriteFrame(fibp.OpcodeAckResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Ack(ctx, "q", "msg-1"); err != nil {
		t.Fatal(err)
	}
}

func TestNack(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeNack {
				w := fibp.NewWriter(16)
				w.WriteU32(1)
				w.WriteU8(0x00)
				_ = fw.WriteFrame(fibp.OpcodeNackResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Nack(ctx, "q", "msg-1", "oops"); err != nil {
		t.Fatal(err)
	}
}

func TestConsume(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			switch hdr.Opcode {
			case fibp.OpcodeConsume:
				// Send ConsumeOk.
				w := fibp.NewWriter(16)
				_ = w.WriteString("consumer-1")
				_ = fw.WriteFrame(fibp.OpcodeConsumeOk, hdr.RequestID, w.Bytes())

				// Send a delivery.
				dw := fibp.NewWriter(128)
				dw.WriteU32(1)
				_ = dw.WriteString("msg-1")
				_ = dw.WriteString("test-q")
				_ = dw.WriteStringMap(nil)
				dw.WriteBytes([]byte("hello"))
				_ = dw.WriteString("")   // fairness_key
				dw.WriteU32(1)            // weight
				_ = dw.WriteStringSlice(nil) // throttle_keys
				dw.WriteU32(1)            // attempt_count
				dw.WriteU64(1000)         // enqueued_at
				dw.WriteU64(2000)         // leased_at
				_ = fw.WriteFrame(fibp.OpcodeDelivery, hdr.RequestID, dw.Bytes())

			case fibp.OpcodeCancelConsume:
				return

			case fibp.OpcodeAck:
				w := fibp.NewWriter(16)
				w.WriteU32(1)
				w.WriteU8(0x00)
				_ = fw.WriteFrame(fibp.OpcodeAckResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, err := c.Consume(ctx, "test-q")
	if err != nil {
		t.Fatal(err)
	}

	msg := <-ch
	if msg == nil {
		t.Fatal("expected a message")
	}
	if msg.ID != "msg-1" {
		t.Fatalf("expected msg-1, got %s", msg.ID)
	}
	if string(msg.Payload) != "hello" {
		t.Fatalf("expected hello, got %s", string(msg.Payload))
	}

	cancel()
}

func TestErrorResponse(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeEnqueue {
				w := fibp.NewWriter(32)
				w.WriteU8(0x0A) // Unauthorized
				_ = w.WriteString("invalid api key")
				_ = w.WriteStringMap(nil)
				_ = fw.WriteFrame(fibp.OpcodeError, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = c.Enqueue(ctx, "q", nil, []byte("msg"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func TestNotLeaderRedirect(t *testing.T) {
	// We test that NotLeader with leader hint causes a ProtocolError with LeaderAddr.
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeConsume {
				w := fibp.NewWriter(64)
				w.WriteU8(0x0C) // NotLeader
				_ = w.WriteString("not the leader for this queue")
				_ = w.WriteStringMap(map[string]string{"leader_addr": "other-host:5555"})
				_ = fw.WriteFrame(fibp.OpcodeError, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// The consume should fail because the leader redirect won't connect.
	_, err = c.Consume(ctx, "q")
	if err == nil {
		t.Fatal("expected error from NotLeader redirect failure")
	}
}

func TestTLSClientCertWithoutTLS(t *testing.T) {
	_, err := Dial("localhost:0", WithTLSClientCert([]byte("cert"), []byte("key")))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAdminCreateDeleteQueue(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			switch hdr.Opcode {
			case fibp.OpcodeCreateQueue:
				w := fibp.NewWriter(32)
				w.WriteU8(0x00)
				_ = w.WriteString("q-id-1")
				_ = fw.WriteFrame(fibp.OpcodeCreateQueueResult, hdr.RequestID, w.Bytes())
			case fibp.OpcodeDeleteQueue:
				w := fibp.NewWriter(8)
				w.WriteU8(0x00)
				_ = fw.WriteFrame(fibp.OpcodeDeleteQueueResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	qID, err := c.CreateQueue(ctx, "myqueue", nil)
	if err != nil {
		t.Fatal(err)
	}
	if qID != "q-id-1" {
		t.Fatalf("expected q-id-1, got %s", qID)
	}

	if err := c.DeleteQueue(ctx, "myqueue"); err != nil {
		t.Fatal(err)
	}
}

func TestAdminListQueues(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeListQueues {
				w := fibp.NewWriter(64)
				w.WriteU8(0x00)
				w.WriteU32(0) // cluster_node_count
				w.WriteU16(1) // queue_count
				_ = w.WriteString("q1")
				w.WriteU64(10) // depth
				w.WriteU64(2)  // in_flight
				w.WriteU32(1)  // active_consumers
				w.WriteU64(0)  // leader_node_id
				_ = fw.WriteFrame(fibp.OpcodeListQueuesResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := c.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Queues) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(result.Queues))
	}
	if result.Queues[0].Name != "q1" {
		t.Fatalf("expected q1, got %s", result.Queues[0].Name)
	}
	if result.Queues[0].Depth != 10 {
		t.Fatalf("expected depth 10, got %d", result.Queues[0].Depth)
	}
}

func TestAuthCreateApiKey(t *testing.T) {
	s := newMockServer(t, func(fr *fibp.FrameReader, fw *fibp.FrameWriter) {
		for {
			hdr, _, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if hdr.Opcode == fibp.OpcodeCreateApiKey {
				w := fibp.NewWriter(64)
				w.WriteU8(0x00)
				_ = w.WriteString("key-id-1")
				_ = w.WriteString("raw-key-abc")
				w.WriteBool(true)
				_ = fw.WriteFrame(fibp.OpcodeCreateApiKeyResult, hdr.RequestID, w.Bytes())
			}
		}
	})
	defer s.close()

	c, err := Dial(s.addr, WithAccumulatorMode(AccumulatorModeDisabled{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	info, err := c.CreateApiKey(ctx, "test-key", time.Time{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if info.KeyID != "key-id-1" {
		t.Fatalf("expected key-id-1, got %s", info.KeyID)
	}
	if info.Key != "raw-key-abc" {
		t.Fatalf("expected raw-key-abc, got %s", info.Key)
	}
	if !info.IsSuperadmin {
		t.Fatal("expected superadmin")
	}
}

