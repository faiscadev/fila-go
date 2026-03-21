package fila_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	fila "github.com/faisca/fila-go"
	filav1 "github.com/faisca/fila-go/filav1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testServer manages a fila-server subprocess for integration tests.
type testServer struct {
	cmd     *exec.Cmd
	addr    string
	dataDir string
}

func findServerBinary() string {
	// Check FILA_SERVER_BIN env var first.
	if bin := os.Getenv("FILA_SERVER_BIN"); bin != "" {
		return bin
	}
	// Fall back to relative path from fila-go to fila repo.
	return filepath.Join("..", "fila", "target", "release", "fila-server")
}

func startTestServer(t *testing.T) *testServer {
	t.Helper()

	bin := findServerBinary()
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("fila-server binary not found at %s: %v", bin, err)
	}
	// Resolve to absolute path since cmd.Dir changes the working directory.
	absBin, absErr := filepath.Abs(bin)
	if absErr != nil {
		t.Fatalf("failed to resolve absolute path for binary: %v", absErr)
	}
	bin = absBin

	// Find a free port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// Create temp data dir.
	dataDir, err := os.MkdirTemp("", "fila-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Write a fila.toml config with the test listen address.
	configPath := filepath.Join(dataDir, "fila.toml")
	configContent := fmt.Sprintf("[server]\nlisten_addr = %q\n", addr)
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("failed to write fila.toml: %v", err)
	}

	cmd := exec.Command(bin)
	cmd.Dir = dataDir
	cmd.Env = append(os.Environ(), "FILA_DATA_DIR="+filepath.Join(dataDir, "db"))
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("failed to start fila-server: %v", err)
	}

	ts := &testServer{
		cmd:     cmd,
		addr:    addr,
		dataDir: dataDir,
	}

	// Wait for server to be ready by polling the gRPC endpoint.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			// Try a lightweight RPC to verify the server is responding.
			adminClient := filav1.NewFilaAdminClient(conn)
			_, listErr := adminClient.ListQueues(ctx, &filav1.ListQueuesRequest{})
			conn.Close()
			if listErr == nil {
				break
			}
		}

		select {
		case <-ctx.Done():
			ts.stop()
			t.Fatalf("fila-server did not become ready within 10s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	t.Cleanup(func() {
		ts.stop()
	})

	return ts
}

func (ts *testServer) stop() {
	if ts.cmd.Process != nil {
		_ = ts.cmd.Process.Kill()
		_ = ts.cmd.Wait()
	}
	os.RemoveAll(ts.dataDir)
}

// createQueue creates a queue on the test server using the admin gRPC client.
func createQueue(t *testing.T, addr, name string) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect for admin: %v", err)
	}
	defer conn.Close()

	adminClient := filav1.NewFilaAdminClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = adminClient.CreateQueue(ctx, &filav1.CreateQueueRequest{
		Name:   name,
		Config: &filav1.QueueConfig{},
	})
	if err != nil {
		t.Fatalf("failed to create queue %q: %v", name, err)
	}
}

func TestEnqueueConsumeAck(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-enqueue-consume-ack"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue a message.
	headers := map[string]string{"tenant": "acme"}
	payload := []byte("hello world")
	msgID, err := client.Enqueue(ctx, queueName, headers, payload)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}

	// Consume the message.
	ch, err := client.Consume(ctx, queueName)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	select {
	case msg := <-ch:
		if msg == nil {
			t.Fatal("channel closed without receiving a message")
		}
		if msg.ID != msgID {
			t.Errorf("expected message ID %q, got %q", msgID, msg.ID)
		}
		if msg.Headers["tenant"] != "acme" {
			t.Errorf("expected header tenant=acme, got %v", msg.Headers)
		}
		if string(msg.Payload) != "hello world" {
			t.Errorf("expected payload 'hello world', got %q", string(msg.Payload))
		}

		// Ack the message.
		if err := client.Ack(ctx, queueName, msg.ID); err != nil {
			t.Fatalf("ack failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestEnqueueConsumeNackRedeliver(t *testing.T) {
	ts := startTestServer(t)
	queueName := "test-nack-redeliver"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue a message.
	msgID, err := client.Enqueue(ctx, queueName, nil, []byte("retry-me"))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Open a consume stream — nacked messages are redelivered on the same stream.
	ch, err := client.Consume(ctx, queueName)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	// First delivery.
	select {
	case msg := <-ch:
		if msg == nil {
			t.Fatal("channel closed without receiving a message")
		}
		if msg.ID != msgID {
			t.Errorf("expected message ID %q, got %q", msgID, msg.ID)
		}
		if msg.AttemptCount != 0 {
			t.Errorf("expected attempt count 0, got %d", msg.AttemptCount)
		}

		// Nack the message.
		if err := client.Nack(ctx, queueName, msg.ID, "transient failure"); err != nil {
			t.Fatalf("nack failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for first delivery")
	}

	// Redelivery on the same stream.
	select {
	case msg := <-ch:
		if msg == nil {
			t.Fatal("channel closed without receiving redelivered message")
		}
		if msg.ID != msgID {
			t.Errorf("expected redelivered message ID %q, got %q", msgID, msg.ID)
		}
		if msg.AttemptCount != 1 {
			t.Errorf("expected attempt count 1, got %d", msg.AttemptCount)
		}

		// Ack to clean up.
		_ = client.Ack(ctx, queueName, msg.ID)
	case <-ctx.Done():
		t.Fatal("timeout waiting for redelivered message")
	}
}

func TestEnqueueNonexistentQueue(t *testing.T) {
	ts := startTestServer(t)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Enqueue(ctx, "does-not-exist", nil, []byte("test"))
	if err == nil {
		t.Fatal("expected error for nonexistent queue")
	}
	if !errors.Is(err, fila.ErrQueueNotFound) {
		t.Errorf("expected ErrQueueNotFound, got: %v", err)
	}
}
