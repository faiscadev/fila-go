package fila_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	fila "github.com/faisca/fila-go"
	filav1 "github.com/faisca/fila-go/filav1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// generateTestCerts creates a self-signed CA, server cert, and client cert
// for integration testing. Returns (caCertPEM, serverCertPEM, serverKeyPEM,
// clientCertPEM, clientKeyPEM).
func generateTestCerts(t *testing.T) ([]byte, []byte, []byte, []byte, []byte) {
	t.Helper()

	// Generate CA key and certificate.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create CA certificate: %v", err)
	}
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("failed to parse CA certificate: %v", err)
	}

	// Generate server key and certificate.
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate server key: %v", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create server certificate: %v", err)
	}
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})

	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		t.Fatalf("failed to marshal server key: %v", err)
	}
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER})

	// Generate client key and certificate.
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate client key: %v", err)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create client certificate: %v", err)
	}
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})

	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatalf("failed to marshal client key: %v", err)
	}
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyDER})

	return caCertPEM, serverCertPEM, serverKeyPEM, clientCertPEM, clientKeyPEM
}

// startTLSTestServer starts a fila-server with TLS and optional mTLS enabled.
// When requireClientCert is true, clientCertPEM and clientKeyPEM are used for
// the health-check polling connection.
func startTLSTestServer(t *testing.T, caCertPEM, serverCertPEM, serverKeyPEM, clientCertPEM, clientKeyPEM []byte, requireClientCert bool) *testServer {
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

	// Create temp data dir and write cert files.
	dataDir, err := os.MkdirTemp("", "fila-tls-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	certFile := filepath.Join(dataDir, "server.crt")
	keyFile := filepath.Join(dataDir, "server.key")
	caFile := filepath.Join(dataDir, "ca.crt")

	if err := os.WriteFile(certFile, serverCertPEM, 0644); err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("failed to write server cert: %v", err)
	}
	if err := os.WriteFile(keyFile, serverKeyPEM, 0600); err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("failed to write server key: %v", err)
	}
	if err := os.WriteFile(caFile, caCertPEM, 0644); err != nil {
		os.RemoveAll(dataDir)
		t.Fatalf("failed to write CA cert: %v", err)
	}

	// Write fila.toml with TLS config.
	configContent := fmt.Sprintf("[server]\nlisten_addr = %q\n\n[tls]\ncert_file = %q\nkey_file = %q\n", addr, certFile, keyFile)
	if requireClientCert {
		configContent += fmt.Sprintf("ca_file = %q\n", caFile)
	}

	configPath := filepath.Join(dataDir, "fila.toml")
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

	// Wait for server to be ready by polling the gRPC endpoint with TLS.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build TLS config for polling (with client cert if mTLS is required).
	pollTLSCfg, err := buildTestTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM)
	if err != nil {
		ts.stop()
		t.Fatalf("failed to build TLS config for polling: %v", err)
	}
	tlsCreds := credentials.NewTLS(pollTLSCfg)

	for {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(tlsCreds))
		if err == nil {
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
			t.Fatalf("fila-server (TLS) did not become ready within 10s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	t.Cleanup(func() {
		ts.stop()
	})

	return ts
}

// startAuthTestServer starts a fila-server with API key authentication enabled.
func startAuthTestServer(t *testing.T, bootstrapKey string) *testServer {
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

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	dataDir, err := os.MkdirTemp("", "fila-auth-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	configContent := fmt.Sprintf("[server]\nlisten_addr = %q\n\n[auth]\nbootstrap_apikey = %q\n", addr, bootstrapKey)
	configPath := filepath.Join(dataDir, "fila.toml")
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

	// Wait for server to be ready. Auth-enabled server still needs the key
	// to respond, so poll with the bootstrap key.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	creds := &apiKeyTestCredentials{key: bootstrapKey}

	for {
		conn, err := grpc.NewClient(addr,
			insecureTransportCredentials(),
			grpc.WithPerRPCCredentials(creds),
		)
		if err == nil {
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
			t.Fatalf("fila-server (auth) did not become ready within 10s")
		case <-time.After(50 * time.Millisecond):
		}
	}

	t.Cleanup(func() {
		ts.stop()
	})

	return ts
}

// apiKeyTestCredentials is a test helper implementing grpc.PerRPCCredentials.
type apiKeyTestCredentials struct {
	key string
}

func (c *apiKeyTestCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + c.key,
	}, nil
}

func (c *apiKeyTestCredentials) RequireTransportSecurity() bool {
	return false
}

func insecureTransportCredentials() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// createQueueWithTLS creates a queue on a TLS-enabled test server.
func createQueueWithTLS(t *testing.T, addr, name string, caCertPEM, clientCertPEM, clientKeyPEM []byte) {
	t.Helper()

	tlsCfg, err := buildTestTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM)
	if err != nil {
		t.Fatalf("failed to build TLS config: %v", err)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		t.Fatalf("failed to connect for admin (TLS): %v", err)
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

// createQueueWithAPIKey creates a queue on an auth-enabled test server.
func createQueueWithAPIKey(t *testing.T, addr, name, apiKey string) {
	t.Helper()

	creds := &apiKeyTestCredentials{key: apiKey}
	conn, err := grpc.NewClient(addr,
		insecureTransportCredentials(),
		grpc.WithPerRPCCredentials(creds),
	)
	if err != nil {
		t.Fatalf("failed to connect for admin (auth): %v", err)
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

func buildTestTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM []byte) (*tls.Config, error) {
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCertPEM) {
		return nil, fmt.Errorf("failed to parse CA cert")
	}
	cfg := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
	}
	if clientCertPEM != nil && clientKeyPEM != nil {
		cert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func TestTLSConnection(t *testing.T) {
	caCert, serverCert, serverKey, clientCert, clientKey := generateTestCerts(t)

	// Start server with mTLS.
	ts := startTLSTestServer(t, caCert, serverCert, serverKey, clientCert, clientKey, true)
	queueName := "test-tls"
	createQueueWithTLS(t, ts.addr, queueName, caCert, clientCert, clientKey)

	// Connect via SDK with TLS + client cert.
	client, err := fila.Dial(ts.addr,
		fila.WithTLSCACert(caCert),
		fila.WithTLSClientCert(clientCert, clientKey),
	)
	if err != nil {
		t.Fatalf("failed to dial with TLS: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue and verify it works through TLS.
	msgID, err := client.Enqueue(ctx, queueName, nil, []byte("tls-test"))
	if err != nil {
		t.Fatalf("enqueue over TLS failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}
}

func TestAPIKeyAuth(t *testing.T) {
	bootstrapKey := "test-bootstrap-key-12345"
	ts := startAuthTestServer(t, bootstrapKey)
	queueName := "test-apikey"
	createQueueWithAPIKey(t, ts.addr, queueName, bootstrapKey)

	// Connect via SDK with API key.
	client, err := fila.Dial(ts.addr, fila.WithAPIKey(bootstrapKey))
	if err != nil {
		t.Fatalf("failed to dial with API key: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue should succeed with valid key.
	msgID, err := client.Enqueue(ctx, queueName, nil, []byte("auth-test"))
	if err != nil {
		t.Fatalf("enqueue with API key failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}
}

func TestAPIKeyAuthRejected(t *testing.T) {
	bootstrapKey := "test-bootstrap-key-12345"
	ts := startAuthTestServer(t, bootstrapKey)
	queueName := "test-apikey-reject"
	createQueueWithAPIKey(t, ts.addr, queueName, bootstrapKey)

	// Connect via SDK with wrong API key.
	client, err := fila.Dial(ts.addr, fila.WithAPIKey("wrong-key"))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue should fail with wrong key.
	_, err = client.Enqueue(ctx, queueName, nil, []byte("should-fail"))
	if err == nil {
		t.Fatal("expected error with wrong API key, got nil")
	}
}

func TestNoAuthBackwardCompatible(t *testing.T) {
	// Start a server without auth — Dial() with no options should work.
	ts := startTestServer(t)
	queueName := "test-no-auth-compat"
	createQueue(t, ts.addr, queueName)

	client, err := fila.Dial(ts.addr)
	if err != nil {
		t.Fatalf("failed to dial without auth: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgID, err := client.Enqueue(ctx, queueName, nil, []byte("compat-test"))
	if err != nil {
		t.Fatalf("enqueue without auth failed: %v", err)
	}
	if msgID == "" {
		t.Fatal("expected non-empty message ID")
	}
}
