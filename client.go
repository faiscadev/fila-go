package fila

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	filav1 "github.com/faisca/fila-go/filav1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is an idiomatic Go client for the Fila message broker.
//
// It wraps the hot-path gRPC operations: Enqueue, Consume, Ack, Nack.
// The client is safe for concurrent use.
//
// By default, Enqueue() routes through an internal batcher that uses
// opportunistic batching (BatchModeAuto). Use WithBatchMode() to change
// the batching strategy.
type Client struct {
	conn    *grpc.ClientConn
	svc     filav1.FilaServiceClient
	opts    []DialOption
	batcher *batcher
}

// DialOption configures how the client connects to the broker.
type DialOption func(*dialOptions)

type dialOptions struct {
	grpcOpts    []grpc.DialOption
	caCertPEM   []byte
	clientCert  []byte
	clientKey   []byte
	apiKey      string
	batchMode   BatchMode
	hasTLS      bool
	hasAPIKey   bool
	hasBatch    bool
}

// WithGRPCDialOption adds a raw gRPC dial option for advanced configuration.
func WithGRPCDialOption(opt grpc.DialOption) DialOption {
	return func(o *dialOptions) {
		o.grpcOpts = append(o.grpcOpts, opt)
	}
}

// WithTLS enables TLS using the operating system's default root CA pool.
//
// Use this when the Fila server's certificate is issued by a public CA
// (e.g., Let's Encrypt) or a corporate CA already installed in the
// system trust store. No CA certificate file is needed.
func WithTLS() DialOption {
	return func(o *dialOptions) {
		o.hasTLS = true
	}
}

// WithTLSCACert configures TLS with a CA certificate for verifying the server.
//
// The caCertPEM should be a PEM-encoded CA certificate. When set, the
// connection uses TLS instead of plaintext. Use this when the server's
// CA is not in the system trust store (e.g., self-signed certificates).
func WithTLSCACert(caCertPEM []byte) DialOption {
	return func(o *dialOptions) {
		o.caCertPEM = caCertPEM
		o.hasTLS = true
	}
}

// WithTLSClientCert configures mTLS with a client certificate and key.
//
// Both certPEM and keyPEM should be PEM-encoded. This option must be
// used together with WithTLS or WithTLSCACert. When set, the client
// presents its certificate to the server for mutual TLS authentication.
func WithTLSClientCert(certPEM, keyPEM []byte) DialOption {
	return func(o *dialOptions) {
		o.clientCert = certPEM
		o.clientKey = keyPEM
	}
}

// WithAPIKey configures API key authentication.
//
// When set, every outgoing RPC includes an "authorization: Bearer <key>"
// metadata header. The server validates this key and applies per-key ACLs.
func WithAPIKey(key string) DialOption {
	return func(o *dialOptions) {
		o.apiKey = key
		o.hasAPIKey = true
	}
}

// WithBatchMode sets the batching strategy for Enqueue() calls.
//
// The default is BatchModeAuto{} (opportunistic batching). Use
// BatchModeLinger{} for timer-based batching, or BatchModeDisabled{}
// to send each Enqueue() as a direct RPC.
func WithBatchMode(mode BatchMode) DialOption {
	return func(o *dialOptions) {
		o.batchMode = mode
		o.hasBatch = true
	}
}

// apiKeyCredentials implements grpc.PerRPCCredentials to attach an API key
// as a Bearer token on every outgoing RPC.
type apiKeyCredentials struct {
	key string
	// requireTransportSecurity is true when TLS is configured.
	requireTransportSecurity bool
}

func (c *apiKeyCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + c.key,
	}, nil
}

func (c *apiKeyCredentials) RequireTransportSecurity() bool {
	return c.requireTransportSecurity
}

// Dial connects to a Fila broker at the given address.
//
// The address should be in the form "host:port" (e.g., "localhost:5555").
// Connection is established lazily on the first RPC call. Use context
// timeouts on individual operations to control deadlines.
//
// By default, a background batcher goroutine is started with
// BatchModeAuto. Call Close() to drain pending messages and shut down
// the batcher cleanly.
func Dial(addr string, opts ...DialOption) (*Client, error) {
	var do dialOptions
	for _, opt := range opts {
		opt(&do)
	}

	// Validate: WithTLSClientCert requires WithTLS or WithTLSCACert.
	if !do.hasTLS && (do.clientCert != nil || do.clientKey != nil) {
		return nil, errors.New("WithTLSClientCert requires WithTLS or WithTLSCACert: client certificate has no effect without TLS")
	}

	var grpcOpts []grpc.DialOption

	if do.hasTLS {
		tlsConfig, err := buildTLSConfig(do.caCertPEM, do.clientCert, do.clientKey)
		if err != nil {
			return nil, fmt.Errorf("tls config: %w", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if do.hasAPIKey {
		grpcOpts = append(grpcOpts, grpc.WithPerRPCCredentials(&apiKeyCredentials{
			key:                      do.apiKey,
			requireTransportSecurity: do.hasTLS,
		}))
	}

	grpcOpts = append(grpcOpts, do.grpcOpts...)

	conn, err := grpc.NewClient(addr, grpcOpts...)
	if err != nil {
		return nil, err
	}

	svc := filav1.NewFilaServiceClient(conn)

	// Determine batch mode.
	batchMode := do.batchMode
	if !do.hasBatch {
		batchMode = BatchModeAuto{}
	}

	c := &Client{
		conn: conn,
		svc:  svc,
		opts: opts,
	}

	// Start batcher unless disabled.
	if _, disabled := batchMode.(BatchModeDisabled); !disabled {
		c.batcher = newBatcher(svc, batchMode)
	}

	return c, nil
}

// Close drains any pending batched messages and closes the underlying
// gRPC connection.
func (c *Client) Close() error {
	if c.batcher != nil {
		c.batcher.drain()
	}
	return c.conn.Close()
}

// buildTLSConfig creates a *tls.Config from PEM-encoded certificates.
// When caCertPEM is nil, the system's default root CA pool is used.
func buildTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM []byte) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if caCertPEM != nil {
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCertPEM) {
			return nil, errors.New("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = certPool
	}

	// Reject partial mTLS config: both cert and key must be provided or neither.
	if (clientCertPEM != nil) != (clientKeyPEM != nil) {
		return nil, errors.New("both client certificate and key must be provided for mTLS")
	}

	if clientCertPEM != nil && clientKeyPEM != nil {
		cert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("failed to parse client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
