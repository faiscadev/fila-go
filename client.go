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
type Client struct {
	conn *grpc.ClientConn
	svc  filav1.FilaServiceClient
}

// DialOption configures how the client connects to the broker.
type DialOption func(*dialOptions)

type dialOptions struct {
	grpcOpts    []grpc.DialOption
	caCertPEM   []byte
	clientCert  []byte
	clientKey   []byte
	apiKey      string
	hasTLS      bool
	hasAPIKey   bool
}

// WithGRPCDialOption adds a raw gRPC dial option for advanced configuration.
func WithGRPCDialOption(opt grpc.DialOption) DialOption {
	return func(o *dialOptions) {
		o.grpcOpts = append(o.grpcOpts, opt)
	}
}

// WithTLSCACert configures TLS with a CA certificate for verifying the server.
//
// The caCertPEM should be a PEM-encoded CA certificate. When set, the
// connection uses TLS instead of plaintext. Required for connecting to
// a TLS-enabled broker.
func WithTLSCACert(caCertPEM []byte) DialOption {
	return func(o *dialOptions) {
		o.caCertPEM = caCertPEM
		o.hasTLS = true
	}
}

// WithTLSClientCert configures mTLS with a client certificate and key.
//
// Both certPEM and keyPEM should be PEM-encoded. This option must be
// used together with WithTLSCACert. When set, the client presents its
// certificate to the server for mutual TLS authentication.
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
func Dial(addr string, opts ...DialOption) (*Client, error) {
	var do dialOptions
	for _, opt := range opts {
		opt(&do)
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

	return &Client{
		conn: conn,
		svc:  filav1.NewFilaServiceClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// buildTLSConfig creates a *tls.Config from PEM-encoded certificates.
func buildTLSConfig(caCertPEM, clientCertPEM, clientKeyPEM []byte) (*tls.Config, error) {
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCertPEM) {
		return nil, errors.New("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
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
