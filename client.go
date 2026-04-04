package fila

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
)

// Client is an idiomatic Go client for the Fila message broker.
//
// It wraps all FIBP operations: Enqueue, Consume, Ack, Nack, and admin ops.
// The client is safe for concurrent use.
//
// By default, Enqueue() routes through an internal accumulator that uses
// opportunistic accumulation (AccumulatorModeAuto). Use
// WithAccumulatorMode() to change the accumulation strategy.
type Client struct {
	conn        *conn
	addr        string
	opts        []DialOption
	accumulator *accumulator
}

// DialOption configures how the client connects to the broker.
type DialOption func(*dialOptions)

type dialOptions struct {
	caCertPEM       []byte
	clientCert      []byte
	clientKey       []byte
	apiKey          string
	accumulatorMode AccumulatorMode
	hasTLS          bool
	hasAPIKey       bool
	hasAccumulator  bool
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
// When set, the API key is sent in the FIBP handshake frame. The server
// validates this key and applies per-key ACLs.
func WithAPIKey(key string) DialOption {
	return func(o *dialOptions) {
		o.apiKey = key
		o.hasAPIKey = true
	}
}

// WithAccumulatorMode sets the accumulation strategy for Enqueue() calls.
//
// The default is AccumulatorModeAuto{} (opportunistic accumulation). Use
// AccumulatorModeLinger{} for timer-based accumulation, or
// AccumulatorModeDisabled{} to send each Enqueue() as a direct call.
func WithAccumulatorMode(mode AccumulatorMode) DialOption {
	return func(o *dialOptions) {
		o.accumulatorMode = mode
		o.hasAccumulator = true
	}
}

// Dial connects to a Fila broker at the given address.
//
// The address should be in the form "host:port" (e.g., "localhost:5555").
// The connection is established immediately, including the FIBP handshake.
//
// By default, a background accumulator goroutine is started with
// AccumulatorModeAuto. Call Close() to drain pending messages and shut
// down the accumulator cleanly.
func Dial(addr string, opts ...DialOption) (*Client, error) {
	var do dialOptions
	for _, opt := range opts {
		opt(&do)
	}

	if !do.hasTLS && (do.clientCert != nil || do.clientKey != nil) {
		return nil, errors.New("WithTLSClientCert requires WithTLS or WithTLSCACert: client certificate has no effect without TLS")
	}

	c, err := dialConn(context.Background(), addr, &do)
	if err != nil {
		return nil, err
	}

	accMode := do.accumulatorMode
	if !do.hasAccumulator {
		accMode = AccumulatorModeAuto{}
	}

	client := &Client{
		conn: c,
		addr: addr,
		opts: opts,
	}

	disabled := false
	switch accMode.(type) {
	case AccumulatorModeDisabled, *AccumulatorModeDisabled:
		disabled = true
	}
	if !disabled {
		client.accumulator = newAccumulator(c, accMode)
	}

	return client, nil
}

// Close drains any pending accumulated messages and closes the underlying
// connection.
func (c *Client) Close() error {
	if c.accumulator != nil {
		c.accumulator.drain()
	}
	return c.conn.close()
}

// buildTLSConfig creates a *tls.Config from the dial options.
func buildTLSConfig(opts *dialOptions) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if opts.caCertPEM != nil {
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(opts.caCertPEM) {
			return nil, errors.New("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = certPool
	}

	if (opts.clientCert != nil) != (opts.clientKey != nil) {
		return nil, errors.New("both client certificate and key must be provided for mTLS")
	}

	if opts.clientCert != nil && opts.clientKey != nil {
		cert, err := tls.X509KeyPair(opts.clientCert, opts.clientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// dialTLS connects with TLS wrapping.
func dialTLS(ctx context.Context, dialer *net.Dialer, addr string, tlsConfig *tls.Config) (net.Conn, error) {
	netConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(netConn, tlsConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		netConn.Close()
		return nil, err
	}
	return tlsConn, nil
}
