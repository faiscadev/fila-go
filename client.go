package fila

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
)

// Client is an idiomatic Go client for the Fila message broker.
//
// It communicates with the broker using FIBP (Fila Binary Protocol) over a
// raw TCP connection. The client is safe for concurrent use.
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
// When set, the API key is sent as an AUTH frame immediately after the
// FIBP handshake. The server validates the key and applies per-key ACLs.
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
// The TCP connection and FIBP handshake are performed eagerly during Dial.
// Use context timeouts on individual operations to control per-operation
// deadlines.
//
// By default, a background accumulator goroutine is started with
// AccumulatorModeAuto. Call Close() to drain pending messages and shut
// down the accumulator cleanly.
func Dial(addr string, opts ...DialOption) (*Client, error) {
	var do dialOptions
	for _, opt := range opts {
		opt(&do)
	}

	// Validate: WithTLSClientCert requires WithTLS or WithTLSCACert.
	if !do.hasTLS && (do.clientCert != nil || do.clientKey != nil) {
		return nil, errors.New("WithTLSClientCert requires WithTLS or WithTLSCACert: client certificate has no effect without TLS")
	}

	var nc net.Conn
	var err error

	if do.hasTLS {
		tlsConfig, err := buildTLSConfig(do.caCertPEM, do.clientCert, do.clientKey)
		if err != nil {
			return nil, fmt.Errorf("tls config: %w", err)
		}
		nc, err = tls.Dial("tcp", addr, tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("fibp dial: %w", err)
		}
	} else {
		nc, err = net.Dial("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("fibp dial: %w", err)
		}
	}

	apiKey := ""
	if do.hasAPIKey {
		apiKey = do.apiKey
	}

	c, err := newConn(nc, apiKey)
	if err != nil {
		return nil, err
	}

	// Determine accumulator mode.
	accMode := do.accumulatorMode
	if !do.hasAccumulator {
		accMode = AccumulatorModeAuto{}
	}

	client := &Client{
		conn: c,
		addr: addr,
		opts: opts,
	}

	// Start accumulator unless disabled.
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
// FIBP connection.
func (c *Client) Close() error {
	if c.accumulator != nil {
		c.accumulator.drain()
	}
	c.conn.close()
	return nil
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
