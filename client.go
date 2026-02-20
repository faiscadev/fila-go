package fila

import (
	filav1 "github.com/faisca/fila-go/filav1"
	"google.golang.org/grpc"
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
	grpcOpts []grpc.DialOption
}

// WithGRPCDialOption adds a raw gRPC dial option for advanced configuration.
func WithGRPCDialOption(opt grpc.DialOption) DialOption {
	return func(o *dialOptions) {
		o.grpcOpts = append(o.grpcOpts, opt)
	}
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

	grpcOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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
