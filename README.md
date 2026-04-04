# fila-go

Go client SDK for the [Fila](https://github.com/faisca/fila) message broker.

Communicates over Fila's custom binary protocol (FIBP) for minimal transport overhead. Zero external dependencies — uses only the Go standard library.

## Installation

```bash
go get github.com/faisca/fila-go
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"

	fila "github.com/faisca/fila-go"
)

func main() {
	client, err := fila.Dial("localhost:5555")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Enqueue a message.
	msgID, err := client.Enqueue(ctx, "my-queue", map[string]string{
		"tenant": "acme",
	}, []byte("hello world"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Enqueued:", msgID)

	// Consume messages.
	ch, err := client.Consume(ctx, "my-queue")
	if err != nil {
		log.Fatal(err)
	}

	for msg := range ch {
		fmt.Printf("Received: %s (attempt %d)\n", msg.ID, msg.AttemptCount)

		// Acknowledge successful processing.
		if err := client.Ack(ctx, "my-queue", msg.ID); err != nil {
			// Negative acknowledge on failure.
			client.Nack(ctx, "my-queue", msg.ID, err.Error())
		}
	}
}
```

## TLS

If the broker's certificate is issued by a public CA (e.g., Let's Encrypt) or a
CA already in the operating system's trust store, enable TLS with no arguments:

```go
client, err := fila.Dial("broker.example.com:5555",
    fila.WithTLS(),
)
```

For self-signed certificates or private CAs not in the system trust store,
provide the CA certificate explicitly:

```go
caCert, err := os.ReadFile("ca.crt")
if err != nil {
    log.Fatal(err)
}
client, err := fila.Dial("localhost:5555",
    fila.WithTLSCACert(caCert),
)
```

For mutual TLS (mTLS), also provide the client certificate and key:

```go
caCert, err := os.ReadFile("ca.crt")
clientCert, err := os.ReadFile("client.crt")
clientKey, err := os.ReadFile("client.key")

client, err := fila.Dial("localhost:5555",
    fila.WithTLSCACert(caCert),
    fila.WithTLSClientCert(clientCert, clientKey),
)
```

## API Key Authentication

Connect to an auth-enabled broker by providing an API key:

```go
client, err := fila.Dial("localhost:5555",
    fila.WithAPIKey("your-api-key"),
)
```

TLS and API key auth can be combined:

```go
client, err := fila.Dial("localhost:5555",
    fila.WithTLSCACert(caCert),
    fila.WithTLSClientCert(clientCert, clientKey),
    fila.WithAPIKey("your-api-key"),
)
```

## API

### `fila.Dial(addr string, opts ...DialOption) (*Client, error)`

Connect to a Fila broker over the binary protocol. The connection is established immediately, including the protocol handshake.

### Options

- `fila.WithTLS()` — Enable TLS using the system's default root CA pool
- `fila.WithTLSCACert(caCertPEM []byte)` — Enable TLS with a custom CA certificate
- `fila.WithTLSClientCert(certPEM, keyPEM []byte)` — Client certificate for mTLS (requires WithTLS or WithTLSCACert)
- `fila.WithAPIKey(key string)` — API key sent in the FIBP handshake
- `fila.WithAccumulatorMode(mode)` — Set accumulation strategy for Enqueue

### Hot-Path Operations

- `client.Enqueue(ctx, queue, headers, payload) (string, error)` — Enqueue a message
- `client.EnqueueMany(ctx, messages) ([]EnqueueManyResult, error)` — Batch enqueue
- `client.Consume(ctx, queue) (<-chan *ConsumeMessage, error)` — Streaming consumer
- `client.Ack(ctx, queue, msgID) error` — Acknowledge a message
- `client.AckMany(ctx, items) ([]AckManyResult, error)` — Batch acknowledge
- `client.Nack(ctx, queue, msgID, errMsg) error` — Negative acknowledge
- `client.NackMany(ctx, items) ([]NackManyResult, error)` — Batch negative acknowledge

### Admin Operations

- `client.CreateQueue(ctx, name, opts) (string, error)` — Create a queue
- `client.DeleteQueue(ctx, name) error` — Delete a queue
- `client.GetStats(ctx, queue) (*QueueStats, error)` — Get queue statistics
- `client.ListQueues(ctx) (*ListQueuesResult, error)` — List all queues
- `client.SetConfig(ctx, key, value) error` — Set runtime config
- `client.GetConfig(ctx, key) (string, error)` — Get runtime config
- `client.ListConfig(ctx, prefix) ([]ConfigEntry, error)` — List config entries
- `client.Redrive(ctx, dlqQueue, count) (uint64, error)` — Redrive DLQ messages

### Auth Management

- `client.CreateApiKey(ctx, name, expiresAt, isSuperadmin) (*ApiKeyInfo, error)` — Create API key
- `client.RevokeApiKey(ctx, keyID) error` — Revoke API key
- `client.ListApiKeys(ctx) ([]ApiKeyListEntry, error)` — List API keys
- `client.SetAcl(ctx, keyID, permissions) error` — Set ACL permissions
- `client.GetAcl(ctx, keyID) (*AclInfo, error)` — Get ACL permissions

## Error Handling

Per-operation sentinel errors are checkable via `errors.Is`:

```go
_, err := client.Enqueue(ctx, "missing-queue", nil, []byte("test"))
if errors.Is(err, fila.ErrQueueNotFound) {
    // handle queue not found
}

err = client.Ack(ctx, "my-queue", "missing-id")
if errors.Is(err, fila.ErrMessageNotFound) {
    // handle message not found
}
```

All 18 FIBP error codes have corresponding sentinel errors:
`ErrQueueNotFound`, `ErrMessageNotFound`, `ErrQueueAlreadyExists`,
`ErrUnauthorized`, `ErrForbidden`, `ErrNotLeader`, `ErrChannelFull`,
`ErrStorageError`, `ErrInternal`, etc.

Server errors are returned as `*fila.ProtocolError`, which provides the error code, message, and metadata map:

```go
var pe *fila.ProtocolError
if errors.As(err, &pe) {
    fmt.Println("Code:", pe.Code)
    fmt.Println("Leader:", pe.LeaderAddr()) // for NotLeader errors
}
```

## License

AGPLv3
