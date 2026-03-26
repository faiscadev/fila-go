# fila-go

Go client SDK for the [Fila](https://github.com/faisca/fila) message broker.

Communicates over **FIBP** (Fila Binary Protocol) — a length-prefixed binary
framing protocol over raw TCP. No gRPC dependency.

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
if err != nil {
    log.Fatal(err)
}
clientCert, err := os.ReadFile("client.crt")
if err != nil {
    log.Fatal(err)
}
clientKey, err := os.ReadFile("client.key")
if err != nil {
    log.Fatal(err)
}

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

Connect to a Fila broker. The TCP connection and FIBP handshake are performed
eagerly during Dial.

### Options

- `fila.WithTLS()` — Enable TLS using the system's default root CA pool
- `fila.WithTLSCACert(caCertPEM []byte)` — Enable TLS with a custom CA certificate for verifying the server
- `fila.WithTLSClientCert(certPEM, keyPEM []byte)` — Client certificate and key for mTLS (requires `WithTLS` or `WithTLSCACert`)
- `fila.WithAPIKey(key string)` — API key sent as a FIBP AUTH frame immediately after the handshake

### `client.Enqueue(ctx, queue, headers, payload) (string, error)`

Enqueue a message. Returns the broker-assigned message ID.

### `client.Consume(ctx, queue) (<-chan *ConsumeMessage, error)`

Open a streaming consumer. Returns a channel that delivers messages as they become available. The channel is closed when the stream ends or the context is cancelled.

### `client.Ack(ctx, queue, msgID) error`

Acknowledge a successfully processed message. The message is permanently removed.

### `client.Nack(ctx, queue, msgID, errMsg) error`

Negatively acknowledge a failed message. The message is requeued or routed to the dead-letter queue based on the queue's configuration.

### `client.Admin() *AdminClient`

Returns an admin client for queue management operations (create, delete, list).

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

## License

AGPLv3
