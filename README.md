# fila-go

Go client SDK for the [Fila](https://github.com/faisca/fila) message broker.

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

Connect to a TLS-enabled broker by providing the CA certificate:

```go
caCert, _ := os.ReadFile("ca.crt")
client, err := fila.Dial("localhost:5555",
    fila.WithTLSCACert(caCert),
)
```

For mutual TLS (mTLS), also provide the client certificate and key:

```go
caCert, _ := os.ReadFile("ca.crt")
clientCert, _ := os.ReadFile("client.crt")
clientKey, _ := os.ReadFile("client.key")

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

Connect to a Fila broker. Connection is established lazily on the first RPC call.

### Options

- `fila.WithTLSCACert(caCertPEM []byte)` — CA certificate for verifying the server (enables TLS)
- `fila.WithTLSClientCert(certPEM, keyPEM []byte)` — Client certificate and key for mTLS
- `fila.WithAPIKey(key string)` — API key sent as `Bearer` token on every RPC
- `fila.WithGRPCDialOption(opt grpc.DialOption)` — Raw gRPC dial option for advanced configuration

### `client.Enqueue(ctx, queue, headers, payload) (string, error)`

Enqueue a message. Returns the broker-assigned message ID.

### `client.Consume(ctx, queue) (<-chan *ConsumeMessage, error)`

Open a streaming consumer. Returns a channel that delivers messages as they become available. The channel is closed when the stream ends or the context is cancelled.

### `client.Ack(ctx, queue, msgID) error`

Acknowledge a successfully processed message. The message is permanently removed.

### `client.Nack(ctx, queue, msgID, errMsg) error`

Negatively acknowledge a failed message. The message is requeued or routed to the dead-letter queue based on the queue's configuration.

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
