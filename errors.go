// Package fila provides an idiomatic Go client SDK for the Fila message broker.
package fila

import (
	"errors"
	"fmt"
)

// Sentinel errors checkable via errors.Is.
var (
	// ErrQueueNotFound is returned when the specified queue does not exist.
	ErrQueueNotFound = errors.New("queue not found")

	// ErrMessageNotFound is returned when the specified message does not exist.
	ErrMessageNotFound = errors.New("message not found")

	// ErrConnectionClosed is returned when the underlying FIBP connection
	// has been closed while an operation was in flight.
	ErrConnectionClosed = errors.New("connection closed")
)

// ProtocolError represents an unexpected FIBP-level error with its error code
// and message preserved.
type ProtocolError struct {
	Code    uint16
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("fibp error (code=%d): %s", e.Code, e.Message)
}

// ItemError represents an error for an individual message within a
// multi-message operation. The server processed the request but this
// specific message failed.
//
// When the error code maps to a sentinel error (e.g., ErrQueueNotFound,
// ErrMessageNotFound), the ItemError wraps it so errors.Is works.
type ItemError struct {
	Code    string
	Message string
	cause   error
}

func (e *ItemError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("item error (%s): %s", e.Code, e.Message)
	}
	return fmt.Sprintf("item error: %s", e.Message)
}

func (e *ItemError) Unwrap() error {
	return e.cause
}
