// Package fila provides an idiomatic Go client SDK for the Fila message broker.
package fila

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sentinel errors checkable via errors.Is.
var (
	// ErrQueueNotFound is returned when the specified queue does not exist.
	ErrQueueNotFound = errors.New("queue not found")

	// ErrMessageNotFound is returned when the specified message does not exist.
	ErrMessageNotFound = errors.New("message not found")
)

// RPCError represents an unexpected gRPC error with code and message preserved.
type RPCError struct {
	Code    codes.Code
	Message string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("rpc error (code = %s): %s", e.Code, e.Message)
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

// mapEnqueueError maps a gRPC status to an enqueue-specific error.
func mapEnqueueError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("enqueue: %w: %s", ErrQueueNotFound, st.Message())
	default:
		return &RPCError{Code: st.Code(), Message: st.Message()}
	}
}

// mapConsumeError maps a gRPC status to a consume-specific error.
func mapConsumeError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("consume: %w: %s", ErrQueueNotFound, st.Message())
	default:
		return &RPCError{Code: st.Code(), Message: st.Message()}
	}
}

// mapAckError maps a gRPC status to an ack-specific error.
func mapAckError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("ack: %w: %s", ErrMessageNotFound, st.Message())
	default:
		return &RPCError{Code: st.Code(), Message: st.Message()}
	}
}

// mapNackError maps a gRPC status to a nack-specific error.
func mapNackError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("nack: %w: %s", ErrMessageNotFound, st.Message())
	default:
		return &RPCError{Code: st.Code(), Message: st.Message()}
	}
}
