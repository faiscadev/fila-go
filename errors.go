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
