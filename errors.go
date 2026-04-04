// Package fila provides an idiomatic Go client SDK for the Fila message broker.
package fila

import (
	"errors"
	"fmt"

	"github.com/faisca/fila-go/fibp"
)

// Sentinel errors checkable via errors.Is.
var (
	ErrQueueNotFound       = errors.New("queue not found")
	ErrMessageNotFound     = errors.New("message not found")
	ErrQueueAlreadyExists  = errors.New("queue already exists")
	ErrLuaCompilation      = errors.New("lua compilation error")
	ErrStorageError        = errors.New("storage error")
	ErrNotADLQ             = errors.New("not a dead-letter queue")
	ErrParentQueueNotFound = errors.New("parent queue not found")
	ErrInvalidConfigValue  = errors.New("invalid config value")
	ErrChannelFull         = errors.New("channel full")
	ErrUnauthorized        = errors.New("unauthorized")
	ErrForbidden           = errors.New("forbidden")
	ErrNotLeader           = errors.New("not leader")
	ErrUnsupportedVersion  = errors.New("unsupported version")
	ErrInvalidFrame        = errors.New("invalid frame")
	ErrApiKeyNotFound      = errors.New("api key not found")
	ErrNodeNotReady        = errors.New("node not ready")
	ErrInternal            = errors.New("internal error")
)

// ProtocolError represents an error returned by the Fila server via the
// FIBP Error frame.
type ProtocolError struct {
	Code     fibp.ErrorCode
	Message  string
	Metadata map[string]string
	cause    error
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("fila error (%s): %s", e.Code, e.Message)
}

func (e *ProtocolError) Unwrap() error {
	return e.cause
}

// LeaderAddr returns the leader address from NotLeader errors, or empty string.
func (e *ProtocolError) LeaderAddr() string {
	if e.Metadata != nil {
		return e.Metadata["leader_addr"]
	}
	return ""
}

// ItemError represents an error for an individual message within a
// multi-message operation.
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

// errorCodeToErr creates a ProtocolError from an error code.
func errorCodeToErr(code fibp.ErrorCode, msg string, metadata map[string]string) *ProtocolError {
	pe := &ProtocolError{
		Code:     code,
		Message:  msg,
		Metadata: metadata,
	}
	switch code {
	case fibp.ErrorQueueNotFound:
		pe.cause = ErrQueueNotFound
	case fibp.ErrorMessageNotFound:
		pe.cause = ErrMessageNotFound
	case fibp.ErrorQueueAlreadyExists:
		pe.cause = ErrQueueAlreadyExists
	case fibp.ErrorLuaCompilation:
		pe.cause = ErrLuaCompilation
	case fibp.ErrorStorage:
		pe.cause = ErrStorageError
	case fibp.ErrorNotADLQ:
		pe.cause = ErrNotADLQ
	case fibp.ErrorParentQueueNotFound:
		pe.cause = ErrParentQueueNotFound
	case fibp.ErrorInvalidConfigValue:
		pe.cause = ErrInvalidConfigValue
	case fibp.ErrorChannelFull:
		pe.cause = ErrChannelFull
	case fibp.ErrorUnauthorized:
		pe.cause = ErrUnauthorized
	case fibp.ErrorForbidden:
		pe.cause = ErrForbidden
	case fibp.ErrorNotLeader:
		pe.cause = ErrNotLeader
	case fibp.ErrorUnsupportedVersion:
		pe.cause = ErrUnsupportedVersion
	case fibp.ErrorInvalidFrame:
		pe.cause = ErrInvalidFrame
	case fibp.ErrorApiKeyNotFound:
		pe.cause = ErrApiKeyNotFound
	case fibp.ErrorNodeNotReady:
		pe.cause = ErrNodeNotReady
	case fibp.ErrorInternal:
		pe.cause = ErrInternal
	}
	return pe
}

// errorCodeToItemError creates an ItemError from an error code.
func errorCodeToItemError(code fibp.ErrorCode) *ItemError {
	ie := &ItemError{
		Code: code.String(),
	}
	switch code {
	case fibp.ErrorQueueNotFound:
		ie.cause = ErrQueueNotFound
	case fibp.ErrorMessageNotFound:
		ie.cause = ErrMessageNotFound
	case fibp.ErrorStorage:
		ie.cause = ErrStorageError
	case fibp.ErrorLuaCompilation:
		ie.cause = ErrLuaCompilation
	case fibp.ErrorForbidden:
		ie.cause = ErrForbidden
	}
	return ie
}
