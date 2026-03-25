package fila

import (
	"context"
	"fmt"

	filav1 "github.com/faisca/fila-go/filav1"
)

// Nack negatively acknowledges a message that failed processing.
//
// The message is requeued for retry or routed to the dead-letter queue
// based on the queue's on_failure Lua hook configuration.
func (c *Client) Nack(ctx context.Context, queue string, msgID string, errMsg string) error {
	resp, err := c.svc.Nack(ctx, &filav1.NackRequest{
		Messages: []*filav1.NackMessage{
			{
				Queue:     queue,
				MessageId: msgID,
				Error:     errMsg,
			},
		},
	})
	if err != nil {
		return mapNackError(err)
	}
	if len(resp.Results) == 0 {
		return fmt.Errorf("nack: server returned no results")
	}
	return parseNackResult(resp.Results[0])
}

// parseNackResult extracts success or error from a single NackResult.
func parseNackResult(r *filav1.NackResult) error {
	switch v := r.Result.(type) {
	case *filav1.NackResult_Success:
		return nil
	case *filav1.NackResult_Error:
		return nackErrorToItemError(v.Error)
	default:
		return fmt.Errorf("nack: unknown result type")
	}
}

func nackErrorToItemError(e *filav1.NackError) *ItemError {
	if e == nil {
		return &ItemError{Message: "unknown error"}
	}
	ie := &ItemError{
		Code:    nackErrorCodeString(e.Code),
		Message: e.Message,
	}
	switch e.Code {
	case filav1.NackErrorCode_NACK_ERROR_CODE_MESSAGE_NOT_FOUND:
		ie.cause = ErrMessageNotFound
	}
	return ie
}

func nackErrorCodeString(code filav1.NackErrorCode) string {
	switch code {
	case filav1.NackErrorCode_NACK_ERROR_CODE_MESSAGE_NOT_FOUND:
		return "message_not_found"
	case filav1.NackErrorCode_NACK_ERROR_CODE_STORAGE:
		return "storage"
	case filav1.NackErrorCode_NACK_ERROR_CODE_PERMISSION_DENIED:
		return "permission_denied"
	default:
		return "unspecified"
	}
}
