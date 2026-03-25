package fila

import (
	"context"
	"fmt"

	filav1 "github.com/faisca/fila-go/filav1"
)

// Ack acknowledges a successfully processed message.
//
// The message is permanently removed from the queue.
func (c *Client) Ack(ctx context.Context, queue string, msgID string) error {
	resp, err := c.svc.Ack(ctx, &filav1.AckRequest{
		Messages: []*filav1.AckMessage{
			{
				Queue:     queue,
				MessageId: msgID,
			},
		},
	})
	if err != nil {
		return mapAckError(err)
	}
	if len(resp.Results) == 0 {
		return fmt.Errorf("ack: server returned no results")
	}
	return parseAckResult(resp.Results[0])
}

// parseAckResult extracts success or error from a single AckResult.
func parseAckResult(r *filav1.AckResult) error {
	switch v := r.Result.(type) {
	case *filav1.AckResult_Success:
		return nil
	case *filav1.AckResult_Error:
		return ackErrorToItemError(v.Error)
	default:
		return fmt.Errorf("ack: unknown result type")
	}
}

func ackErrorToItemError(e *filav1.AckError) *ItemError {
	if e == nil {
		return &ItemError{Message: "unknown error"}
	}
	ie := &ItemError{
		Code:    ackErrorCodeString(e.Code),
		Message: e.Message,
	}
	switch e.Code {
	case filav1.AckErrorCode_ACK_ERROR_CODE_MESSAGE_NOT_FOUND:
		ie.cause = ErrMessageNotFound
	}
	return ie
}

func ackErrorCodeString(code filav1.AckErrorCode) string {
	switch code {
	case filav1.AckErrorCode_ACK_ERROR_CODE_MESSAGE_NOT_FOUND:
		return "message_not_found"
	case filav1.AckErrorCode_ACK_ERROR_CODE_STORAGE:
		return "storage"
	case filav1.AckErrorCode_ACK_ERROR_CODE_PERMISSION_DENIED:
		return "permission_denied"
	default:
		return "unspecified"
	}
}
