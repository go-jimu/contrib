package kafka

import "errors"

var (
	ErrNilClient          = errors.New("kafka client is nil")
	ErrNilMessage         = errors.New("message is nil")
	ErrNilPayload         = errors.New("message payload is nil")
	ErrNoTopic            = errors.New("kafka topic is empty")
	ErrNoKind             = errors.New("message kind is empty")
	ErrNoPayloadResolver  = errors.New("payload resolver is not configured")
	ErrUnhandledMessage   = errors.New("message is unhandled")
	ErrRetryPublishFailed = errors.New("retry publish failed")
	ErrDLQPublishFailed   = errors.New("dlq publish failed")
	ErrCommitFailed       = errors.New("commit failed")
)
