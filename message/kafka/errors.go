package kafka

import (
	"errors"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Stage string

const (
	StagePoll         Stage = "poll"
	StageDecode       Stage = "decode"
	StageUnhandled    Stage = "unhandled"
	StageHandle       Stage = "handle"
	StageRetryPublish Stage = "retry_publish"
	StageDLQPublish   Stage = "dlq_publish"
	StageCommit       Stage = "commit"
)

type Error struct {
	Stage   Stage
	Record  *kgo.Record
	Message message.Message
	Err     error
}

var (
	ErrNilClient          = errors.New("kafka client is nil")
	ErrNilRecord          = errors.New("kafka record is nil")
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
