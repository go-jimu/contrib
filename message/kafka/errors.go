package kafka

import (
	"errors"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Stage names the step of record processing where a failure happened.
type Stage string

const (
	// StagePoll is a fetch error from PollFetches. It is observed only; Run
	// returns it when the poll produced no records.
	StagePoll Stage = "poll"
	// StageDecode is a failure to turn a record into a message.Message:
	// missing kind, unresolvable payload, or a payload that does not
	// unmarshal. It is a message-level failure.
	StageDecode Stage = "decode"
	// StageUnhandled is a decoded message whose Kind has no subscribed
	// handler; Err matches message.ErrUnhandledKind. It is a message-level
	// failure.
	StageUnhandled Stage = "unhandled"
	// StageHandle is an error returned by a message.Handler. It is a
	// message-level failure.
	StageHandle Stage = "handle"
	// StageRetryPublish is a failure to resolve or publish the retry record.
	StageRetryPublish Stage = "retry_publish"
	// StageDLQPublish is a failure to resolve or publish the DLQ record.
	StageDLQPublish Stage = "dlq_publish"
	// StageCommit is a failure to commit the source offset.
	StageCommit Stage = "commit"
)

// Error describes one consumer failure for a FailurePolicy and an
// ErrorObserver. Record is the source record, nil for StagePoll; Message is
// the decoded message, zero before decoding succeeds; Err is the cause. Error
// is a value passed to callbacks and does not implement the error interface.
type Error struct {
	Stage   Stage
	Record  *kgo.Record
	Message message.Message
	Err     error
}

// Errors returned by this package. Compare with errors.Is: they are often
// wrapped with the underlying cause.
var (
	// ErrNilClient means NewPublisher or NewConsumer received a nil client;
	// Publish and Run return it.
	ErrNilClient = errors.New("kafka client is nil")
	// ErrNilRecord means a failure carried no source record.
	ErrNilRecord = errors.New("kafka record is nil")
	// ErrNilMessage is not returned by this version.
	ErrNilMessage = errors.New("message is nil")
	// ErrNilPayload means Publish received a message without a payload, or a
	// payload resolver returned nil.
	ErrNilPayload = errors.New("message payload is nil")
	// ErrNoTopic means a topic resolver returned "" or no retry or DLQ topic
	// resolver is configured for the action the policy chose.
	ErrNoTopic = errors.New("kafka topic is empty")
	// ErrNoKind means a consumed record has no message kind header, or the
	// KindResolver returned "". The record fails at StageDecode.
	ErrNoKind = errors.New("message kind is empty")
	// ErrNoPayloadResolver means NewConsumer was called without
	// WithPayloadResolver.
	ErrNoPayloadResolver = errors.New("payload resolver is not configured")
	// ErrUnhandledMessage is not returned by this version; an unhandled kind
	// fails at StageUnhandled with message.ErrUnhandledKind.
	ErrUnhandledMessage = errors.New("message is unhandled")
	// ErrNilFailurePolicy means NewConsumer received a nil FailurePolicy.
	ErrNilFailurePolicy = errors.New("kafka failure policy is nil")
	// ErrRetryPublishFailed wraps a failure to resolve or publish a retry
	// record. Run returns it under FailurePublishFallbackStop.
	ErrRetryPublishFailed = errors.New("retry publish failed")
	// ErrDLQPublishFailed wraps a failure to resolve or publish a DLQ record.
	// Run returns it under FailurePublishFallbackStop.
	ErrDLQPublishFailed = errors.New("dlq publish failed")
	// ErrCommitFailed wraps an offset commit failure. Run returns it.
	ErrCommitFailed = errors.New("commit failed")
)
