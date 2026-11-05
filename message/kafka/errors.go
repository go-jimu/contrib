package kafka

import (
	"errors"
	"fmt"

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
	// StageHandle is an error returned by a message.Handler, or a handler
	// panic recovered as a *PanicError. It is a message-level failure.
	StageHandle Stage = "handle"
	// StageRecover is an error returned by the Recoverer for a record the
	// FailurePolicy dropped; Err matches ErrRecoverFailed. The record stays
	// uncommitted and the Recoverer is called again after a backoff.
	StageRecover Stage = "recover"
	// StageCommit is a failure to commit the source offset.
	StageCommit Stage = "commit"
)

// Error describes one consumer failure for the policies, the Recoverer, and
// the ErrorObserver. Record is the source record, nil for StagePoll; Message is
// the decoded message, zero before decoding succeeds; Err is the cause.
//
// Attempt counts the failed attempts at this record, starting at 1: for the
// Consumer, attempts since this process started handling the record at the
// head of its partition (see Consumer failures in the package documentation);
// for the ShareConsumer, the broker's delivery count.
// Final reports that no further attempt follows: the policy chose to drop the
// record or stop. The FailurePolicy sees Final false, as it decides it.
//
// Error is a value passed to callbacks and does not implement the error
// interface.
type Error struct {
	Stage   Stage
	Record  *kgo.Record
	Message message.Message
	Err     error
	Attempt int
	Final   bool
}

// Errors returned by this package. Compare with errors.Is: they are often
// wrapped with the underlying cause.
var (
	// ErrNilClient means a constructor received a nil client; Publish and Run
	// return it.
	ErrNilClient = errors.New("kafka client is nil")
	// ErrNilRecord means a failure carried no source record.
	ErrNilRecord = errors.New("kafka record is nil")
	// ErrNilMessage is not returned by this version.
	ErrNilMessage = errors.New("message is nil")
	// ErrNilPayload means Publish received a message without a payload, or a
	// payload resolver returned nil.
	ErrNilPayload = errors.New("message payload is nil")
	// ErrNoTopic means a topic resolver returned "".
	ErrNoTopic = errors.New("kafka topic is empty")
	// ErrNoKind means a consumed record has no message kind header, or the
	// KindResolver returned "". The record fails at StageDecode.
	ErrNoKind = errors.New("message kind is empty")
	// ErrNoPayloadResolver means NewConsumer or NewShareConsumer was called
	// without WithPayloadResolver.
	ErrNoPayloadResolver = errors.New("payload resolver is not configured")
	// ErrUnhandledMessage is not returned by this version; an unhandled kind
	// fails at StageUnhandled with message.ErrUnhandledKind.
	ErrUnhandledMessage = errors.New("message is unhandled")
	// ErrNilFailurePolicy means NewConsumer or NewShareConsumer received a nil
	// policy.
	ErrNilFailurePolicy = errors.New("kafka failure policy is nil")
	// ErrInvalidClient means NewConsumer or NewShareConsumer received a
	// client without the options it requires; the error names them.
	ErrInvalidClient = errors.New("kafka client is not configured for this consumer")
	// ErrStopped wraps the failure for which the FailurePolicy chose
	// FailureActionStop. Run returns it.
	ErrStopped = errors.New("consumer stopped by failure policy")
	// ErrHandlerTimeout means a handler did not return within the
	// WithHandlerTimeout duration. It is a StageHandle failure.
	ErrHandlerTimeout = errors.New("message handler timed out")
	// ErrNonRetryable matches an error wrapped with NonRetryable: a failure a
	// retry cannot fix. The built-in policies do not retry it.
	ErrNonRetryable = errors.New("non-retryable failure")
	// ErrRecoverFailed wraps an error returned by the Recoverer.
	ErrRecoverFailed = errors.New("recover failed")
	// ErrCommitFailed wraps an offset commit failure. Run returns it.
	ErrCommitFailed = errors.New("commit failed")
	// ErrHandlerPanic matches a *PanicError: a handler panicked and the
	// consumer recovered it as a StageHandle failure.
	ErrHandlerPanic = errors.New("message handler panicked")
)

// PanicError is the Error.Err of a recovered handler panic.
// Value is what the handler passed to panic and Stack the goroutine stack at
// the panic; read them with errors.As, for example to log the stack in an
// ErrorObserver. It matches ErrHandlerPanic and, when Value is an error, that
// error too. Error omits the stack.
type PanicError struct {
	Value any
	Stack []byte
}

// Error returns "message handler panicked: " followed by Value.
func (e *PanicError) Error() string {
	return fmt.Sprintf("%v: %v", ErrHandlerPanic, e.Value)
}

// Unwrap returns ErrHandlerPanic and, when Value is an error, Value.
func (e *PanicError) Unwrap() []error {
	if err, ok := e.Value.(error); ok {
		return []error{ErrHandlerPanic, err}
	}
	return []error{ErrHandlerPanic}
}

// NonRetryable marks err as a failure a retry cannot fix, such as invalid
// input or a missing entity. The result matches ErrNonRetryable and err with
// errors.Is. A handler returns it so that RetryPolicy goes straight to its
// exhausted action and the ShareConsumer rejects the record. NonRetryable(nil)
// returns nil.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

type nonRetryableError struct {
	err error
}

func (e *nonRetryableError) Error() string {
	return e.err.Error()
}

func (e *nonRetryableError) Unwrap() []error {
	return []error{ErrNonRetryable, e.err}
}
