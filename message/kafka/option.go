package kafka

import (
	"context"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// TopicResolver returns the topic a message is published to. The default
// returns the Kind string. An error or "" fails Publish.
type TopicResolver func(message.Message) (string, error)

// KindResolver returns the message Kind of a consumed record. The default
// reads the jimu-message-kind header. An error or "" fails the record at
// StageDecode.
type KindResolver func(*kgo.Record) (message.Kind, error)

// PayloadResolver is the upstream transport-neutral message payload resolver.
// A *message.PayloadRegistry is the usual value.
type PayloadResolver = message.PayloadResolver

// Deprecated: use ErrorObserver. ErrorHandler return values no longer control
// message-level failure flow.
type ErrorHandler func(context.Context, Error) error

// ErrorObserver observes provider failures without controlling consumer flow,
// for logging, alerting, and metrics. It runs for every failure, including
// failures the policy then retries or drops; Error.Final tells a last failure
// from one that is retried. It runs concurrently for different partitions, so
// it must be safe for concurrent use; keep it fast. A message-level failure
// after Run's ctx ended is left for redelivery and not observed.
type ErrorObserver func(context.Context, Error)

// Recoverer takes a record the FailurePolicy dropped, before its offset is
// committed, for example to save it in a failed-messages table. The Error has
// Final set and carries the source Record. Return nil to commit and move on.
// An error leaves the record uncommitted: the consumer observes it at
// StageRecover, pauses the partition for the retry backoff, and calls the
// Recoverer again without running the handlers. It runs concurrently for
// different partitions, so it must be safe for concurrent use.
type Recoverer func(context.Context, Error) error

// RetryBackoff returns the delay before attempt+1, after attempt failed
// attempts at a record (attempt starts at 1).
type RetryBackoff func(attempt int) time.Duration

// PublishBackoff returns the delay before the next publish retry attempt.
type PublishBackoff func(attempt int) time.Duration

// Option configures NewPublisher and NewConsumer. Each option notes which of
// the two it affects; the other ignores it.
type Option func(*config)

// HeaderNames are the reserved record headers that carry message metadata.
// Publisher and consumer must use the same names.
type HeaderNames struct {
	MessageID   string
	MessageKind string
	OccurredAt  string
}

type config struct {
	codec               Codec
	topicResolver       TopicResolver
	kindResolver        KindResolver
	defaultKindResolver bool
	payloadResolver     PayloadResolver
	errorObserver       ErrorObserver
	recoverer           Recoverer
	retryBackoff        RetryBackoff
	handlerTimeout      time.Duration
	commitTimeout       time.Duration
	maxPollRecords      int
	concurrency         int
	afterFunc           func(time.Duration, func())
	publishRetry        publishRetryConfig
	closeClient         bool
	headerNames         HeaderNames
}

type publishRetryConfig struct {
	maxAttempts int
	backoff     PublishBackoff
	sleep       func(context.Context, time.Duration) error
}

func defaultConfig() config {
	return config{
		codec:               ProtoCodec{},
		topicResolver:       defaultTopicResolver,
		kindResolver:        defaultKindResolver(defaultHeaderNames()),
		defaultKindResolver: true,
		retryBackoff:        DefaultRetryBackoff,
		commitTimeout:       10 * time.Second,
		maxPollRecords:      DefaultMaxPollRecords,
		afterFunc:           func(d time.Duration, f func()) { time.AfterFunc(d, f) },
		publishRetry:        publishRetryConfig{maxAttempts: 1, sleep: sleepContext},
		headerNames:         defaultHeaderNames(),
	}
}

func defaultHeaderNames() HeaderNames {
	return HeaderNames{
		MessageID:   "jimu-message-id",
		MessageKind: "jimu-message-kind",
		OccurredAt:  "jimu-message-occurred-at",
	}
}

func defaultTopicResolver(msg message.Message) (string, error) {
	if msg.Kind() == "" {
		return "", ErrNoTopic
	}
	return string(msg.Kind()), nil
}

func defaultKindResolver(headers HeaderNames) KindResolver {
	return func(record *kgo.Record) (message.Kind, error) {
		if record == nil {
			return "", ErrNoKind
		}
		for _, header := range record.Headers {
			if header.Key == headers.MessageKind {
				if len(header.Value) == 0 {
					return "", ErrNoKind
				}
				return message.Kind(header.Value), nil
			}
		}
		return "", ErrNoKind
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// WithCodec replaces the protobuf codec for payloads, for the publisher and
// the consumer. A nil codec is ignored.
func WithCodec(codec Codec) Option {
	return func(cfg *config) {
		if codec != nil {
			cfg.codec = codec
		}
	}
}

// WithTopicResolver sets the publisher's message-to-topic mapping. A nil
// resolver is ignored.
func WithTopicResolver(resolver TopicResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.topicResolver = resolver
		}
	}
}

// WithKindResolver sets how the consumer derives a record's Kind, for records
// written by producers other than this package. A nil resolver is ignored.
func WithKindResolver(resolver KindResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.kindResolver = resolver
			cfg.defaultKindResolver = false
		}
	}
}

// WithPayloadResolver sets the consumer's payload resolver, usually a
// *message.PayloadRegistry. NewConsumer requires it and returns
// ErrNoPayloadResolver without it. A nil resolver is ignored.
func WithPayloadResolver(resolver PayloadResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.payloadResolver = resolver
		}
	}
}

// WithPayloadResolverFunc adapts a function into message.PayloadResolver.
func WithPayloadResolverFunc(resolver func(message.Kind) (proto.Message, error)) Option {
	if resolver == nil {
		return WithPayloadResolver(nil)
	}
	return WithPayloadResolver(message.PayloadResolverFunc(resolver))
}

// WithErrorObserver sets the consumer's ErrorObserver. A nil observer is
// ignored.
func WithErrorObserver(observer ErrorObserver) Option {
	return func(cfg *config) {
		if observer != nil {
			cfg.errorObserver = observer
		}
	}
}

// Deprecated: use WithErrorObserver. Returned errors are ignored because
// message-level failure flow is controlled by FailurePolicy.
func WithErrorHandler(handler ErrorHandler) Option {
	return func(cfg *config) {
		if handler != nil {
			cfg.errorObserver = func(ctx context.Context, failure Error) {
				_ = handler(ctx, failure)
			}
		}
	}
}

// WithPublishRetry makes Publish try a failed produce up to maxAttempts times
// in total, waiting backoff(attempt) between attempts or until ctx ends. A
// maxAttempts of 1 or less keeps one attempt; a nil backoff retries at once.
// It affects the publisher only.
func WithPublishRetry(maxAttempts int, backoff PublishBackoff) Option {
	return func(cfg *config) {
		if maxAttempts > 1 {
			cfg.publishRetry.maxAttempts = maxAttempts
		}
		cfg.publishRetry.backoff = backoff
	}
}

// WithCloseClient makes Consumer.Close close the franz-go client.
func WithCloseClient(closeClient bool) Option {
	return func(cfg *config) {
		cfg.closeClient = closeClient
	}
}

// WithHeaderNames replaces the reserved metadata header names for the
// publisher and the default kind resolver. A zero HeaderNames is ignored.
func WithHeaderNames(headers HeaderNames) Option {
	return func(cfg *config) {
		if headers == (HeaderNames{}) {
			return
		}
		cfg.headerNames = headers
		if cfg.defaultKindResolver {
			cfg.kindResolver = defaultKindResolver(headers)
		}
	}
}

// DefaultRetryBackoff waits 1s after the first failed attempt and doubles the
// delay for each further one, up to 1 minute.
func DefaultRetryBackoff(attempt int) time.Duration {
	const maxDelay = time.Minute
	if attempt <= 1 {
		return time.Second
	}
	if attempt > 7 {
		return maxDelay
	}
	return min(time.Second<<(attempt-1), maxDelay)
}

// WithRetryBackoff sets the consumer's delay before handling a record again
// after FailureActionRetry, and before calling a failed Recoverer again. The
// default is DefaultRetryBackoff. A nil backoff is ignored.
func WithRetryBackoff(backoff RetryBackoff) Option {
	return func(cfg *config) {
		if backoff != nil {
			cfg.retryBackoff = backoff
		}
	}
}

// WithHandlerTimeout bounds how long the handlers of one record may run. A
// handler still running at the timeout gets its ctx cancelled and the record
// fails at StageHandle with an error matching ErrHandlerTimeout. Go cannot
// stop a goroutine, so a handler that ignores ctx keeps running while the
// consumer moves on and may retry the record: make handlers honor ctx.
// 0 or less, the default, sets no timeout.
//
// The Consumer must handle a polled batch within the client's
// kgo.RebalanceTimeout, because it blocks rebalances while it holds the batch;
// set a timeout well below it.
func WithHandlerTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		cfg.handlerTimeout = timeout
	}
}

// WithRecoverer sets the consumer's Recoverer, which takes every record the
// FailurePolicy drops before its offset is committed. A nil recoverer is
// ignored.
func WithRecoverer(recoverer Recoverer) Option {
	return func(cfg *config) {
		if recoverer != nil {
			cfg.recoverer = recoverer
		}
	}
}

// DefaultMaxPollRecords is the default WithMaxPollRecords limit.
const DefaultMaxPollRecords = 500

// WithMaxPollRecords limits how many records the Consumer takes in one poll.
// The Consumer blocks rebalances until it has handled a whole poll, so the
// slowest partition of a batch must finish within the client's
// kgo.RebalanceTimeout; a smaller limit shortens that wait and lets a new
// member get partitions sooner. 0 or less means no limit. The default is
// DefaultMaxPollRecords.
func WithMaxPollRecords(n int) Option {
	return func(cfg *config) {
		cfg.maxPollRecords = max(n, 0)
	}
}

// WithConcurrency sets how many records the ShareConsumer handles at once,
// independent of the partition count. 0 or less, the default, means
// runtime.GOMAXPROCS(0). The Consumer ignores it: it handles each partition
// of a poll on its own goroutine, so its concurrency is the number of
// partitions assigned to it, and a group's is at most the partition count.
func WithConcurrency(n int) Option {
	return func(cfg *config) {
		cfg.concurrency = n
	}
}
