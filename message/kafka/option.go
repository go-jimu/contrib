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
// for logging and metrics. It runs on the Run goroutine for every failure,
// including failures the policy then drops, so keep it fast.
type ErrorObserver func(context.Context, Error)

// FailureTopicResolver resolves the retry or DLQ topic for a failed source record.
type FailureTopicResolver func(Error) (string, error)

// Deprecated: use RetryThenDLQPolicy with NewConsumer.
type RetryPolicy struct {
	MaxAttempts int
	Retryable   func(Error) bool
}

// FailurePublishFallback chooses what happens when a retry or DLQ record
// cannot be resolved or published.
type FailurePublishFallback int

const (
	// FailurePublishFallbackStop returns the publish failure from Run and leaves the source offset uncommitted.
	FailurePublishFallbackStop FailurePublishFallback = iota
	// FailurePublishFallbackDrop commits the source offset when retry or DLQ publish fails.
	FailurePublishFallbackDrop
)

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
	retryTopicResolver  FailureTopicResolver
	dlqTopicResolver    FailureTopicResolver
	failureFallback     FailurePublishFallback
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
		failureFallback:     FailurePublishFallbackStop,
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

// Deprecated: use RetryThenDLQPolicy with NewConsumer.
func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		Retryable: func(failure Error) bool {
			return failure.Stage == StageHandle
		},
	}
}

func defaultRetryTopicResolver(failure Error) (string, error) {
	topic, err := sourceTopic(failure)
	if err != nil {
		return "", err
	}
	return topic + ".retry", nil
}

func defaultDLQTopicResolver(failure Error) (string, error) {
	topic, err := sourceTopic(failure)
	if err != nil {
		return "", err
	}
	return topic + ".dlq", nil
}

func sourceTopic(failure Error) (string, error) {
	if failure.Record == nil {
		return "", ErrNilRecord
	}
	if topic := headerValue(failure.Record.Headers, headerOriginalTopic); topic != "" {
		return topic, nil
	}
	if failure.Record.Topic == "" {
		return "", ErrNoTopic
	}
	return failure.Record.Topic, nil
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

// Deprecated: pass RetryThenDLQPolicy to NewConsumer.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(*config) {}
}

// Deprecated: choose DropAndCommitPolicy or a custom FailurePolicy.
func WithDLQDisabled() Option {
	return func(*config) {}
}

// WithDefaultFailureTopics makes the consumer publish retry records to
// <source-topic>.retry and DLQ records to <source-topic>.dlq, where the source
// topic is the original topic of a record consumed from a retry topic.
func WithDefaultFailureTopics() Option {
	return func(cfg *config) {
		cfg.retryTopicResolver = defaultRetryTopicResolver
		cfg.dlqTopicResolver = defaultDLQTopicResolver
	}
}

// WithRetryTopicResolver sets the consumer's retry topic. A nil resolver is
// ignored.
func WithRetryTopicResolver(resolver FailureTopicResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.retryTopicResolver = resolver
		}
	}
}

// WithDLQTopicResolver sets the consumer's DLQ topic. A nil resolver is
// ignored.
func WithDLQTopicResolver(resolver FailureTopicResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.dlqTopicResolver = resolver
		}
	}
}

// WithFailurePublishFallback sets what the consumer does when a retry or DLQ
// record cannot be resolved or published. The default,
// FailurePublishFallbackStop, stops Run without committing.
func WithFailurePublishFallback(fallback FailurePublishFallback) Option {
	return func(cfg *config) {
		cfg.failureFallback = fallback
	}
}

// WithPublishRetry makes Publish try a failed produce up to maxAttempts times
// in total, waiting backoff(attempt) between attempts or until ctx ends. A
// maxAttempts of 1 or less keeps one attempt; a nil backoff retries at once.
// It affects the publisher only, not retry or DLQ records.
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
