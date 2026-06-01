package kafka

import (
	"context"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type TopicResolver func(message.Message) (string, error)
type KindResolver func(*kgo.Record) (message.Kind, error)

// PayloadResolver is the upstream transport-neutral message payload resolver.
type PayloadResolver = message.PayloadResolver

// Deprecated: use ErrorObserver. ErrorHandler return values no longer control
// message-level failure flow.
type ErrorHandler func(context.Context, Error) error

// ErrorObserver observes provider failures without controlling consumer flow.
type ErrorObserver func(context.Context, Error)

// FailureTopicResolver resolves the retry or DLQ topic for a failed source record.
type FailureTopicResolver func(Error) (string, error)

// Deprecated: use RetryThenDLQPolicy with NewConsumer.
type RetryPolicy struct {
	MaxAttempts int
	Retryable   func(Error) bool
}

type FailurePublishFallback int

const (
	// FailurePublishFallbackStop returns the publish failure from Run and leaves the source offset uncommitted.
	FailurePublishFallbackStop FailurePublishFallback = iota
	// FailurePublishFallbackDrop commits the source offset when retry or DLQ publish fails.
	FailurePublishFallbackDrop
)

// PublishBackoff returns the delay before the next publish retry attempt.
type PublishBackoff func(attempt int) time.Duration

type Option func(*config)

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
		payloadResolver:     defaultPayloadResolver,
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

var defaultPayloadResolver PayloadResolver = message.PayloadResolverFunc(func(message.Kind) (proto.Message, error) {
	return nil, ErrNoPayloadResolver
})

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

func WithCodec(codec Codec) Option {
	return func(cfg *config) {
		if codec != nil {
			cfg.codec = codec
		}
	}
}

func WithTopicResolver(resolver TopicResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.topicResolver = resolver
		}
	}
}

func WithKindResolver(resolver KindResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.kindResolver = resolver
			cfg.defaultKindResolver = false
		}
	}
}

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

func WithDefaultFailureTopics() Option {
	return func(cfg *config) {
		cfg.retryTopicResolver = defaultRetryTopicResolver
		cfg.dlqTopicResolver = defaultDLQTopicResolver
	}
}

func WithRetryTopicResolver(resolver FailureTopicResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.retryTopicResolver = resolver
		}
	}
}

func WithDLQTopicResolver(resolver FailureTopicResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.dlqTopicResolver = resolver
		}
	}
}

func WithFailurePublishFallback(fallback FailurePublishFallback) Option {
	return func(cfg *config) {
		cfg.failureFallback = fallback
	}
}

func WithPublishRetry(maxAttempts int, backoff PublishBackoff) Option {
	return func(cfg *config) {
		if maxAttempts > 1 {
			cfg.publishRetry.maxAttempts = maxAttempts
		}
		cfg.publishRetry.backoff = backoff
	}
}

func WithCloseClient(closeClient bool) Option {
	return func(cfg *config) {
		cfg.closeClient = closeClient
	}
}

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
