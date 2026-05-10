package kafka

import (
	"context"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type TopicResolver func(message.Message) (string, error)
type KindResolver func(*kgo.Record) (message.Kind, error)

// PayloadResolver is the upstream transport-neutral message payload resolver.
type PayloadResolver = message.PayloadResolver
type ErrorHandler func(context.Context, Error) error
type FailureTopicResolver func(Error) (string, error)

type RetryPolicy struct {
	MaxAttempts int
	Retryable   func(Error) bool
}

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
	errorHandler        ErrorHandler
	retryPolicy         RetryPolicy
	dlqEnabled          bool
	retryTopicResolver  FailureTopicResolver
	dlqTopicResolver    FailureTopicResolver
	closeClient         bool
	headerNames         HeaderNames
}

func defaultConfig() config {
	return config{
		codec:               ProtoCodec{},
		topicResolver:       defaultTopicResolver,
		kindResolver:        defaultKindResolver(defaultHeaderNames()),
		defaultKindResolver: true,
		payloadResolver:     defaultPayloadResolver,
		errorHandler:        defaultErrorHandler,
		retryPolicy:         defaultRetryPolicy(),
		dlqEnabled:          true,
		retryTopicResolver:  defaultRetryTopicResolver,
		dlqTopicResolver:    defaultDLQTopicResolver,
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

func defaultErrorHandler(_ context.Context, failure Error) error {
	return failure.Err
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		Retryable: func(failure Error) bool {
			return failure.Stage == StageHandle || failure.Stage == StageUnhandled
		},
	}
}

func defaultRetryTopicResolver(failure Error) (string, error) {
	if failure.Record == nil {
		return "", ErrNilRecord
	}
	if failure.Record.Topic == "" {
		return "", ErrNoTopic
	}
	return failure.Record.Topic + ".retry", nil
}

func defaultDLQTopicResolver(failure Error) (string, error) {
	if failure.Record == nil {
		return "", ErrNilRecord
	}
	if failure.Record.Topic == "" {
		return "", ErrNoTopic
	}
	return failure.Record.Topic + ".dlq", nil
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

func WithErrorHandler(handler ErrorHandler) Option {
	return func(cfg *config) {
		if handler != nil {
			cfg.errorHandler = handler
		}
	}
}

func WithRetryPolicy(policy RetryPolicy) Option {
	return func(cfg *config) {
		if policy.MaxAttempts > 0 {
			cfg.retryPolicy.MaxAttempts = policy.MaxAttempts
		}
		if policy.Retryable != nil {
			cfg.retryPolicy.Retryable = policy.Retryable
		}
	}
}

func WithDLQDisabled() Option {
	return func(cfg *config) {
		cfg.dlqEnabled = false
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
