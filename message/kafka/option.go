package kafka

import (
	"context"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type TopicResolver func(message.Message) (string, error)
type KindResolver func(*kgo.Record) (message.Kind, error)
type PayloadResolver func(message.Kind) (proto.Message, error)
type ErrorHandler func(context.Context, Error) error
type RetryPolicy func(Error) (bool, error)
type DLQPolicy func(Error) (bool, error)
type Option func(*config)

type Error struct {
	Err error
}

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
	dlqPolicy           DLQPolicy
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
		retryPolicy:         defaultRetryPolicy,
		dlqPolicy:           defaultDLQPolicy,
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

func defaultPayloadResolver(message.Kind) (proto.Message, error) {
	return nil, ErrNoPayloadResolver
}

func defaultErrorHandler(context.Context, Error) error {
	return nil
}

func defaultRetryPolicy(Error) (bool, error) {
	return true, nil
}

func defaultDLQPolicy(Error) (bool, error) {
	return true, nil
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

func WithErrorHandler(handler ErrorHandler) Option {
	return func(cfg *config) {
		if handler != nil {
			cfg.errorHandler = handler
		}
	}
}

func WithRetryPolicy(policy RetryPolicy) Option {
	return func(cfg *config) {
		if policy != nil {
			cfg.retryPolicy = policy
		}
	}
}

func WithDLQPolicy(policy DLQPolicy) Option {
	return func(cfg *config) {
		if policy != nil {
			cfg.dlqPolicy = policy
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
