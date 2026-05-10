package kafka

import (
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
)

// Intent: by default a message kind should be usable as the Kafka topic so a minimal publisher works without topic configuration.
func TestDefaultTopicResolverUsesKind(t *testing.T) {
	msg := newTestMessage(t, "order.payment.v1.OrderPaid")

	topic, err := defaultTopicResolver(msg)

	if err != nil {
		t.Fatalf("defaultTopicResolver returned error: %v", err)
	}
	if topic != "order.payment.v1.OrderPaid" {
		t.Fatalf("defaultTopicResolver topic = %q, want %q", topic, "order.payment.v1.OrderPaid")
	}
}

// Intent: an empty resolved Kafka topic must fail before producing because Kafka records require a topic.
func TestDefaultTopicResolverRejectsEmptyKind(t *testing.T) {
	msg := message.Message{}

	topic, err := defaultTopicResolver(msg)

	if !errors.Is(err, ErrNoTopic) {
		t.Fatalf("defaultTopicResolver error = %v, want %v", err, ErrNoTopic)
	}
	if topic != "" {
		t.Fatalf("defaultTopicResolver topic = %q, want empty", topic)
	}
}

// Intent: nil option values should leave defaults intact so optional configuration hooks cannot disable the provider accidentally.
func TestOptionsNilValuesPreserveDefaults(t *testing.T) {
	cfg := defaultConfig()
	defaultCodec := cfg.codec
	defaultTopicResolver := cfg.topicResolver
	defaultKindResolver := cfg.kindResolver
	defaultPayloadResolver := cfg.payloadResolver
	defaultErrorHandler := cfg.errorHandler
	defaultRetryPolicy := cfg.retryPolicy
	defaultDLQPolicy := cfg.dlqPolicy
	defaultHeaders := cfg.headerNames

	for _, opt := range []Option{
		WithCodec(nil),
		WithTopicResolver(nil),
		WithKindResolver(nil),
		WithPayloadResolver(nil),
		WithErrorHandler(nil),
		WithRetryPolicy(nil),
		WithDLQPolicy(nil),
	} {
		opt(&cfg)
	}

	if cfg.codec != defaultCodec {
		t.Fatal("nil codec option overwrote default codec")
	}
	if cfg.topicResolver == nil || defaultTopicResolver == nil {
		t.Fatal("topic resolver should remain configured")
	}
	if cfg.kindResolver == nil || defaultKindResolver == nil {
		t.Fatal("kind resolver should remain configured")
	}
	if cfg.payloadResolver == nil || defaultPayloadResolver == nil {
		t.Fatal("payload resolver should remain configured")
	}
	if cfg.errorHandler == nil || defaultErrorHandler == nil {
		t.Fatal("error handler should remain configured")
	}
	if cfg.retryPolicy == nil || defaultRetryPolicy == nil {
		t.Fatal("retry policy should remain configured")
	}
	if cfg.dlqPolicy == nil || defaultDLQPolicy == nil {
		t.Fatal("DLQ policy should remain configured")
	}
	if cfg.headerNames != defaultHeaders {
		t.Fatalf("header names = %#v, want %#v", cfg.headerNames, defaultHeaders)
	}
}

// Intent: header names must be replaceable so callers can avoid collisions with existing Kafka header conventions.
func TestOptionsHeaderOverride(t *testing.T) {
	cfg := defaultConfig()

	WithHeaderNames(HeaderNames{
		MessageID:   "x-message-id",
		MessageKind: "x-message-kind",
		OccurredAt:  "x-occurred-at",
	})(&cfg)

	if cfg.headerNames.MessageID != "x-message-id" {
		t.Fatalf("message id header = %q", cfg.headerNames.MessageID)
	}
	if cfg.headerNames.MessageKind != "x-message-kind" {
		t.Fatalf("message kind header = %q", cfg.headerNames.MessageKind)
	}
	if cfg.headerNames.OccurredAt != "x-occurred-at" {
		t.Fatalf("occurred at header = %q", cfg.headerNames.OccurredAt)
	}
}

// Intent: close-client ownership must be configurable because callers may share or transfer the franz-go client.
func TestOptionsCloseClient(t *testing.T) {
	cfg := defaultConfig()
	if cfg.closeClient {
		t.Fatal("closeClient default = true, want false")
	}

	WithCloseClient(true)(&cfg)

	if !cfg.closeClient {
		t.Fatal("closeClient = false, want true")
	}
}
