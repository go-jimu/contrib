package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
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
	defaultHeaders := cfg.headerNames
	defaultCloseClient := cfg.closeClient

	for _, opt := range []Option{
		WithCodec(nil),
		WithTopicResolver(nil),
		WithKindResolver(nil),
		WithPayloadResolver(nil),
		WithErrorHandler(nil),
		WithRetryTopicResolver(nil),
		WithDLQTopicResolver(nil),
	} {
		opt(&cfg)
	}

	if cfg.codec != defaultCodec {
		t.Fatal("nil codec option overwrote default codec")
	}

	msg := newTestMessage(t, "order.payment.v1.OrderPaid")
	topic, err := cfg.topicResolver(msg)
	if err != nil {
		t.Fatalf("topic resolver returned error: %v", err)
	}
	if topic != "order.payment.v1.OrderPaid" {
		t.Fatalf("topic resolver topic = %q, want %q", topic, "order.payment.v1.OrderPaid")
	}

	kind, err := cfg.kindResolver(&kgo.Record{
		Headers: []kgo.RecordHeader{{
			Key:   defaultHeaders.MessageKind,
			Value: []byte("order.payment.v1.OrderPaid"),
		}},
	})
	if err != nil {
		t.Fatalf("kind resolver returned error: %v", err)
	}
	if kind != "order.payment.v1.OrderPaid" {
		t.Fatalf("kind resolver kind = %q, want %q", kind, "order.payment.v1.OrderPaid")
	}

	payload, err := cfg.payloadResolver("order.payment.v1.OrderPaid")
	if !errors.Is(err, ErrNoPayloadResolver) {
		t.Fatalf("payload resolver error = %v, want %v", err, ErrNoPayloadResolver)
	}
	if payload != nil {
		t.Fatalf("payload resolver payload = %#v, want nil", payload)
	}

	if err := cfg.errorHandler(context.Background(), Error{Err: ErrUnhandledMessage}); !errors.Is(err, ErrUnhandledMessage) {
		t.Fatalf("error handler error = %v, want %v", err, ErrUnhandledMessage)
	}

	if cfg.retryPolicy.MaxAttempts != 3 {
		t.Fatalf("retry max attempts = %d, want 3", cfg.retryPolicy.MaxAttempts)
	}
	if !cfg.retryPolicy.Retryable(Error{Stage: StageHandle, Err: ErrUnhandledMessage}) {
		t.Fatal("retry policy returned false for handle stage, want true")
	}

	if !cfg.dlqEnabled {
		t.Fatal("DLQ enabled = false, want true")
	}
	if cfg.headerNames != defaultHeaders {
		t.Fatalf("header names = %#v, want %#v", cfg.headerNames, defaultHeaders)
	}
	if cfg.closeClient != defaultCloseClient {
		t.Fatalf("closeClient = %v, want %v", cfg.closeClient, defaultCloseClient)
	}
}

// Intent: header names must be replaceable so callers can avoid collisions with existing Kafka header conventions.
func TestOptionsHeaderOverride(t *testing.T) {
	cfg := defaultConfig()
	defaultHeaders := cfg.headerNames

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

	kind, err := cfg.kindResolver(&kgo.Record{
		Headers: []kgo.RecordHeader{{
			Key:   "x-message-kind",
			Value: []byte("order.payment.v1.OrderPaid"),
		}},
	})
	if err != nil {
		t.Fatalf("kind resolver with override header returned error: %v", err)
	}
	if kind != "order.payment.v1.OrderPaid" {
		t.Fatalf("kind resolver kind = %q, want %q", kind, "order.payment.v1.OrderPaid")
	}

	kind, err = cfg.kindResolver(&kgo.Record{
		Headers: []kgo.RecordHeader{{
			Key:   defaultHeaders.MessageKind,
			Value: []byte("order.payment.v1.OrderPaid"),
		}},
	})
	if !errors.Is(err, ErrNoKind) {
		t.Fatalf("kind resolver with old header error = %v, want %v", err, ErrNoKind)
	}
	if kind != "" {
		t.Fatalf("kind resolver with old header kind = %q, want empty", kind)
	}
}

// Intent: changing header names must not replace a caller-provided kind resolver because custom routing may ignore headers.
func TestOptionsHeaderOverridePreservesCustomKindResolver(t *testing.T) {
	cfg := defaultConfig()
	WithKindResolver(func(*kgo.Record) (message.Kind, error) {
		return "custom.Kind", nil
	})(&cfg)

	WithHeaderNames(HeaderNames{
		MessageID:   "x-message-id",
		MessageKind: "x-message-kind",
		OccurredAt:  "x-occurred-at",
	})(&cfg)

	kind, err := cfg.kindResolver(&kgo.Record{})
	if err != nil {
		t.Fatalf("custom kind resolver returned error: %v", err)
	}
	if kind != "custom.Kind" {
		t.Fatalf("kind resolver kind = %q, want custom.Kind", kind)
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
