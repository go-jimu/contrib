package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newConsumerForTest(t *testing.T, client pollClient, policy FailurePolicy, cfg config) *Consumer {
	t.Helper()

	consumer, err := newConsumer(client, policy, cfg)
	if err != nil {
		t.Fatalf("newConsumer returned error: %v", err)
	}
	return consumer
}

// Intent: constructing a consumer without a failure policy should fail fast so message-level failures never inherit an accidental runtime-stop default.
func TestNewConsumerRejectsNilFailurePolicy(t *testing.T) {
	consumer, err := NewConsumer(nil, nil)

	if !errors.Is(err, ErrNilFailurePolicy) {
		t.Fatalf("NewConsumer error = %v, want %v", err, ErrNilFailurePolicy)
	}
	if consumer != nil {
		t.Fatalf("consumer = %#v, want nil", consumer)
	}
}

// Intent: a drop policy must treat handler errors as message-level failures, commit the failed source, and keep the run loop alive for later records.
func TestConsumerRunContinuesAfterHandlerFailureWithDropPolicy(t *testing.T) {
	cfg := testConsumerConfig()
	var observed []Error
	WithErrorObserver(func(_ context.Context, failure Error) {
		if failure.Stage != StagePoll {
			observed = append(observed, failure)
		}
	})(&cfg)
	failedRecord := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	nextRecord := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	client := &fakeConsumerClient{fetches: []kgo.Fetches{
		fetchRecords(failedRecord),
		fetchRecords(nextRecord),
		fetchError(context.Canceled),
	}}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), cfg)
	handlerErr := errors.New("handler failed")
	handler := &testHandler{
		kinds: []message.Kind{"order.payment.v1.OrderPaid"},
		errs:  []error{handlerErr, nil},
	}
	if err := consumer.Subscribe(handler); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	err := consumer.Run(context.Background())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want %v", err, context.Canceled)
	}
	if len(handler.seen) != 2 {
		t.Fatalf("handler saw %d messages, want 2", len(handler.seen))
	}
	if len(client.records) != 0 {
		t.Fatalf("produced failure records = %d, want 0", len(client.records))
	}
	if len(client.committed) != 2 || client.committed[0] != failedRecord || client.committed[1] != nextRecord {
		t.Fatalf("committed records = %#v, want failed then next source records", client.committed)
	}
	if len(observed) != 1 {
		t.Fatalf("observed failures = %d, want 1", len(observed))
	}
	if observed[0].Stage != StageHandle || !errors.Is(observed[0].Err, handlerErr) {
		t.Fatalf("observed failure = %#v, want handle stage with handler error", observed[0])
	}
}

// Intent: retry/DLQ policy should make failure handling explicit and continue the run loop after a retry handoff succeeds.
func TestConsumerRunContinuesAfterRetryHandoff(t *testing.T) {
	cfg := testConsumerConfig()
	WithDefaultFailureTopics()(&cfg)
	failedRecord := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	nextRecord := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	client := &fakeConsumerClient{fetches: []kgo.Fetches{
		fetchRecords(failedRecord),
		fetchRecords(nextRecord),
		fetchError(context.Canceled),
	}}
	consumer := newConsumerForTest(t, client, RetryThenDLQPolicy(RetryThenDLQConfig{MaxAttempts: 3}), cfg)
	if err := consumer.Subscribe(&testHandler{
		kinds: []message.Kind{"order.payment.v1.OrderPaid"},
		errs:  []error{errors.New("temporary handler failure"), nil},
	}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	err := consumer.Run(context.Background())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want %v", err, context.Canceled)
	}
	if len(client.records) != 1 {
		t.Fatalf("produced failure records = %d, want 1", len(client.records))
	}
	if client.records[0].Topic != "orders.retry" {
		t.Fatalf("retry topic = %q, want orders.retry", client.records[0].Topic)
	}
	if len(client.committed) != 2 || client.committed[0] != failedRecord || client.committed[1] != nextRecord {
		t.Fatalf("committed records = %#v, want failed then next source records", client.committed)
	}
}

// Intent: a record consumed from a retry topic should route further retry/DLQ handoff by original topic metadata, avoiding chained topics such as orders.retry.retry.
func TestFailureTopicResolversUseOriginalTopicHeader(t *testing.T) {
	cfg := defaultConfig()
	WithDefaultFailureTopics()(&cfg)
	record := &kgo.Record{
		Topic: "orders.retry",
		Headers: []kgo.RecordHeader{
			{Key: headerOriginalTopic, Value: []byte("orders")},
			{Key: headerRetryAttempt, Value: []byte("1")},
		},
	}
	failure := Error{Stage: StageHandle, Record: record, Err: errors.New("handler failed")}

	retryTopic, retryErr := cfg.retryTopicResolver(failure)
	dlqTopic, dlqErr := cfg.dlqTopicResolver(failure)

	if retryErr != nil {
		t.Fatalf("retry topic resolver returned error: %v", retryErr)
	}
	if retryTopic != "orders.retry" {
		t.Fatalf("retry topic = %q, want orders.retry", retryTopic)
	}
	if dlqErr != nil {
		t.Fatalf("dlq topic resolver returned error: %v", dlqErr)
	}
	if dlqTopic != "orders.dlq" {
		t.Fatalf("dlq topic = %q, want orders.dlq", dlqTopic)
	}
}
