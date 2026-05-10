package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProducerClient struct {
	records []*kgo.Record
	err     error
}

func (f *fakeProducerClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.records = append(f.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: f.err}
	}
	return results
}

// Intent: publishing a message should hand one fully mapped Kafka record to the producer.
func TestPublisherPublishProducesRecord(t *testing.T) {
	client := &fakeProducerClient{}
	publisher := newPublisher(client, defaultConfig())
	msg := newTestMessage(t, "order.payment.v1.OrderPaid")

	err := publisher.Publish(context.Background(), msg)

	if err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if len(client.records) != 1 {
		t.Fatalf("produced records = %d, want 1", len(client.records))
	}
	record := client.records[0]
	if record.Topic != "order.payment.v1.OrderPaid" {
		t.Fatalf("topic = %q, want order.payment.v1.OrderPaid", record.Topic)
	}
	if string(record.Key) != "order-1" {
		t.Fatalf("key = %q, want order-1", record.Key)
	}
	if len(record.Value) == 0 {
		t.Fatal("value is empty, want protobuf bytes")
	}
	cfg := defaultConfig()
	if got := headerValue(record.Headers, cfg.headerNames.MessageID); got != "msg-1" {
		t.Fatalf("message id header = %q, want msg-1", got)
	}
	if got := headerValue(record.Headers, cfg.headerNames.MessageKind); got != "order.payment.v1.OrderPaid" {
		t.Fatalf("message kind header = %q, want order.payment.v1.OrderPaid", got)
	}
	if got := headerValue(record.Headers, "trace_id"); got != "trace-1" {
		t.Fatalf("trace header = %q, want trace-1", got)
	}
}

// Intent: producer failures must be returned unchanged so callers can apply their own retry policy.
func TestPublisherPublishReturnsProduceError(t *testing.T) {
	produceErr := errors.New("produce failed")
	client := &fakeProducerClient{err: produceErr}
	publisher := newPublisher(client, defaultConfig())

	err := publisher.Publish(context.Background(), newTestMessage(t, "order.payment.v1.OrderPaid"))

	if !errors.Is(err, produceErr) {
		t.Fatalf("Publish error = %v, want %v", err, produceErr)
	}
}

// Intent: constructing with a nil franz-go client should defer failure to Publish without panicking.
func TestNewPublisherRejectsNilClient(t *testing.T) {
	publisher := NewPublisher(nil)

	err := publisher.Publish(context.Background(), newTestMessage(t, "order.payment.v1.OrderPaid"))

	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("Publish error = %v, want %v", err, ErrNilClient)
	}
}

// Intent: exported construction must apply topic resolver options to the record mapping config.
func TestPublisherPublishAppliesTopicResolverOption(t *testing.T) {
	msgPublisher := NewPublisher(nil, WithTopicResolver(func(message.Message) (string, error) {
		return "payments", nil
	}))

	kafkaPublisher, ok := msgPublisher.(*publisher)
	if !ok {
		t.Fatalf("publisher type = %T, want *publisher", msgPublisher)
	}

	record, err := messageToRecord(newTestMessage(t, "order.payment.v1.OrderPaid"), kafkaPublisher.cfg)
	if err != nil {
		t.Fatalf("messageToRecord returned error: %v", err)
	}
	if record.Topic != "payments" {
		t.Fatalf("topic = %q, want payments", record.Topic)
	}
}
