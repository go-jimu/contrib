package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type fakeConsumerClient struct {
	records    []*kgo.Record
	committed  []*kgo.Record
	fetches    []kgo.Fetches
	pollHooks  []func()
	pollCount  int
	produceErr error
	commitErr  error
	closed     bool
}

func (f *fakeConsumerClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.records = append(f.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: f.produceErr}
	}
	return results
}

func (f *fakeConsumerClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.committed = append(f.committed, records...)
	return f.commitErr
}

func (f *fakeConsumerClient) PollFetches(_ context.Context) kgo.Fetches {
	f.pollCount++
	if len(f.pollHooks) > 0 {
		hook := f.pollHooks[0]
		f.pollHooks = f.pollHooks[1:]
		if hook != nil {
			hook()
		}
	}
	if len(f.fetches) == 0 {
		return fetchError(context.Canceled)
	}
	fetches := f.fetches[0]
	f.fetches = f.fetches[1:]
	return fetches
}

func (f *fakeConsumerClient) Close() {
	f.closed = true
}

type testHandler struct {
	kinds []message.Kind
	err   error
	errs  []error
	seen  []message.Message
}

func (h *testHandler) Listening() []message.Kind {
	return h.kinds
}

func (h *testHandler) Handle(_ context.Context, msg message.Message) error {
	h.seen = append(h.seen, msg)
	if len(h.errs) > 0 {
		err := h.errs[0]
		h.errs = h.errs[1:]
		return err
	}
	return h.err
}

func testConsumerConfig() config {
	cfg := defaultConfig()
	WithPayloadResolverFunc(func(message.Kind) (proto.Message, error) {
		return &testdata.TestModel{}, nil
	})(&cfg)
	return cfg
}

func testRecord(t *testing.T, cfg config, kind message.Kind) *kgo.Record {
	t.Helper()

	record, err := messageToRecord(newTestMessage(t, kind), cfg)
	if err != nil {
		t.Fatalf("messageToRecord returned error: %v", err)
	}
	record.Topic = "orders"
	return record
}

// Intent: a successfully handled source record should commit its offset without publishing retry or DLQ records.
func TestConsumerProcessRecordCommitsAfterSuccess(t *testing.T) {
	cfg := testConsumerConfig()
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), cfg)
	handler := &testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}
	if err := consumer.Subscribe(handler); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")

	err := consumer.processRecord(context.Background(), record)

	if err != nil {
		t.Fatalf("processRecord returned error: %v", err)
	}
	if len(handler.seen) != 1 {
		t.Fatalf("handler saw %d messages, want 1", len(handler.seen))
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
	if len(client.records) != 0 {
		t.Fatalf("produced records = %d, want 0", len(client.records))
	}
}

// Intent: decode failures should route the raw source record to DLQ and commit only after DLQ publish succeeds.
func TestConsumerProcessRecordDLQDecodeFailure(t *testing.T) {
	cfg := testConsumerConfig()
	WithDefaultFailureTopics()(&cfg)
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, DLQPolicy(), cfg)
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	record.Value = []byte("not protobuf")

	err := consumer.processRecord(context.Background(), record)

	if err != nil {
		t.Fatalf("processRecord returned error: %v", err)
	}
	if len(client.records) != 1 {
		t.Fatalf("produced records = %d, want 1", len(client.records))
	}
	if client.records[0].Topic != "orders.dlq" {
		t.Fatalf("produced topic = %q, want orders.dlq", client.records[0].Topic)
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
}

// Intent: retryable handler failures below the attempt limit should publish to retry and then commit the source.
func TestConsumerProcessRecordRetriesHandlerError(t *testing.T) {
	cfg := testConsumerConfig()
	WithDefaultFailureTopics()(&cfg)
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, RetryThenDLQPolicy(RetryThenDLQConfig{}), cfg)
	handlerErr := errors.New("handler failed")
	if err := consumer.Subscribe(&testHandler{
		kinds: []message.Kind{"order.payment.v1.OrderPaid"},
		err:   handlerErr,
	}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")

	err := consumer.processRecord(context.Background(), record)

	if err != nil {
		t.Fatalf("processRecord returned error: %v", err)
	}
	if len(client.records) != 1 {
		t.Fatalf("produced records = %d, want 1", len(client.records))
	}
	if client.records[0].Topic != "orders.retry" {
		t.Fatalf("produced topic = %q, want orders.retry", client.records[0].Topic)
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
}

// Intent: unhandled messages should be classified separately so retry/DLQ metadata shows routing gaps.
func TestConsumerProcessRecordDLQUnhandledMessage(t *testing.T) {
	cfg := testConsumerConfig()
	WithDefaultFailureTopics()(&cfg)
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, RetryThenDLQPolicy(RetryThenDLQConfig{}), cfg)
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")

	err := consumer.processRecord(context.Background(), record)

	if err != nil {
		t.Fatalf("processRecord returned error: %v", err)
	}
	if len(client.records) != 1 {
		t.Fatalf("produced records = %d, want 1", len(client.records))
	}
	if client.records[0].Topic != "orders.dlq" {
		t.Fatalf("produced topic = %q, want orders.dlq", client.records[0].Topic)
	}
	if got := headerValue(client.records[0].Headers, headerFailedStage); got != "unhandled" {
		t.Fatalf("failed stage header = %q, want unhandled", got)
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
}

// Intent: retry publish failures must leave the source uncommitted so Kafka can redeliver it.
func TestConsumerProcessRecordDoesNotCommitWhenRetryPublishFails(t *testing.T) {
	cfg := testConsumerConfig()
	WithDefaultFailureTopics()(&cfg)
	produceErr := errors.New("produce failed")
	client := &fakeConsumerClient{produceErr: produceErr}
	consumer := newConsumerForTest(t, client, RetryThenDLQPolicy(RetryThenDLQConfig{}), cfg)
	if err := consumer.Subscribe(&testHandler{
		kinds: []message.Kind{"order.payment.v1.OrderPaid"},
		err:   errors.New("handler failed"),
	}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")

	err := consumer.processRecord(context.Background(), record)

	if !errors.Is(err, ErrRetryPublishFailed) {
		t.Fatalf("processRecord error = %v, want %v", err, ErrRetryPublishFailed)
	}
	if !errors.Is(err, produceErr) {
		t.Fatalf("processRecord error = %v, want underlying produce error", err)
	}
	if len(client.committed) != 0 {
		t.Fatalf("committed records = %d, want 0", len(client.committed))
	}
}

// Intent: DLQ publish failures must leave the source uncommitted so a decode failure is not lost.
func TestConsumerProcessRecordDoesNotCommitWhenDLQPublishFails(t *testing.T) {
	cfg := testConsumerConfig()
	WithDefaultFailureTopics()(&cfg)
	produceErr := errors.New("produce failed")
	client := &fakeConsumerClient{produceErr: produceErr}
	consumer := newConsumerForTest(t, client, DLQPolicy(), cfg)
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	record.Value = []byte("not protobuf")

	err := consumer.processRecord(context.Background(), record)

	if !errors.Is(err, ErrDLQPublishFailed) {
		t.Fatalf("processRecord error = %v, want %v", err, ErrDLQPublishFailed)
	}
	if !errors.Is(err, produceErr) {
		t.Fatalf("processRecord error = %v, want underlying produce error", err)
	}
	if len(client.committed) != 0 {
		t.Fatalf("committed records = %d, want 0", len(client.committed))
	}
}

// Intent: commit failures should be surfaced as runtime-level failures rather than hidden after processing succeeds.
func TestConsumerProcessRecordReturnsCommitError(t *testing.T) {
	cfg := testConsumerConfig()
	commitErr := errors.New("commit failed")
	client := &fakeConsumerClient{commitErr: commitErr}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), cfg)
	if err := consumer.Subscribe(&testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")

	err := consumer.processRecord(context.Background(), record)

	if !errors.Is(err, ErrCommitFailed) {
		t.Fatalf("processRecord error = %v, want %v", err, ErrCommitFailed)
	}
	if !errors.Is(err, commitErr) {
		t.Fatalf("processRecord error = %v, want underlying commit error", err)
	}
}

// Intent: commit failures must remain visible even when a custom observer only records the failure.
func TestConsumerProcessRecordReturnsCommitErrorWhenObserverRecordsFailure(t *testing.T) {
	cfg := testConsumerConfig()
	var handled []Error
	WithErrorObserver(func(_ context.Context, failure Error) {
		handled = append(handled, failure)
	})(&cfg)
	commitErr := errors.New("commit failed")
	client := &fakeConsumerClient{commitErr: commitErr}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), cfg)
	if err := consumer.Subscribe(&testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")

	err := consumer.processRecord(context.Background(), record)

	if !errors.Is(err, ErrCommitFailed) {
		t.Fatalf("processRecord error = %v, want %v", err, ErrCommitFailed)
	}
	if !errors.Is(err, commitErr) {
		t.Fatalf("processRecord error = %v, want underlying commit error", err)
	}
	if len(handled) != 1 {
		t.Fatalf("handled errors = %d, want 1", len(handled))
	}
	if handled[0].Stage != StageCommit {
		t.Fatalf("handled stage = %q, want %q", handled[0].Stage, StageCommit)
	}
}

// Intent: Close should only close the franz-go client when ownership was explicitly transferred to the consumer.
func TestConsumerCloseHonorsClientOwnership(t *testing.T) {
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), defaultConfig())

	consumer.Close()
	if client.closed {
		t.Fatal("client closed by default, want open")
	}

	cfg := defaultConfig()
	WithCloseClient(true)(&cfg)
	ownedClient := &fakeConsumerClient{}
	ownedConsumer := newConsumerForTest(t, ownedClient, DropAndCommitPolicy(), cfg)
	ownedConsumer.Close()
	if !ownedClient.closed {
		t.Fatal("owned client was not closed")
	}
}

// Intent: constructing a consumer with a nil franz-go client should fail record processing without a typed-nil panic.
func TestNewConsumerNilClientProcessRecordReturnsErrNilClient(t *testing.T) {
	consumer, err := NewConsumer(nil, DropAndCommitPolicy())
	if err != nil {
		t.Fatalf("NewConsumer returned error: %v", err)
	}

	err = consumer.processRecord(context.Background(), &kgo.Record{Topic: "orders"})

	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("processRecord error = %v, want %v", err, ErrNilClient)
	}
}

// Intent: an explicit drop policy observes a message-level failure, commits the source, and keeps the consumer moving.
func TestConsumerProcessRecordDropPolicyObservesBeforeCommit(t *testing.T) {
	cfg := testConsumerConfig()
	var handled []Error
	WithErrorObserver(func(_ context.Context, failure Error) {
		handled = append(handled, failure)
	})(&cfg)
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), cfg)
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	record.Value = []byte("not protobuf")

	err := consumer.processRecord(context.Background(), record)

	if err != nil {
		t.Fatalf("processRecord returned error: %v", err)
	}
	if len(handled) != 1 {
		t.Fatalf("handled errors = %d, want 1", len(handled))
	}
	if handled[0].Stage != StageDecode {
		t.Fatalf("handled stage = %q, want %q", handled[0].Stage, StageDecode)
	}
	if len(client.records) != 0 {
		t.Fatalf("produced records = %d, want 0", len(client.records))
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
}
