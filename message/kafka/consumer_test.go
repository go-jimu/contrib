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

func (f *fakeConsumerClient) Close() {
	f.closed = true
}

type testHandler struct {
	kinds []message.Kind
	err   error
	seen  []message.Message
}

func (h *testHandler) Listening() []message.Kind {
	return h.kinds
}

func (h *testHandler) Handle(_ context.Context, msg message.Message) error {
	h.seen = append(h.seen, msg)
	return h.err
}

func testConsumerConfig() config {
	cfg := defaultConfig()
	WithPayloadResolver(func(message.Kind) (proto.Message, error) {
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
	consumer := newConsumer(client, cfg)
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
	client := &fakeConsumerClient{}
	consumer := newConsumer(client, cfg)
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
	client := &fakeConsumerClient{}
	consumer := newConsumer(client, cfg)
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
	client := &fakeConsumerClient{}
	consumer := newConsumer(client, cfg)
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
	produceErr := errors.New("produce failed")
	client := &fakeConsumerClient{produceErr: produceErr}
	consumer := newConsumer(client, cfg)
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
	produceErr := errors.New("produce failed")
	client := &fakeConsumerClient{produceErr: produceErr}
	consumer := newConsumer(client, cfg)
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

// Intent: commit failures should be surfaced through the default error handler rather than hidden after processing succeeds.
func TestConsumerProcessRecordReturnsCommitError(t *testing.T) {
	cfg := testConsumerConfig()
	commitErr := errors.New("commit failed")
	client := &fakeConsumerClient{commitErr: commitErr}
	consumer := newConsumer(client, cfg)
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

// Intent: commit failures must remain visible even when a custom error handler only observes and returns nil.
func TestConsumerProcessRecordReturnsCommitErrorWhenCustomHandlerReturnsNil(t *testing.T) {
	cfg := testConsumerConfig()
	var handled []Error
	WithErrorHandler(func(_ context.Context, failure Error) error {
		handled = append(handled, failure)
		return nil
	})(&cfg)
	commitErr := errors.New("commit failed")
	client := &fakeConsumerClient{commitErr: commitErr}
	consumer := newConsumer(client, cfg)
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
	consumer := newConsumer(client, defaultConfig())

	consumer.Close()
	if client.closed {
		t.Fatal("client closed by default, want open")
	}

	cfg := defaultConfig()
	WithCloseClient(true)(&cfg)
	ownedClient := &fakeConsumerClient{}
	ownedConsumer := newConsumer(ownedClient, cfg)
	ownedConsumer.Close()
	if !ownedClient.closed {
		t.Fatal("owned client was not closed")
	}
}

// Intent: constructing a consumer with a nil franz-go client should fail record processing without a typed-nil panic.
func TestNewConsumerNilClientProcessRecordReturnsErrNilClient(t *testing.T) {
	consumer := NewConsumer(nil)

	err := consumer.processRecord(context.Background(), &kgo.Record{Topic: "orders"})

	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("processRecord error = %v, want %v", err, ErrNilClient)
	}
}

// Intent: when DLQ is disabled, only a custom nil-returning error handler may drop and commit a non-retryable failure.
func TestConsumerProcessRecordDLQDisabledUsesErrorHandlerBeforeDrop(t *testing.T) {
	cfg := testConsumerConfig()
	var handled []Error
	WithDLQDisabled()(&cfg)
	WithErrorHandler(func(_ context.Context, failure Error) error {
		handled = append(handled, failure)
		return nil
	})(&cfg)
	client := &fakeConsumerClient{}
	consumer := newConsumer(client, cfg)
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

	defaultCfg := testConsumerConfig()
	WithDLQDisabled()(&defaultCfg)
	defaultClient := &fakeConsumerClient{}
	defaultConsumer := newConsumer(defaultClient, defaultCfg)
	defaultRecord := testRecord(t, defaultCfg, "order.payment.v1.OrderPaid")
	defaultRecord.Value = []byte("not protobuf")

	err = defaultConsumer.processRecord(context.Background(), defaultRecord)

	if err == nil {
		t.Fatal("processRecord returned nil with default error handler, want original decode error")
	}
	if len(defaultClient.committed) != 0 {
		t.Fatalf("default handler committed records = %d, want 0", len(defaultClient.committed))
	}
}
