package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fetchRecords(records ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "orders",
			Partitions: []kgo.FetchPartition{{
				Partition: 0,
				Records:   records,
			}},
		}},
	}}
}

func fetchError(err error) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "orders",
			Partitions: []kgo.FetchPartition{{
				Partition: 0,
				Err:       err,
			}},
		}},
	}}
}

func fetchErrorAndRecords(err error, records ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "orders",
			Partitions: []kgo.FetchPartition{
				{
					Partition: 0,
					Err:       err,
				},
				{
					Partition: 1,
					Records:   records,
				},
			},
		}},
	}}
}

// Intent: fetched records should flow through the normal handler and commit path before a later poll stop.
func TestConsumerRunProcessesFetchedRecords(t *testing.T) {
	cfg := testConsumerConfig()
	var handled []Error
	WithErrorHandler(func(ctx context.Context, failure Error) error {
		handled = append(handled, failure)
		return ctx.Err()
	})(&cfg)
	ctx, cancel := context.WithCancel(context.Background())
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	client := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchRecords(record),
			fetchError(context.Canceled),
		},
		pollHooks: []func(){
			nil,
			cancel,
		},
	}
	consumer := newConsumer(client, cfg)
	handler := &testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}
	if err := consumer.Subscribe(handler); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	err := consumer.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want %v", err, context.Canceled)
	}
	if len(handler.seen) != 1 {
		t.Fatalf("handler saw %d messages, want 1", len(handler.seen))
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
	if len(handled) != 1 {
		t.Fatalf("handled errors = %d, want 1", len(handled))
	}
	if handled[0].Stage != StagePoll {
		t.Fatalf("handled stage = %q, want %q", handled[0].Stage, StagePoll)
	}
}

// Intent: records in a partial fetch should still be handled when poll errors are observed and ignored.
func TestConsumerRunProcessesRecordsFromMixedFetchWhenPollHandlerReturnsNil(t *testing.T) {
	cfg := testConsumerConfig()
	var handled []Error
	WithErrorHandler(func(_ context.Context, failure Error) error {
		handled = append(handled, failure)
		if errors.Is(failure.Err, context.Canceled) {
			return failure.Err
		}
		return nil
	})(&cfg)
	transientErr := errors.New("partition fetch failed")
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	client := &fakeConsumerClient{fetches: []kgo.Fetches{
		fetchErrorAndRecords(transientErr, record),
		fetchError(context.Canceled),
	}}
	consumer := newConsumer(client, cfg)
	handler := &testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}
	if err := consumer.Subscribe(handler); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	err := consumer.Run(context.Background())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want %v", err, context.Canceled)
	}
	if len(handler.seen) != 1 {
		t.Fatalf("handler saw %d messages, want 1", len(handler.seen))
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
	if len(handled) != 2 {
		t.Fatalf("handled errors = %d, want 2", len(handled))
	}
	if !errors.Is(handled[0].Err, transientErr) {
		t.Fatalf("first handled error = %v, want %v", handled[0].Err, transientErr)
	}
}

// Intent: poll failures should be reported as StagePoll and stop when the error handler returns an error.
func TestConsumerRunSurfacesPollError(t *testing.T) {
	cfg := testConsumerConfig()
	pollErr := errors.New("poll failed")
	var handled []Error
	WithErrorHandler(func(_ context.Context, failure Error) error {
		handled = append(handled, failure)
		return failure.Err
	})(&cfg)
	client := &fakeConsumerClient{fetches: []kgo.Fetches{fetchError(pollErr)}}
	consumer := newConsumer(client, cfg)

	err := consumer.Run(context.Background())

	if !errors.Is(err, pollErr) {
		t.Fatalf("Run error = %v, want %v", err, pollErr)
	}
	if len(handled) != 1 {
		t.Fatalf("handled errors = %d, want 1", len(handled))
	}
	if handled[0].Stage != StagePoll {
		t.Fatalf("handled stage = %q, want %q", handled[0].Stage, StagePoll)
	}
}

// Intent: context cancellation from polling should stop Run even when the error handler only observes it.
func TestConsumerRunStopsOnCanceledPollWhenErrorHandlerReturnsNil(t *testing.T) {
	cfg := testConsumerConfig()
	var handled []Error
	WithErrorHandler(func(_ context.Context, failure Error) error {
		handled = append(handled, failure)
		return nil
	})(&cfg)
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	client := &fakeConsumerClient{fetches: []kgo.Fetches{
		fetchError(context.Canceled),
		fetchRecords(record),
	}}
	consumer := newConsumer(client, cfg)

	err := consumer.Run(context.Background())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want %v", err, context.Canceled)
	}
	if client.pollCount != 1 {
		t.Fatalf("poll count = %d, want 1", client.pollCount)
	}
	if len(client.committed) != 0 {
		t.Fatalf("committed records = %d, want 0", len(client.committed))
	}
	if len(handled) != 1 {
		t.Fatalf("handled errors = %d, want 1", len(handled))
	}
	if handled[0].Stage != StagePoll {
		t.Fatalf("handled stage = %q, want %q", handled[0].Stage, StagePoll)
	}
}

// Intent: a nil-returning poll error handler should allow the consumer loop to continue to later records.
func TestConsumerRunContinuesWhenPollErrorHandlerReturnsNil(t *testing.T) {
	cfg := testConsumerConfig()
	var handled []Error
	WithErrorHandler(func(_ context.Context, failure Error) error {
		handled = append(handled, failure)
		if errors.Is(failure.Err, context.Canceled) {
			return failure.Err
		}
		return nil
	})(&cfg)
	transientErr := errors.New("temporary poll failed")
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	client := &fakeConsumerClient{fetches: []kgo.Fetches{
		fetchError(transientErr),
		fetchRecords(record),
		fetchError(context.Canceled),
	}}
	consumer := newConsumer(client, cfg)
	if err := consumer.Subscribe(&testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	err := consumer.Run(context.Background())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want %v", err, context.Canceled)
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
	if len(handled) != 2 {
		t.Fatalf("handled errors = %d, want 2", len(handled))
	}
	if handled[0].Stage != StagePoll || handled[1].Stage != StagePoll {
		t.Fatalf("handled stages = %#v, want poll stages", handled)
	}
}

// Intent: a consumer without a poll-capable client should fail fast instead of panicking or blocking.
func TestConsumerRunReturnsErrNilClient(t *testing.T) {
	consumer := NewConsumer(nil)

	err := consumer.Run(context.Background())

	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("Run error = %v, want %v", err, ErrNilClient)
	}
}

// Intent: record-processing failures reached from the poll loop should stop Run and leave the source uncommitted.
func TestConsumerRunReturnsProcessRecordError(t *testing.T) {
	cfg := testConsumerConfig()
	produceErr := errors.New("produce failed")
	record := testRecord(t, cfg, "order.payment.v1.OrderPaid")
	record.Value = []byte("not protobuf")
	client := &fakeConsumerClient{
		fetches:    []kgo.Fetches{fetchRecords(record)},
		produceErr: produceErr,
	}
	consumer := newConsumer(client, cfg)

	err := consumer.Run(context.Background())

	if !errors.Is(err, ErrDLQPublishFailed) {
		t.Fatalf("Run error = %v, want %v", err, ErrDLQPublishFailed)
	}
	if !errors.Is(err, produceErr) {
		t.Fatalf("Run error = %v, want underlying produce error", err)
	}
	if len(client.committed) != 0 {
		t.Fatalf("committed records = %d, want 0", len(client.committed))
	}
}
