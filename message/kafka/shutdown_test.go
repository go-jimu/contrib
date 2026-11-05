package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type funcHandler struct {
	kinds  []message.Kind
	handle func(context.Context, message.Message) error
}

func (h funcHandler) Listening() []message.Kind { return h.kinds }

func (h funcHandler) Handle(ctx context.Context, msg message.Message) error {
	return h.handle(ctx, msg)
}

// Intent: once Run's ctx has ended, a failure is left undecided for every
// policy: nothing is committed, published, or observed, so Kafka redelivers
// the record instead of a drop policy committing it unhandled.
func TestConsumerProcessRecordSkipsPolicyAfterContextEnds(t *testing.T) {
	policies := map[string]FailurePolicy{
		"drop":           DropAndCommitPolicy(),
		"dlq":            DLQPolicy(),
		"retry then dlq": RetryThenDLQPolicy(RetryThenDLQConfig{}),
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			cfg := testConsumerConfig()
			WithDefaultFailureTopics()(&cfg)
			var observed []Error
			WithErrorObserver(func(_ context.Context, failure Error) {
				observed = append(observed, failure)
			})(&cfg)
			client := &fakeConsumerClient{}
			consumer := newConsumerForTest(t, client, policy, cfg)
			kind := message.Kind("order.payment.v1.OrderPaid")
			if err := consumer.Subscribe(funcHandler{
				kinds:  []message.Kind{kind},
				handle: func(ctx context.Context, _ message.Message) error { return ctx.Err() },
			}); err != nil {
				t.Fatalf("Subscribe returned error: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := consumer.processRecord(ctx, testRecord(t, cfg, kind))

			if !errors.Is(err, context.Canceled) {
				t.Fatalf("processRecord error = %v, want context.Canceled", err)
			}
			if len(client.committed) != 0 || len(client.records) != 0 {
				t.Fatalf("committed %d, produced %d, want neither", len(client.committed), len(client.records))
			}
			if len(observed) != 0 {
				t.Fatalf("observed failures = %#v, want none", observed)
			}
		})
	}
}

// Intent: a handler cut short by shutdown makes Run return nil, the normal
// shutdown result, with the record left uncommitted.
func TestConsumerRunLeavesRecordUncommittedWhenHandlerFailsAtShutdown(t *testing.T) {
	cfg := testConsumerConfig()
	kind := message.Kind("order.payment.v1.OrderPaid")
	record := testRecord(t, cfg, kind)
	client := &fakeConsumerClient{fetches: []kgo.Fetches{fetchRecords(record)}}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), cfg)
	ctx, cancel := context.WithCancel(context.Background())
	if err := consumer.Subscribe(funcHandler{
		kinds: []message.Kind{kind},
		handle: func(ctx context.Context, _ message.Message) error {
			cancel() // shutdown begins while the handler runs
			return ctx.Err()
		},
	}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}

	err := consumer.Run(ctx)

	if err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if len(client.committed) != 0 {
		t.Fatalf("committed records = %#v, want none", client.committed)
	}
}

// Intent: a decode failure after ctx ended is left undecided too, because the
// publish or commit would run on the ended ctx.
func TestConsumerProcessRecordSkipsPolicyForDecodeFailureAfterContextEnds(t *testing.T) {
	cfg := testConsumerConfig()
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := consumer.processRecord(ctx, &kgo.Record{Topic: "orders"}) // no kind header

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("processRecord error = %v, want context.Canceled", err)
	}
	if len(client.committed) != 0 {
		t.Fatalf("committed records = %#v, want none", client.committed)
	}
}
