package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/go-jimu/components/ddd/message"
	"github.com/go-jimu/contrib/message/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ExampleRetryThenDLQPolicy shows the outcome for each handler attempt of one
// record: the jimu-retry-attempt header counts the attempts already made.
func ExampleRetryThenDLQPolicy() {
	policy := kafka.RetryThenDLQPolicy(kafka.RetryThenDLQConfig{MaxAttempts: 3})
	names := map[kafka.FailureAction]string{
		kafka.FailureActionDrop:  "drop",
		kafka.FailureActionRetry: "retry",
		kafka.FailureActionDLQ:   "dlq",
	}

	for attempt := range 3 {
		record := &kgo.Record{Topic: "orders.paid"}
		if attempt > 0 {
			record.Topic = "orders.paid.retry"
			record.Headers = []kgo.RecordHeader{{Key: "jimu-retry-attempt", Value: []byte(strconv.Itoa(attempt))}}
		}
		action := policy.Decide(context.Background(), kafka.Error{
			Stage:  kafka.StageHandle,
			Record: record,
			Err:    errors.New("database unavailable"),
		})
		fmt.Println("handler attempt", attempt+1, "->", names[action])
	}

	decode := policy.Decide(context.Background(), kafka.Error{
		Stage:  kafka.StageDecode,
		Record: &kgo.Record{Topic: "orders.paid"},
		Err:    message.ErrUnknownKind,
	})
	fmt.Println("decode failure ->", names[decode])

	// Output:
	// handler attempt 1 -> retry
	// handler attempt 2 -> retry
	// handler attempt 3 -> dlq
	// decode failure -> dlq
}

// customerCreated is a message.Handler owned by a bounded context. It depends
// on ddd/message only.
type customerCreated struct{}

func (customerCreated) Listening() []message.Kind {
	return []message.Kind{message.KindOf(&wrapperspb.StringValue{})}
}

func (customerCreated) Handle(_ context.Context, msg message.Message) error {
	if _, ok := msg.Payload().(*wrapperspb.StringValue); !ok {
		return fmt.Errorf("unexpected payload %T", msg.Payload())
	}
	return nil
}

// ExampleNewConsumer composes the consumer in runtime code: client, payload
// registry, failure policy, handlers, then Run until shutdown. It needs a
// broker, so it is compiled but not run.
func ExampleNewConsumer() {
	kind := message.KindOf(&wrapperspb.StringValue{})

	client, err := kafka.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("customers-service"),
		// RetryThenDLQPolicy needs the retry topic consumed too.
		kgo.ConsumeTopics(string(kind), string(kind)+".retry"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close() // after Run has returned

	registry := message.NewPayloadRegistry()
	consumer, err := kafka.NewConsumer(
		client,
		kafka.RetryThenDLQPolicy(kafka.RetryThenDLQConfig{MaxAttempts: 3}),
		kafka.WithPayloadResolver(registry),
		kafka.WithDefaultFailureTopics(),
		kafka.WithErrorObserver(func(ctx context.Context, failure kafka.Error) {
			slog.ErrorContext(ctx, "kafka message failed",
				slog.String("stage", string(failure.Stage)), slog.Any("error", failure.Err))
		}),
	)
	if err != nil {
		panic(err)
	}

	// Each bounded context registers its payloads and subscribes its
	// handlers before Run starts.
	if err := registry.Register(kind, func() proto.Message { return &wrapperspb.StringValue{} }); err != nil {
		panic(err)
	}
	if err := consumer.Subscribe(customerCreated{}); err != nil {
		panic(err)
	}

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	// On shutdown: cancel, wait for Run, then close the client (deferred).
	stop()
	if err := <-done; err != nil {
		// Run failed while consuming: the consumer cannot continue.
		panic(err)
	}
}

// ExampleNewPublisher builds the publisher in runtime code. Application code
// receives it as a message.Publisher. It needs a broker, so it is compiled but
// not run.
func ExampleNewPublisher() {
	client, err := kafka.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	var publisher message.Publisher = kafka.NewPublisher(client)

	payload := wrapperspb.String("alice")
	msg, err := message.New(message.KindOf(payload), payload, message.WithKey("customer-1"))
	if err != nil {
		panic(err)
	}
	// Publishes to the topic "google.protobuf.StringValue", the Kind.
	if err := publisher.Publish(context.Background(), msg); err != nil {
		panic(err)
	}
}
