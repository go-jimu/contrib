package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/go-jimu/contrib/message/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ExampleRetryPolicy shows the decision for each attempt at one record, and
// for failures a retry cannot fix.
func ExampleRetryPolicy() {
	policy := kafka.RetryPolicy(kafka.RetryConfig{MaxAttempts: 3})
	names := map[kafka.FailureAction]string{
		kafka.FailureActionDrop:  "drop",
		kafka.FailureActionRetry: "retry",
		kafka.FailureActionStop:  "stop",
	}

	for attempt := 1; attempt <= 3; attempt++ {
		action := policy.Decide(context.Background(), kafka.Error{
			Stage:   kafka.StageHandle,
			Err:     errors.New("database unavailable"),
			Attempt: attempt,
		})
		fmt.Println("attempt", attempt, "->", names[action])
	}

	permanent := policy.Decide(context.Background(), kafka.Error{
		Stage:   kafka.StageHandle,
		Err:     kafka.NonRetryable(errors.New("order does not exist")),
		Attempt: 1,
	})
	fmt.Println("non-retryable ->", names[permanent])

	decode := policy.Decide(context.Background(), kafka.Error{
		Stage:   kafka.StageDecode,
		Err:     message.ErrUnknownKind,
		Attempt: 1,
	})
	fmt.Println("decode failure ->", names[decode])

	// Output:
	// attempt 1 -> retry
	// attempt 2 -> retry
	// attempt 3 -> drop
	// non-retryable -> drop
	// decode failure -> drop
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
		kgo.ConsumeTopics(string(kind)),
		kgo.DisableAutoCommit(),    // the consumer commits
		kgo.BlockRebalanceOnPoll(), // no rebalance while a batch is handled
	)
	if err != nil {
		panic(err)
	}
	defer client.Close() // after Run has returned

	registry := message.NewPayloadRegistry()
	consumer, err := kafka.NewConsumer(
		client,
		kafka.RetryPolicy(kafka.RetryConfig{MaxAttempts: 5}),
		kafka.WithPayloadResolver(registry),
		kafka.WithHandlerTimeout(5*time.Second),
		kafka.WithMaxPollRecords(10), // 10 × 5s stays below kgo's 60s RebalanceTimeout
		kafka.WithErrorObserver(func(ctx context.Context, failure kafka.Error) {
			slog.ErrorContext(ctx, "kafka message failed",
				slog.String("stage", string(failure.Stage)), slog.Int("attempt", failure.Attempt),
				slog.Bool("final", failure.Final), slog.Any("error", failure.Err))
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

	done := make(chan error, 1)
	go func() { done <- consumer.Run(context.Background()) }()

	// On shutdown: let running handlers finish within the deadline, wait for
	// Run, then close the client (deferred).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.Shutdown(ctx); err != nil {
		// The deadline passed: running handlers were canceled and their
		// records are left for redelivery.
		slog.WarnContext(ctx, "kafka consumer shutdown forced", slog.Any("error", err))
	}
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
