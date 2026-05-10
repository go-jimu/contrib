//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type integrationHandler struct {
	kinds  []message.Kind
	handle func(context.Context, message.Message) error
}

func (h integrationHandler) Listening() []message.Kind {
	return h.kinds
}

func (h integrationHandler) Handle(ctx context.Context, msg message.Message) error {
	return h.handle(ctx, msg)
}

func startKafka(t *testing.T) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := tckafka.Run(
		ctx,
		"confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID(fmt.Sprintf("jimu-it-%d", time.Now().UnixNano())),
	)
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() {
		terminateCtx, terminateCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer terminateCancel()
		if err := container.Terminate(terminateCtx); err != nil {
			t.Errorf("terminate kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("get kafka brokers: %v", err)
	}
	return brokers
}

func newIntegrationClient(t *testing.T, brokers []string, group string, topics ...string) *kgo.Client {
	t.Helper()

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	if group != "" {
		opts = append(opts, kgo.ConsumerGroup(group))
	}
	if len(topics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(topics...))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("new kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func integrationPayloadResolver(message.Kind) (proto.Message, error) {
	return &testdata.TestModel{}, nil
}

// Intent: a message published through a real Kafka broker should be consumed with its kind, key, headers, and protobuf payload intact.
func TestIntegrationPublishAndConsumeMessage(t *testing.T) {
	brokers := startKafka(t)
	topic := uniqueIntegrationName("orders-paid")
	group := uniqueIntegrationName("orders-paid-group")
	client := newIntegrationClient(t, brokers, group, topic)
	consumer := NewConsumer(client, WithPayloadResolver(integrationPayloadResolver))
	publisher := NewPublisher(client)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	received := make(chan message.Message, 1)
	if err := consumer.Subscribe(integrationHandler{
		kinds: []message.Kind{message.Kind(topic)},
		handle: func(_ context.Context, msg message.Message) error {
			received <- msg
			cancelRun()
			return nil
		},
	}); err != nil {
		t.Fatalf("subscribe handler: %v", err)
	}

	runErr := make(chan error, 1)
	go func() {
		runErr <- consumer.Run(runCtx)
	}()

	msg, err := message.New(
		message.Kind(topic),
		&testdata.TestModel{Id: 7, Name: "paid"},
		message.WithKey("order-7"),
		message.WithHeader("trace_id", "trace-1"),
	)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	if err := publisher.Publish(context.Background(), msg); err != nil {
		t.Fatalf("publish message: %v", err)
	}

	var got message.Message
	select {
	case got = <-received:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for consumed message")
	}

	if got.Kind() != message.Kind(topic) {
		t.Fatalf("kind = %q, want %q", got.Kind(), topic)
	}
	if got.Key() != "order-7" {
		t.Fatalf("key = %q, want order-7", got.Key())
	}
	if got.Headers()["trace_id"] != "trace-1" {
		t.Fatalf("trace header = %q, want trace-1", got.Headers()["trace_id"])
	}
	payload, ok := got.Payload().(*testdata.TestModel)
	if !ok {
		t.Fatalf("payload type = %T, want *testdata.TestModel", got.Payload())
	}
	if payload.GetId() != 7 || payload.GetName() != "paid" {
		t.Fatalf("payload = {Id:%d Name:%q}, want {Id:7 Name:%q}", payload.GetId(), payload.GetName(), "paid")
	}

	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for consumer Run to exit")
	}
}

// Intent: a handler failure against a real Kafka broker should publish a retry record with metadata that identifies the original topic and failed stage.
func TestIntegrationHandlerErrorPublishesRetryRecord(t *testing.T) {
	brokers := startKafka(t)
	sourceTopic := uniqueIntegrationName("orders-source")
	retryTopic := sourceTopic + ".retry"
	sourceGroup := uniqueIntegrationName("orders-source-group")
	retryGroup := uniqueIntegrationName("orders-retry-group")
	sourceClient := newIntegrationClient(t, brokers, sourceGroup, sourceTopic)
	retryClient := newIntegrationClient(t, brokers, retryGroup, retryTopic)
	consumer := NewConsumer(sourceClient, WithPayloadResolver(integrationPayloadResolver))
	publisher := NewPublisher(sourceClient)

	handlerErr := errors.New("handler failed")
	if err := consumer.Subscribe(integrationHandler{
		kinds: []message.Kind{message.Kind(sourceTopic)},
		handle: func(context.Context, message.Message) error {
			return handlerErr
		},
	}); err != nil {
		t.Fatalf("subscribe handler: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runErr := make(chan error, 1)
	go func() {
		runErr <- consumer.Run(runCtx)
	}()

	msg, err := message.New(
		message.Kind(sourceTopic),
		&testdata.TestModel{Id: 7, Name: "paid"},
		message.WithKey("order-7"),
		message.WithHeader("trace_id", "trace-1"),
	)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	if err := publisher.Publish(context.Background(), msg); err != nil {
		t.Fatalf("publish source message: %v", err)
	}

	record := pollOneRecord(t, retryClient, 30*time.Second)
	if record.Topic != retryTopic {
		t.Fatalf("retry topic = %q, want %q", record.Topic, retryTopic)
	}
	if string(record.Key) != "order-7" {
		t.Fatalf("retry key = %q, want order-7", record.Key)
	}
	if got := headerValue(record.Headers, headerRetryAttempt); got != "1" {
		t.Fatalf("retry attempt header = %q, want 1", got)
	}
	if got := headerValue(record.Headers, headerOriginalTopic); got != sourceTopic {
		t.Fatalf("original topic header = %q, want %q", got, sourceTopic)
	}
	if got := headerValue(record.Headers, headerFailedStage); got != string(StageHandle) {
		t.Fatalf("failed stage header = %q, want %q", got, StageHandle)
	}

	cancelRun()
	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for source consumer Run to exit")
	}
}

func pollOneRecord(t *testing.T, client *kgo.Client, timeout time.Duration) *kgo.Record {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for kafka record: %v", ctx.Err())
		}
		for _, fetchErr := range fetches.Errors() {
			if errors.Is(fetchErr.Err, context.Canceled) || errors.Is(fetchErr.Err, context.DeadlineExceeded) {
				t.Fatalf("poll retry record: %v", fetchErr.Err)
			}
		}
		for iter := fetches.RecordIter(); !iter.Done(); {
			return iter.Next()
		}
	}
}

func uniqueIntegrationName(prefix string) string {
	return fmt.Sprintf("jimu-it-%s-%d", prefix, time.Now().UnixNano())
}
