# Kafka Message

Kafka provider for `github.com/go-jimu/components/ddd/message`, built on
`github.com/twmb/franz-go`.

The contracts live in the package documentation, which `go doc -all
github.com/go-jimu/contrib/message/kafka` prints: where the import belongs,
the record mapping, choosing a `FailurePolicy`, the `PayloadResolver`
requirement, offset commits, the `Consumer.Run` lifecycle and shutdown order,
and error identities. This README walks through complete programs;
`example_test.go` runs the policy outcomes and compiles the composition.

## Install

```sh
go get github.com/go-jimu/contrib/message/kafka@latest
```

## Publisher

Runtime code builds the publisher; application code receives it as a
`message.Publisher`.

```go
package main

import (
	"context"

	"github.com/go-jimu/components/ddd/message"
	ordersv1 "example.com/shop/gen/orders/v1" // your generated protobuf package
	"github.com/go-jimu/contrib/message/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func publish(ctx context.Context) error {
	client, err := kafka.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.SnappyCompression()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	publisher := kafka.NewPublisher(client)

	payload := &ordersv1.OrderPaidV1{OrderId: "order-7"}
	msg, err := message.New(
		message.KindOf(payload), // "orders.v1.OrderPaidV1", also the topic
		payload,
		message.WithKey("order-7"),
		message.WithHeader("trace_id", "trace-1"),
	)
	if err != nil {
		return err
	}

	return publisher.Publish(ctx, msg)
}
```

`ordersv1.OrderPaidV1` stands for your own generated protobuf message. The
Kind, and so the default topic, is its protobuf full name.

## Consumer

```go
package main

import (
	"context"
	"fmt"

	"github.com/go-jimu/components/ddd/message"
	ordersv1 "example.com/shop/gen/orders/v1" // your generated protobuf package
	"github.com/go-jimu/contrib/message/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type paidHandler struct{}

func (paidHandler) Listening() []message.Kind {
	return []message.Kind{message.KindOf(&ordersv1.OrderPaidV1{})}
}

func (paidHandler) Handle(ctx context.Context, msg message.Message) error {
	paid, ok := msg.Payload().(*ordersv1.OrderPaidV1)
	if !ok {
		return fmt.Errorf("unexpected payload %T", msg.Payload())
	}
	_ = paid // delegate once to an application use case
	return nil
}

func run(ctx context.Context) error {
	client, err := kafka.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("orders-service"),
		kgo.ConsumeTopics("orders.v1.OrderPaidV1"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	registry := message.NewPayloadRegistry()
	if err := registry.Register(message.KindOf(&ordersv1.OrderPaidV1{}), func() proto.Message {
		return &ordersv1.OrderPaidV1{}
	}); err != nil {
		return err
	}

	consumer, err := kafka.NewConsumer(
		client,
		kafka.DropAndCommitPolicy(),
		kafka.WithPayloadResolver(registry),
	)
	if err != nil {
		return err
	}

	if err := consumer.Subscribe(paidHandler{}); err != nil {
		return err
	}

	return consumer.Run(ctx)
}
```

`DropAndCommitPolicy` loses failed messages. See "Choosing a FailurePolicy" in
the package documentation before picking one.

## Retry and DLQ

`RetryThenDLQPolicy` needs failure topics and a client that also consumes the
retry topics:

```go
client, err := kafka.NewClient(
	kgo.SeedBrokers("localhost:9092"),
	kgo.ConsumerGroup("orders-service"),
	kgo.ConsumeTopics("orders.v1.OrderPaidV1", "orders.v1.OrderPaidV1.retry"),
	kgo.DisableAutoCommit(),
)

consumer, err := kafka.NewConsumer(
	client,
	kafka.RetryThenDLQPolicy(kafka.RetryThenDLQConfig{MaxAttempts: 3}),
	kafka.WithPayloadResolver(registry),
	kafka.WithDefaultFailureTopics(),
)
```

Provision `orders.v1.OrderPaidV1.retry` and `orders.v1.OrderPaidV1.dlq` in Kafka, or replace the
naming with `WithRetryTopicResolver` and `WithDLQTopicResolver`.

## Publisher Retry

```go
publisher := kafka.NewPublisher(
	client,
	kafka.WithPublishRetry(3, func(attempt int) time.Duration {
		return time.Duration(attempt) * 100 * time.Millisecond
	}),
)
```

## Deprecated options

`WithRetryPolicy`, `WithDLQDisabled`, and `WithErrorHandler` are deprecated
and have no effect on failure flow. Use a `FailurePolicy`, explicit failure
topic options, and `WithErrorObserver`.

## Integration Tests

Integration tests use Testcontainers and require Docker:

```sh
cd message/kafka && go test -tags=integration ./...
```
