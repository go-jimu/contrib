# Kafka Message

Kafka provider for `github.com/go-jimu/components/ddd/message`, built on
`github.com/twmb/franz-go`.

The contracts live in the package documentation, which `go doc -all
github.com/go-jimu/contrib/message/kafka` prints: where the import belongs,
the record mapping, choosing a `FailurePolicy`, dead letters, the
`PayloadResolver` requirement, offsets and rebalances, the `Consumer.Run`
lifecycle and shutdown order,
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
	"time"

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
		kgo.DisableAutoCommit(),    // required: the consumer commits
		kgo.BlockRebalanceOnPoll(), // required: no rebalance mid-batch
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
		kafka.RetryPolicy(kafka.RetryConfig{}),
		kafka.WithPayloadResolver(registry),
		kafka.WithHandlerTimeout(30*time.Second),
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

`RetryPolicy` retries a handler failure in place, after a backoff, keeping the
partition's order, and drops the record after 3 attempts. See "Choosing a
FailurePolicy" in the package documentation before picking one.

## Failed messages

The consumer publishes no retry or dead-letter records. A handler marks a
failure a retry cannot fix with `kafka.NonRetryable(err)`. To keep the records
the policy drops, set a `Recoverer`; the offset is committed only after it
succeeds:

```go
consumer, err := kafka.NewConsumer(
	client,
	kafka.RetryPolicy(kafka.RetryConfig{MaxAttempts: 5}),
	kafka.WithPayloadResolver(registry),
	kafka.WithRecoverer(func(ctx context.Context, failure kafka.Error) error {
		return failedMessages.Save(ctx, failure.Record, failure.Err) // your table
	}),
	kafka.WithErrorObserver(func(ctx context.Context, failure kafka.Error) {
		if failure.Final || failure.Stage == kafka.StageRecover {
			alert(ctx, failure)
		}
	}),
)
```

## Share groups

For messages that need no order, `ShareConsumer` consumes a Kafka share group
(Kafka 4.2+). Retries and dead letters are the broker's: a released record is
delivered again, and a rejected one is archived or written to the group's
dead-letter topic when the broker supports it.

```go
client, err := kafka.NewClient(
	kgo.SeedBrokers("localhost:9092"),
	kgo.ShareGroup("notifications-service"),
	kgo.ConsumeTopics("orders.v1.OrderPaidV1"),
)

consumer, err := kafka.NewShareConsumer(
	client,
	kafka.ReleaseOrRejectPolicy(),
	kafka.WithPayloadResolver(registry),
	kafka.WithConcurrency(16),
	kafka.WithHandlerTimeout(20*time.Second), // below the 30s acquisition lock
)
```

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

`WithErrorHandler` is deprecated; use `WithErrorObserver`.

## Integration Tests

Integration tests use Testcontainers with the `apache/kafka` image and require
Docker:

```sh
cd message/kafka && go test -tags=integration ./...
```
