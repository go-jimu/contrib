# Kafka Message

Kafka provider for `github.com/go-jimu/components/ddd/message`.

The package adapts the experimental `components/ddd/message` publisher and
subscriber contracts to Kafka through `github.com/twmb/franz-go`.

## Install

```sh
go get github.com/go-jimu/contrib/message/kafka@latest
```

## Publisher

```go
package main

import (
	"context"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
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

	msg, err := message.New(
		"orders.paid",
		&testdata.TestModel{Id: 7, Name: "paid"},
		message.WithKey("order-7"),
		message.WithHeader("trace_id", "trace-1"),
	)
	if err != nil {
		return err
	}

	return publisher.Publish(ctx, msg)
}
```

By default, the message kind is used as the Kafka topic. Use
`kafka.WithTopicResolver` when the topic name should differ from
`message.Message.Kind()`.

`Message.Key()` maps to the Kafka record key. Within the same topic, normal
Kafka hash partitioning routes records with the same key to the same partition,
which preserves ordering for that key within that partition.

## Consumer

```go
package main

import (
	"context"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	"github.com/go-jimu/contrib/message/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type paidHandler struct{}

func (paidHandler) Listening() []message.Kind {
	return []message.Kind{"orders.paid"}
}

func (paidHandler) Handle(ctx context.Context, msg message.Message) error {
	_ = ctx
	_ = msg
	return nil
}

func run(ctx context.Context) error {
	client, err := kafka.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("orders-service"),
		kgo.ConsumeTopics("orders.paid"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	consumer := kafka.NewConsumer(
		client,
		kafka.WithPayloadResolver(func(kind message.Kind) (proto.Message, error) {
			switch kind {
			case "orders.paid":
				return &testdata.TestModel{}, nil
			default:
				return nil, kafka.ErrNoPayloadResolver
			}
		}),
	)

	if err := consumer.Subscribe(paidHandler{}); err != nil {
		return err
	}

	return consumer.Run(ctx)
}
```

`WithPayloadResolver` must return an empty protobuf message for the incoming
message kind so the adapter can unmarshal the Kafka record value.

## Retry And DLQ

By default, handler and unhandled-kind failures are retried on
`<source-topic>.retry`. When attempts are exhausted, or when the failure is not
retryable, the source record is published to `<source-topic>.dlq`. The default
maximum attempt count is `3`.

The source offset is committed only after one of these outcomes:

- the handler returns `nil`;
- publishing the retry record succeeds;
- publishing the DLQ record succeeds.

If retry or DLQ publishing fails, the source offset is not committed, allowing
Kafka to redeliver the source record according to the consumer group behavior.

Retry behavior can be customized with `kafka.WithRetryPolicy`,
`kafka.WithRetryTopicResolver`, `kafka.WithDLQTopicResolver`, and
`kafka.WithDLQDisabled`.

## Contract Stability

`github.com/go-jimu/components/ddd/message` is experimental. If real Kafka
behavior requires a core contract change, change the upstream components
contract first instead of adding adapter-specific workarounds here.

## Integration Tests

Integration tests use Testcontainers and require Docker:

```sh
go test -tags=integration ./...
```
