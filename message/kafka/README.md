# Kafka Message

Kafka provider for `github.com/go-jimu/components/ddd/message`.

The package adapts the experimental `components/ddd/message` publisher and
subscriber contracts to Kafka through `github.com/twmb/franz-go`.

Use this module when an application already owns a franz-go `*kgo.Client` and
wants to publish or consume `message.Message` values through Kafka. The caller
keeps normal Kafka configuration in franz-go options; this package maps the
`ddd/message` contract to Kafka records and owns retry/DLQ/commit behavior.

## Install

```sh
go get github.com/go-jimu/contrib/message/kafka@latest
```

## Quick Start Checklist

1. Create a franz-go client with `kafka.NewClient` or `kgo.NewClient`.
2. For publishing, create a `message.Message` and call `NewPublisher(client).Publish`.
3. For consuming, configure `kgo.ConsumerGroup`, `kgo.ConsumeTopics`, and `kgo.DisableAutoCommit()`.
4. Register protobuf payload factories with `message.NewPayloadRegistry`.
5. Create `NewConsumer(client, failurePolicy, kafka.WithPayloadResolver(registry))`.
6. Subscribe `message.Handler` values and call `consumer.Run(ctx)`.

## Message Mapping

| `message.Message` field | Kafka record mapping |
| --- | --- |
| `Payload()` | `Record.Value`, encoded with protobuf by default |
| `Key()` | `Record.Key`; use it for per-key partition affinity and ordering |
| `OccurredAt()` | `Record.Timestamp` and reserved metadata header |
| `Headers()` | Kafka headers, except reserved adapter metadata headers |
| `ID()` | reserved Kafka header |
| `Kind()` | reserved Kafka header and default topic name |

By default, `Kind()` is used as the Kafka topic. Use `WithTopicResolver` when
topic names differ from semantic message kinds.

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

	registry := message.NewPayloadRegistry()
	if err := registry.Register("orders.paid", func() proto.Message {
		return &testdata.TestModel{}
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

`WithPayloadResolver` accepts the upstream `message.PayloadResolver` interface.
`message.PayloadRegistry` is the default setup helper for mapping incoming
message kinds to empty protobuf messages so the adapter can unmarshal Kafka
record values.
For small setups, `kafka.WithPayloadResolverFunc` can adapt a function into the
same interface.
The `encoding/testdata` type in the example is only a placeholder; real
projects should replace it with their own generated protobuf message type.

Consumers must configure `kgo.DisableAutoCommit()`. The provider commits
offsets manually after the handler succeeds, a configured message-level failure
action completes, or an explicit drop policy chooses to commit the failed source.
If franz-go auto commit is enabled, it can commit offsets outside the provider's
commit semantics.

`message.Handler.Handle` errors are message-level failures. They do not stop
`Run` by themselves. The required `FailurePolicy` decides whether the failed
source record is dropped, retried, or published to DLQ. `Consumer.Run` returns
only when the runtime cannot continue, the context is canceled, commit fails, or
failure publishing fails with the stop fallback.

- return `nil` when processing is complete and the source offset may be committed;
- return an error when the provider should apply the configured failure policy;
- handle business failures inside the handler and return `nil` when redelivery
  is not desired.

## Retry And DLQ

Retry and DLQ are disabled until the caller configures both a policy and failure
topics. This avoids silently depending on undeclared Kafka topics such as
`<source-topic>.retry` or `<source-topic>.dlq`.

Use `RetryThenDLQPolicy` with explicit topic resolvers:

```go
consumer, err := kafka.NewConsumer(
	client,
	kafka.RetryThenDLQPolicy(kafka.RetryThenDLQConfig{
		MaxAttempts: 3,
	}),
	kafka.WithPayloadResolver(registry),
	kafka.WithDefaultFailureTopics(),
)
```

`WithDefaultFailureTopics` routes retry records to `<source-topic>.retry` and
DLQ records to `<source-topic>.dlq`. You can replace that naming with
`WithRetryTopicResolver` and `WithDLQTopicResolver`. When consuming records from
a retry topic, the default resolvers prefer the original-topic metadata header,
so a later retry uses `orders.retry` instead of `orders.retry.retry`.
The provider does not create or validate failure topics; provision them in Kafka
or enable auto topic creation explicitly. If a configured retry/DLQ publish
fails, the default fallback stops `Run` and leaves the source offset uncommitted.

The built-in policies are:

| Policy | Behavior |
| --- | --- |
| `DropAndCommitPolicy` | Drop every message-level failure and commit the source offset. |
| `DLQPolicy` | Publish every message-level failure to DLQ. |
| `RetryThenDLQPolicy` | Retry handler failures until the attempt limit, then DLQ; decode and unhandled-kind failures go directly to DLQ. |

The source offset is committed only after one of these outcomes:

- the handler returns `nil`;
- the policy drops the message-level failure;
- publishing the retry record succeeds;
- publishing the DLQ record succeeds.

If retry or DLQ publishing fails, the source offset is not committed, allowing
Kafka to redeliver the source record according to the consumer group behavior.
Use `WithFailurePublishFallback(FailurePublishFallbackDrop)` only when losing
the original failed record is an intentional tradeoff.

## Publisher Retry

`NewPublisher` does one produce attempt by default. Use `WithPublishRetry` to
retry transient `ProduceSync` failures inside the adapter:

```go
publisher := kafka.NewPublisher(
	client,
	kafka.WithPublishRetry(3, func(attempt int) time.Duration {
		return time.Duration(attempt) * 100 * time.Millisecond
	}),
)
```

Message-to-record mapping errors, such as an empty topic, are returned before
producing and are not retried.

## Defaults

| Setting | Default |
| --- | --- |
| Codec | Protobuf marshal/unmarshal through `ProtoCodec` |
| Topic resolver | `message.Kind` string |
| Kind resolver | reserved Kafka message-kind header |
| Payload resolver | unset; consuming requires `WithPayloadResolver` |
| Reserved headers | `jimu-message-id`, `jimu-message-kind`, `jimu-message-occurred-at` |
| Consumer failure policy | required constructor argument |
| Retry topic | disabled until `WithDefaultFailureTopics` or `WithRetryTopicResolver` |
| DLQ topic | disabled until `WithDefaultFailureTopics` or `WithDLQTopicResolver` |
| Failure publish fallback | stop `Run` and leave the source offset uncommitted |
| Publisher retry | one produce attempt |
| Client ownership | caller-owned; `Consumer.Close` closes the client only with `WithCloseClient(true)` |

## Options

| Option | Purpose |
| --- | --- |
| `WithCodec` | Replace protobuf encoding/decoding. |
| `WithTopicResolver` | Map `message.Message` to a Kafka topic. |
| `WithKindResolver` | Derive `message.Kind` from a consumed Kafka record. |
| `WithPayloadResolver` | Configure the upstream `message.PayloadResolver` used for decode. |
| `WithPayloadResolverFunc` | Convenience wrapper for function-based payload resolvers. |
| `WithDefaultFailureTopics` | Enable `<source-topic>.retry` and `<source-topic>.dlq` topic naming. |
| `WithRetryTopicResolver` | Change retry topic selection. |
| `WithDLQTopicResolver` | Change DLQ topic selection. |
| `WithFailurePublishFallback` | Choose whether retry/DLQ publish failure stops `Run` or drops and commits the source. |
| `WithPublishRetry` | Retry transient publisher `ProduceSync` failures. |
| `WithErrorObserver` | Observe provider failures without controlling consumer flow. |
| `WithCloseClient` | Let the provider close the franz-go client when `Consumer.Close` is called. |
| `WithHeaderNames` | Override reserved metadata header names. |

`WithRetryPolicy`, `WithDLQDisabled`, and `WithErrorHandler` are deprecated.
Use `FailurePolicy`, explicit failure topic options, and `WithErrorObserver`.

## Error Surface

Most provider errors wrap one of the package sentinel errors in `Error.Err` and
include a `Stage` such as `decode`, `handle`, `retry_publish`, `dlq_publish`, or
`commit`. Use `WithErrorObserver` to record these failures. Observer return
values do not control flow; message-level decisions belong to `FailurePolicy`,
and runtime-level failures are returned from `Consumer.Run`.

Payload allocation errors come from the upstream `message.PayloadResolver`.
For example, a resolver or registry factory returning nil is reported as
`message.ErrNilPayloadFactory`; a missing resolver configuration is reported as
`kafka.ErrNoPayloadResolver`.

## Contract Stability

`github.com/go-jimu/components/ddd/message` is experimental. If real Kafka
behavior requires a core contract change, change the upstream components
contract first instead of adding adapter-specific workarounds here.

## Integration Tests

Integration tests use Testcontainers and require Docker:

```sh
cd message/kafka && go test -tags=integration ./...
```
