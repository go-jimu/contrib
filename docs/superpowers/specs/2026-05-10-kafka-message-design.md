# Kafka Message Provider Design

Date: 2026-05-10
Status: Draft for review

## Purpose

Add a Kafka-backed provider for the experimental `github.com/go-jimu/components/ddd/message` contract.

The module path is:

```text
message/kafka
```

This provider is both an implementation of Kafka publish/consume behavior and a validation pass for the current `ddd/message` API. If the implementation reveals that the upstream API shape is wrong, implementation should stop and the API should be revisited before continuing.

## Architecture Gate

- Gate level: Level 3, because this adds a new cross-context messaging adapter and validates a DDD integration message contract against a real broker.
- Bounded context / business capability: contrib provider capability for Kafka-backed integration messaging.
- Stable language / data authority: `message.Message`, `message.Kind`, `message.Publisher`, `message.Subscriber`, `message.Handler`, and `message.Router` from `github.com/go-jimu/components v0.8.0`.
- Affected aggregate, policy, or service: no business aggregate. Affects Kafka publish, consume, routing, retry, DLQ, and offset commit policy.
- Invariants and rules: provider must preserve message metadata, keep Kafka topic/partition/offset as infrastructure concerns, and avoid hiding problems in the experimental upstream API.
- Technical capability classification: Infrastructure adapter, with runtime delivery policy for retry and DLQ.
- Layer ownership: Application code creates and handles `message.Message`; this module adapts Kafka transport and owns broker-specific envelope, retry, DLQ, and commit behavior.
- Proceed / Stop: proceed with design. Stop during implementation if `ddd/message` must change.

## Dependencies

- `github.com/go-jimu/components v0.8.0`
- `github.com/twmb/franz-go v1.21.1`
- Test-only integration dependency: `github.com/testcontainers/testcontainers-go/modules/kafka v0.42.0`

`franz-go` is selected because it is pure Go, feature-complete, and exposes producer, consumer group, manual commit, and advanced Kafka options without requiring librdkafka.

## Module Shape

Proposed files:

```text
message/kafka/
  go.mod
  README.md
  kafka.go
  option.go
  record.go
  consumer.go
  retry.go
  errors.go
  *_test.go
```

`go.work` should include `./message/kafka`. Release automation should include the package if it should receive package-scoped tags.

## Public API

The primary constructors accept a caller-created `*kgo.Client`:

```go
func NewPublisher(client *kgo.Client, opts ...Option) message.Publisher
func NewConsumer(client *kgo.Client, opts ...Option) *Consumer

func (c *Consumer) Subscribe(handler message.Handler) error
func (c *Consumer) Run(ctx context.Context) error
func (c *Consumer) Close()
```

The caller owns normal Kafka configuration through `kgo.Opt`, such as brokers, TLS, SASL, compression, acks, idempotence, consumer group, topics, logging, and metrics. This provider must not re-wrap the full franz-go configuration surface.

An optional helper may be provided only as a convenience:

```go
func NewClient(opts ...kgo.Opt) (*kgo.Client, error)
```

It should call `kgo.NewClient` directly and add no hidden behavior.

## Message To Kafka Mapping

Publishing maps `message.Message` to `kgo.Record`:

| Message field | Kafka record field |
| --- | --- |
| `Message.Payload()` | `Record.Value`, encoded by the configured codec |
| `Message.Key()` | `Record.Key` |
| `Message.OccurredAt()` | `Record.Timestamp` |
| `Message.Headers()` | `Record.Headers` |
| `Message.ID()` | reserved header |
| `Message.Kind()` | reserved header and topic resolver input |

`Message.Key()` is the Kafka partition key. With the normal hash partitioner, records with the same key in the same topic are routed to the same partition, preserving per-partition ordering for that key. It is not the message ID.

Default reserved headers:

```text
jimu-message-id
jimu-message-kind
jimu-message-occurred-at
```

The default topic resolver maps `message.Kind` to the Kafka topic name. The kind header is still written so multi-kind topics remain possible.

Consumption reconstructs `message.Message` from a Kafka record. It should prefer the reserved kind header. If configured, a fallback may derive kind from the Kafka topic.

## Codec

The default codec uses protobuf:

- encode: `proto.Marshal(msg.Payload())`
- decode: create the expected protobuf message and `proto.Unmarshal(record.Value, payload)`

Because `message.Message` stores `proto.Message` but not a payload factory, the Kafka provider needs a registry or resolver for consumed payload types:

```go
type PayloadResolver func(message.Kind) (proto.Message, error)
func WithPayloadResolver(PayloadResolver) Option
```

The default resolver can return an error. Consumers that decode records must configure a resolver. Publishing does not require one.

If implementation shows that payload type resolution is awkward or belongs in `ddd/message`, stop and propose an upstream API change.

## Options

Adapter options focus on message semantics and delivery policy:

```go
type Option func(*options)

func WithTopicResolver(TopicResolver) Option
func WithKindResolver(KindResolver) Option
func WithPayloadResolver(PayloadResolver) Option
func WithCodec(Codec) Option
func WithErrorHandler(ErrorHandler) Option
func WithHeaderPrefix(string) Option
func WithRetryPolicy(RetryPolicy) Option
func WithRetryTopicResolver(TopicResolver) Option
func WithDLQTopicResolver(TopicResolver) Option
func WithCloseClient(bool) Option
```

`WithCloseClient(false)` should be the default because the caller supplied the client and may share it.

## Consume And Commit Semantics

Default consume flow:

```text
PollFetches(ctx)
  -> decode Kafka record into message.Message
  -> route through message.Router
  -> success: commit source offset
  -> failure: publish to retry topic or DLQ topic
```

Under the current `ddd/message` API, `Handler.Handle(ctx, msg) error` is treated as an experimental signal. The default assumption for this provider is:

- `nil`: the message was accepted by the handler path and the source offset can be committed.
- non-nil error: processing did not complete; do not commit the source offset unless retry or DLQ publication succeeds.

This is a design assumption, not a fixed upstream rule. If implementation shows that `Handler.Handle` should not return error, or that error does not map cleanly to Kafka commit behavior, stop and revisit `components/ddd/message`.

## Retry And DLQ

Retry and DLQ are in scope for the first version.

Default behavior:

```text
source topic record
  -> decode / route / handle
  -> success:
       commit source offset
  -> retryable failure:
       publish to retry topic
       publish success: commit source offset
       publish failure: do not commit source offset
  -> attempts exceeded or non-retryable failure:
       publish to DLQ topic
       publish success: commit source offset
       publish failure: do not commit source offset
```

Default topics:

```text
retry: <source-topic>.retry
dlq:   <source-topic>.dlq
```

Default max attempts:

```text
3
```

Retry/DLQ reserved headers:

```text
jimu-retry-attempt
jimu-original-topic
jimu-original-partition
jimu-original-offset
jimu-failed-stage
jimu-first-error
jimu-last-error
```

Decode failures may happen before a `message.Message` exists, so DLQ publishing must support raw Kafka records. Retry/DLQ logic cannot depend only on `message.Publisher`.

Kafka has no native per-message delay. The first version does not guarantee delayed retry. The envelope may reserve room for a future `next-at` header, but the implementation should not pretend that delayed retry is complete unless it is actually enforced.

## Error Handling

Error stages:

```text
poll
decode
unhandled
handle
retry_publish
dlq_publish
commit
```

`ErrorHandler` observes errors and decides whether the consume loop continues:

```go
type ErrorHandler func(context.Context, ErrorContext) error
```

Default behavior returns the original error and exits `Run(ctx)`. If a custom handler returns nil, the consumer may continue after applying the retry/DLQ/commit rule for that failure.

`ErrorHandler` must not silently convert a failed source record into a committed offset unless the configured retry/DLQ path has durably accepted that record.

## Upstream API Stability

`ddd/message` is experimental. This provider should validate, not work around, the upstream API.

Implementation must pause and report if any of these appear wrong:

- `Message` lacks data needed for real broker adapters.
- `Kind` and topic/routing semantics do not compose.
- `Payload` decoding requires a core API addition.
- `Publisher.Publish` semantics are too weak or misleading.
- `Subscriber`, `Handler`, or `Router` force the adapter to own application concerns.
- `Handler.Handle` error semantics conflict with offset commit, retry, or DLQ behavior.
- Ack, commit, retry, or DLQ concepts need to move into core API rather than stay adapter-specific.

If such a mismatch appears, first propose changes to `github.com/go-jimu/components/ddd/message`, then continue the provider after the contract is clarified.

## Tests

Unit tests should be real tests of this provider's implementation, without a live Kafka broker by default.

Planned coverage:

- message-to-record mapping preserves key, timestamp, kind, ID, headers, and protobuf payload.
- record-to-message mapping restores kind, ID, occurrence time, headers, key, and protobuf payload.
- default topic resolver maps kind to topic.
- empty topic is rejected.
- consumer routes decoded messages through the router.
- successful handler path commits source offset.
- decode failure goes to DLQ.
- unhandled kind goes to retry or DLQ according to policy.
- handler error goes to retry or DLQ according to policy.
- retry/DLQ publish success commits source offset.
- retry/DLQ publish failure does not commit source offset.
- commit error is surfaced through `ErrorHandler`.
- reserved headers are copied and protected from accidental user-header overwrite.

Build-tagged integration tests should use `testcontainers-go/modules/kafka` to exercise the provider against a real Kafka broker when Docker is available:

- publish a `message.Message` through `NewPublisher`, consume it through `Consumer.Run`, and verify handler-visible `Message` fields.
- route a handler failure through retry or DLQ and verify the destination topic receives the failure record.

These tests should run with `go test -tags=integration ./...`; they should not be required by default `make test`.

## Non-Goals

The first version does not include:

- transactional producer/consumer
- exactly-once end-to-end semantics
- outbox storage or outbox relay
- schema registry integration
- guaranteed delayed retry
- re-wrapping all franz-go options

Retry and DLQ are in scope. They are adapter-level Kafka failure routing, not a full workflow or business retry framework.

## Success Criteria

- `message/kafka` is an independent Go module.
- `go.work` includes `./message/kafka`.
- The module uses `github.com/go-jimu/components v0.8.0`.
- `NewPublisher` implements `message.Publisher`.
- `Consumer` implements `message.Subscriber`.
- Consumer run loop supports route, retry, DLQ, and commit behavior.
- README documents producer and consumer examples, retry/DLQ defaults, and `Message.Key()` as Kafka partition key.
- `make test` passes from the repository root.
