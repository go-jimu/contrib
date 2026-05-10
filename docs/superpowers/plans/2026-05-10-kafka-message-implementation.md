# Kafka Message Provider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `message/kafka`, a Kafka provider for `github.com/go-jimu/components/ddd/message` with publish, consume, retry topic, DLQ topic, and offset commit behavior.

**Architecture:** The provider is an independent Go module that accepts caller-created `*kgo.Client` instances and adapts Kafka records to the experimental `ddd/message` contract. Kafka envelope, retry, DLQ, and commit behavior stay inside this adapter; application code owns message creation and handler behavior. Implementation must stop and report if the current `ddd/message` API does not fit real Kafka adapter needs.

**Tech Stack:** Go 1.25 toolchain, `github.com/go-jimu/components v0.8.0`, `github.com/twmb/franz-go v1.21.1`, protobuf via `google.golang.org/protobuf/proto`, tests with Go `testing` and `github.com/stretchr/testify/require`.

---

## Source Spec

- `docs/superpowers/specs/2026-05-10-kafka-message-design.md`

## Scope Check

This plan covers one provider module: `message/kafka`.

In scope:

- Kafka publisher implementing `message.Publisher`.
- Kafka consumer implementing `message.Subscriber`.
- Record/message mapping.
- Protobuf codec and payload resolver.
- Retry topic and DLQ topic behavior.
- Offset commit after successful processing or successful retry/DLQ handoff.
- README examples.

Out of scope:

- Outbox storage or relay.
- Kafka transactions / exactly-once end-to-end semantics.
- Schema registry integration.
- Guaranteed delayed retry.
- Re-wrapping franz-go configuration.

## Architecture Gate

- Gate level: Level 3.
- Bounded context / business capability: Kafka-backed integration messaging provider for contrib.
- Stable language / data authority: `message.Message`, `message.Kind`, `message.Publisher`, `message.Subscriber`, `message.Handler`, and `message.Router` from `github.com/go-jimu/components v0.8.0`.
- Affected aggregate, policy, or service: no aggregate; Kafka adapter runtime delivery policy.
- Invariants and rules: preserve message metadata; never hide upstream `ddd/message` contract problems; commit source offsets only after success or durable retry/DLQ handoff.
- Technical capability classification: Infrastructure adapter with retry/DLQ delivery policy.
- Layer ownership: Application code owns message semantics and handlers; Infrastructure owns Kafka records, headers, offsets, retry, and DLQ.
- Proceed / Stop: proceed task-by-task; stop if `ddd/message` API needs correction.

## File Structure

- Create `message/kafka/go.mod`: independent Go module.
- Create `message/kafka/doc.go`: package docs and experimental contract warning.
- Create `message/kafka/errors.go`: sentinel adapter errors and stage names.
- Create `message/kafka/option.go`: options, resolvers, retry policy, error handler types.
- Create `message/kafka/codec.go`: protobuf codec and payload resolver integration.
- Create `message/kafka/headers.go`: reserved header names, header encode/decode helpers.
- Create `message/kafka/record.go`: `message.Message` ↔ `kgo.Record` mapping and raw record cloning.
- Create `message/kafka/publisher.go`: `message.Publisher` implementation.
- Create `message/kafka/retry.go`: retry/DLQ decision and topic resolution.
- Create `message/kafka/consumer.go`: router, poll loop, per-record processing, commit behavior.
- Create `message/kafka/README.md`: install and usage examples.
- Modify `go.work`: include `./message/kafka`.
- Modify `.github/workflows/ci.yml`: add `message/kafka` to package tag release matrix.
- Update `docs/project-knowledge/*` after code is complete with `superpowers-memory:update`.

## Intent-First Test List

Intent source: approved Kafka provider spec.

- [ ] unit real: default topic resolver receives valid kind -> returns kind as topic.
- [ ] unit real: default topic resolver receives empty kind -> returns an empty-topic error.
- [ ] unit real: message-to-record mapping with ID/kind/key/occurredAt/headers/payload -> produces a Kafka record with expected topic, key, timestamp, value, and reserved headers.
- [ ] unit real: user headers collide with reserved headers -> reserved headers win and user cannot corrupt adapter metadata.
- [ ] unit real: record-to-message mapping with valid headers and payload resolver -> reconstructs `message.Message`.
- [ ] unit real: record-to-message mapping without payload resolver -> returns payload resolver error.
- [ ] unit real: record-to-message mapping with invalid occurredAt header -> returns decode error.
- [ ] unit real: publisher with valid message -> calls `ProduceSync` and returns nil.
- [ ] unit real: publisher when `ProduceSync` returns an error -> returns the publish error.
- [ ] unit real: decode failure during consume -> publishes raw record to DLQ and commits source record after DLQ publish succeeds.
- [ ] unit real: unhandled kind during consume -> publishes record to DLQ and commits source record after DLQ publish succeeds.
- [ ] unit real: handler error below max attempts -> publishes record to retry topic and commits source record after retry publish succeeds.
- [ ] unit real: handler error at max attempts -> publishes record to DLQ and commits source record after DLQ publish succeeds.
- [ ] unit real: retry/DLQ publish failure -> does not commit source record and returns error.
- [ ] unit real: successful handler path -> commits source record and does not publish retry/DLQ.
- [ ] unit real: commit failure after success -> returns commit error through `Run`/processing path.
- [ ] seam real: `Consumer.Run` receives fetch errors -> calls `ErrorHandler` with `poll` stage and obeys returned error.
- [ ] seam real: `Consumer.Run` receives records -> routes each through the real consumer processing path.
- [ ] docs real: README examples compile with exported API names.

Regression protected:

- If envelope headers drift, consumers cannot route or deduplicate messages.
- If payload resolution is wrong, protobuf messages cannot cross service boundaries.
- If retry/DLQ handoff commits too early or too late, consumers lose messages or reprocess poison records indefinitely.
- If `message.Handler` semantics do not fit Kafka commit behavior, the implementation must surface the mismatch before release.

## Task 1: Scaffold Module And Public Types

**Files:**
- Create: `message/kafka/go.mod`
- Create: `message/kafka/doc.go`
- Create: `message/kafka/errors.go`
- Create: `message/kafka/option.go`
- Create: `message/kafka/codec.go`
- Modify: `go.work`
- Test: `message/kafka/options_test.go`

- [ ] **Step 1: Write failing option/default tests**

Create `message/kafka/options_test.go`:

```go
package kafka

import (
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/stretchr/testify/require"
)

// Intent: by default a message kind should be usable as the Kafka topic so a minimal publisher works without topic configuration.
func TestDefaultTopicResolverUsesKind(t *testing.T) {
	msg := mustMessage(t, "order.payment.v1.OrderPaid")

	topic, err := defaultTopicResolver(msg)

	require.NoError(t, err)
	require.Equal(t, "order.payment.v1.OrderPaid", topic)
}

// Intent: an empty resolved Kafka topic must fail before producing because Kafka records require a topic.
func TestDefaultTopicResolverRejectsEmptyKind(t *testing.T) {
	msg := message.Message{}

	topic, err := defaultTopicResolver(msg)

	require.ErrorIs(t, err, ErrEmptyTopic)
	require.Empty(t, topic)
}
```

Also create `message/kafka/test_helpers_test.go`:

```go
package kafka

import (
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	"github.com/stretchr/testify/require"
)

func mustMessage(t *testing.T, kind message.Kind, opts ...message.Option) message.Message {
	t.Helper()

	all := []message.Option{
		message.WithID("msg-1"),
		message.WithKey("order-1"),
		message.WithOccurredAt(time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)),
		message.WithHeader("trace_id", "trace-1"),
	}
	all = append(all, opts...)

	msg, err := message.New(kind, &testdata.TestModel{Id: 7, Name: "paid"}, all...)
	require.NoError(t, err)
	return msg
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd message/kafka && go test ./... -run 'TestDefaultTopicResolver' -count=1
```

Expected: FAIL because the module and functions do not exist.

- [ ] **Step 3: Create module files**

Create `message/kafka/go.mod`:

```go
module github.com/go-jimu/contrib/message/kafka

go 1.25.0

require (
	github.com/go-jimu/components v0.8.0
	github.com/stretchr/testify v1.11.1
	github.com/twmb/franz-go v1.21.1
	google.golang.org/protobuf v1.36.9
)
```

Create `message/kafka/doc.go`:

```go
// Package kafka adapts Kafka to github.com/go-jimu/components/ddd/message.
//
// This package is intentionally an infrastructure adapter. Kafka topics,
// partitions, offsets, retry topics, and DLQ topics remain outside the core
// message contract.
//
// The upstream ddd/message API is still experimental. If implementing this
// adapter reveals that the core API is awkward or incorrect for real broker
// usage, stop and fix the upstream contract before adding workarounds here.
package kafka
```

Create `message/kafka/errors.go`:

```go
package kafka

import "errors"

var (
	ErrNilClient         = errors.New("kafka client is nil")
	ErrEmptyTopic       = errors.New("kafka topic is empty")
	ErrNilPayload       = errors.New("kafka payload resolver returned nil payload")
	ErrNoPayloadResolver = errors.New("kafka payload resolver is not configured")
)

type Stage string

const (
	StagePoll         Stage = "poll"
	StageDecode       Stage = "decode"
	StageUnhandled    Stage = "unhandled"
	StageHandle       Stage = "handle"
	StageRetryPublish Stage = "retry_publish"
	StageDLQPublish   Stage = "dlq_publish"
	StageCommit       Stage = "commit"
)
```

Create `message/kafka/codec.go`:

```go
package kafka

import (
	"github.com/go-jimu/components/ddd/message"
	"google.golang.org/protobuf/proto"
)

type Codec interface {
	Marshal(proto.Message) ([]byte, error)
	Unmarshal([]byte, proto.Message) error
}

type ProtoCodec struct{}

func (ProtoCodec) Marshal(payload proto.Message) ([]byte, error) {
	return proto.Marshal(payload)
}

func (ProtoCodec) Unmarshal(data []byte, payload proto.Message) error {
	return proto.Unmarshal(data, payload)
}

type PayloadResolver func(message.Kind) (proto.Message, error)
```

Create `message/kafka/option.go`:

```go
package kafka

import (
	"context"
	"fmt"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TopicResolver func(message.Message) (string, error)
type KindResolver func(*kgo.Record) (message.Kind, error)
type FailureTopicResolver func(ErrorContext) (string, error)
type ErrorHandler func(context.Context, ErrorContext) error

type RetryPolicy struct {
	MaxAttempts int
	Retryable   func(ErrorContext) bool
}

type ErrorContext struct {
	Stage   Stage
	Record  *kgo.Record
	Message message.Message
	Err     error
}

type Option func(*options)

type options struct {
	topicResolver      TopicResolver
	kindResolver       KindResolver
	payloadResolver    PayloadResolver
	codec              Codec
	errorHandler       ErrorHandler
	headerPrefix       string
	retryPolicy        RetryPolicy
	retryTopicResolver FailureTopicResolver
	dlqTopicResolver   FailureTopicResolver
	closeClient        bool
}

func defaultOptions() options {
	return options{
		topicResolver:      defaultTopicResolver,
		kindResolver:       defaultKindResolver("jimu-"),
		codec:              ProtoCodec{},
		errorHandler:       defaultErrorHandler,
		headerPrefix:       "jimu-",
		retryPolicy:        RetryPolicy{MaxAttempts: 3, Retryable: defaultRetryable},
		retryTopicResolver: suffixFailureTopicResolver(".retry"),
		dlqTopicResolver:   suffixFailureTopicResolver(".dlq"),
		closeClient:        false,
	}
}

func WithTopicResolver(resolver TopicResolver) Option {
	return func(o *options) {
		if resolver != nil {
			o.topicResolver = resolver
		}
	}
}

func WithKindResolver(resolver KindResolver) Option {
	return func(o *options) {
		if resolver != nil {
			o.kindResolver = resolver
		}
	}
}

func WithPayloadResolver(resolver PayloadResolver) Option {
	return func(o *options) {
		o.payloadResolver = resolver
	}
}

func WithCodec(codec Codec) Option {
	return func(o *options) {
		if codec != nil {
			o.codec = codec
		}
	}
}

func WithErrorHandler(handler ErrorHandler) Option {
	return func(o *options) {
		if handler != nil {
			o.errorHandler = handler
		}
	}
}

func WithHeaderPrefix(prefix string) Option {
	return func(o *options) {
		if prefix != "" {
			o.headerPrefix = prefix
			o.kindResolver = defaultKindResolver(prefix)
		}
	}
}

func WithRetryPolicy(policy RetryPolicy) Option {
	return func(o *options) {
		if policy.MaxAttempts > 0 {
			o.retryPolicy.MaxAttempts = policy.MaxAttempts
		}
		if policy.Retryable != nil {
			o.retryPolicy.Retryable = policy.Retryable
		}
	}
}

func WithRetryTopicResolver(resolver FailureTopicResolver) Option {
	return func(o *options) {
		if resolver != nil {
			o.retryTopicResolver = resolver
		}
	}
}

func WithDLQTopicResolver(resolver FailureTopicResolver) Option {
	return func(o *options) {
		if resolver != nil {
			o.dlqTopicResolver = resolver
		}
	}
}

func WithCloseClient(closeClient bool) Option {
	return func(o *options) {
		o.closeClient = closeClient
	}
}

func applyOptions(opts []Option) options {
	cfg := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func defaultTopicResolver(msg message.Message) (string, error) {
	if msg.Kind() == "" {
		return "", ErrEmptyTopic
	}
	return string(msg.Kind()), nil
}

func defaultErrorHandler(_ context.Context, ctx ErrorContext) error {
	return ctx.Err
}

func defaultRetryable(ctx ErrorContext) bool {
	return ctx.Stage == StageHandle
}

func suffixFailureTopicResolver(suffix string) FailureTopicResolver {
	return func(ctx ErrorContext) (string, error) {
		if ctx.Record == nil || ctx.Record.Topic == "" {
			return "", fmt.Errorf("%w: source topic is empty", ErrEmptyTopic)
		}
		return ctx.Record.Topic + suffix, nil
	}
}
```

Modify `go.work` to add `./message/kafka` and raise the workspace Go directive for the new provider dependencies:

```go
go 1.25.0

use (
	.
	./config/apollo
	./config/etcd
	./config/nacos
	./config/kubernetes
	./logger/zap
	./message/kafka
)
```

- [ ] **Step 4: Run scaffold tests**

Run:

```bash
cd message/kafka && GOPROXY=direct go mod tidy && go test ./... -run 'TestDefaultTopicResolver' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit scaffold**

```bash
git add go.work message/kafka
git commit -m "feat: scaffold kafka message provider"
```

## Task 2: Record Envelope Mapping

**Files:**
- Create: `message/kafka/headers.go`
- Create: `message/kafka/record.go`
- Test: `message/kafka/record_test.go`

- [ ] **Step 1: Write failing record mapping tests**

Create `message/kafka/record_test.go`:

```go
package kafka

import (
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Intent: publishing must preserve message routing, identity, time, headers, and protobuf payload in the Kafka record envelope.
func TestMessageToRecordMapsMessageEnvelope(t *testing.T) {
	msg := mustMessage(t, "order.payment.v1.OrderPaid")
	cfg := defaultOptions()

	record, err := messageToRecord(msg, cfg)

	require.NoError(t, err)
	require.Equal(t, "order.payment.v1.OrderPaid", record.Topic)
	require.Equal(t, []byte("order-1"), record.Key)
	require.Equal(t, msg.OccurredAt(), record.Timestamp)
	require.Equal(t, "msg-1", headerValue(record.Headers, "jimu-message-id"))
	require.Equal(t, "order.payment.v1.OrderPaid", headerValue(record.Headers, "jimu-message-kind"))
	require.Equal(t, msg.OccurredAt().Format(time.RFC3339Nano), headerValue(record.Headers, "jimu-message-occurred-at"))
	require.Equal(t, "trace-1", headerValue(record.Headers, "trace_id"))
	require.NotEmpty(t, record.Value)
}

// Intent: user-supplied headers must not overwrite reserved adapter metadata used for routing and reconstruction.
func TestMessageToRecordReservedHeadersWin(t *testing.T) {
	msg := mustMessage(
		t,
		"order.payment.v1.OrderPaid",
		message.WithHeader("jimu-message-id", "user-id"),
		message.WithHeader("jimu-message-kind", "wrong.Kind"),
	)
	cfg := defaultOptions()

	record, err := messageToRecord(msg, cfg)

	require.NoError(t, err)
	require.Equal(t, "msg-1", headerValue(record.Headers, "jimu-message-id"))
	require.Equal(t, "order.payment.v1.OrderPaid", headerValue(record.Headers, "jimu-message-kind"))
}

// Intent: consuming must reconstruct the transport-neutral message so handlers do not depend on Kafka record details.
func TestRecordToMessageReconstructsMessage(t *testing.T) {
	occurredAt := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	payload := &testdata.TestModel{Id: 7, Name: "paid"}
	value, err := proto.Marshal(payload)
	require.NoError(t, err)
	record := &kgo.Record{
		Topic:     "order.payment.v1.OrderPaid",
		Key:       []byte("order-1"),
		Value:     value,
		Timestamp: occurredAt,
		Headers: []kgo.RecordHeader{
			{Key: "jimu-message-id", Value: []byte("msg-1")},
			{Key: "jimu-message-kind", Value: []byte("order.payment.v1.OrderPaid")},
			{Key: "jimu-message-occurred-at", Value: []byte(occurredAt.Format(time.RFC3339Nano))},
			{Key: "trace_id", Value: []byte("trace-1")},
		},
	}
	cfg := defaultOptions()
	cfg.payloadResolver = func(kind message.Kind) (proto.Message, error) {
		require.Equal(t, message.Kind("order.payment.v1.OrderPaid"), kind)
		return &testdata.TestModel{}, nil
	}

	msg, err := recordToMessage(record, cfg)

	require.NoError(t, err)
	require.Equal(t, "msg-1", msg.ID())
	require.Equal(t, message.Kind("order.payment.v1.OrderPaid"), msg.Kind())
	require.Equal(t, "order-1", msg.Key())
	require.Equal(t, occurredAt, msg.OccurredAt())
	require.Equal(t, map[string]string{"trace_id": "trace-1"}, msg.Headers())
	require.Equal(t, payload, msg.Payload())
}

// Intent: a consumer without a payload resolver should fail clearly instead of constructing a message with an unknown protobuf type.
func TestRecordToMessageRequiresPayloadResolver(t *testing.T) {
	record := &kgo.Record{
		Topic: "order.payment.v1.OrderPaid",
		Headers: []kgo.RecordHeader{
			{Key: "jimu-message-kind", Value: []byte("order.payment.v1.OrderPaid")},
		},
	}

	_, err := recordToMessage(record, defaultOptions())

	require.ErrorIs(t, err, ErrNoPayloadResolver)
}

// Intent: malformed occurrence time metadata should fail decoding instead of silently using broker append time.
func TestRecordToMessageRejectsInvalidOccurredAt(t *testing.T) {
	record := &kgo.Record{
		Topic: "order.payment.v1.OrderPaid",
		Headers: []kgo.RecordHeader{
			{Key: "jimu-message-kind", Value: []byte("order.payment.v1.OrderPaid")},
			{Key: "jimu-message-occurred-at", Value: []byte("not-time")},
		},
	}
	cfg := defaultOptions()
	cfg.payloadResolver = func(message.Kind) (proto.Message, error) {
		return &testdata.TestModel{}, nil
	}

	_, err := recordToMessage(record, cfg)

	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd message/kafka && go test ./... -run 'Test(MessageToRecord|RecordToMessage)' -count=1
```

Expected: FAIL because mapping helpers do not exist.

- [ ] **Step 3: Implement headers and mapping**

Create `message/kafka/headers.go`:

```go
package kafka

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	headerMessageID         = "message-id"
	headerMessageKind       = "message-kind"
	headerMessageOccurredAt = "message-occurred-at"
	headerRetryAttempt      = "retry-attempt"
	headerOriginalTopic     = "original-topic"
	headerOriginalPartition = "original-partition"
	headerOriginalOffset    = "original-offset"
	headerFailedStage       = "failed-stage"
	headerFirstError        = "first-error"
	headerLastError         = "last-error"
)

func headerKey(prefix, name string) string {
	return prefix + name
}

func appendHeader(headers []kgo.RecordHeader, key string, value string) []kgo.RecordHeader {
	return append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func retryAttempt(headers []kgo.RecordHeader, prefix string) int {
	raw := headerValue(headers, headerKey(prefix, headerRetryAttempt))
	if raw == "" {
		return 0
	}
	attempt, err := strconv.Atoi(raw)
	if err != nil || attempt < 0 {
		return 0
	}
	return attempt
}
```

Create `message/kafka/record.go`:

```go
package kafka

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

func messageToRecord(msg message.Message, cfg options) (*kgo.Record, error) {
	topic, err := cfg.topicResolver(msg)
	if err != nil {
		return nil, err
	}
	if topic == "" {
		return nil, ErrEmptyTopic
	}
	value, err := cfg.codec.Marshal(msg.Payload())
	if err != nil {
		return nil, err
	}

	headers := make([]kgo.RecordHeader, 0, len(msg.Headers())+3)
	reserved := map[string]struct{}{
		headerKey(cfg.headerPrefix, headerMessageID):         {},
		headerKey(cfg.headerPrefix, headerMessageKind):       {},
		headerKey(cfg.headerPrefix, headerMessageOccurredAt): {},
	}
	for key, value := range msg.Headers() {
		if _, ok := reserved[key]; ok {
			continue
		}
		headers = appendHeader(headers, key, value)
	}
	headers = appendHeader(headers, headerKey(cfg.headerPrefix, headerMessageID), msg.ID())
	headers = appendHeader(headers, headerKey(cfg.headerPrefix, headerMessageKind), string(msg.Kind()))
	headers = appendHeader(headers, headerKey(cfg.headerPrefix, headerMessageOccurredAt), msg.OccurredAt().Format(time.RFC3339Nano))

	return &kgo.Record{
		Topic:     topic,
		Key:       []byte(msg.Key()),
		Value:     value,
		Headers:   headers,
		Timestamp: msg.OccurredAt(),
	}, nil
}

func recordToMessage(record *kgo.Record, cfg options) (message.Message, error) {
	kind, err := cfg.kindResolver(record)
	if err != nil {
		return message.Message{}, err
	}
	if cfg.payloadResolver == nil {
		return message.Message{}, ErrNoPayloadResolver
	}
	payload, err := cfg.payloadResolver(kind)
	if err != nil {
		return message.Message{}, err
	}
	if payload == nil {
		return message.Message{}, ErrNilPayload
	}
	if err = cfg.codec.Unmarshal(record.Value, payload); err != nil {
		return message.Message{}, err
	}

	opts := []message.Option{message.WithKey(string(record.Key))}
	if id := headerValue(record.Headers, headerKey(cfg.headerPrefix, headerMessageID)); id != "" {
		opts = append(opts, message.WithID(id))
	}
	if occurredAt := headerValue(record.Headers, headerKey(cfg.headerPrefix, headerMessageOccurredAt)); occurredAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, occurredAt)
		if err != nil {
			return message.Message{}, err
		}
		opts = append(opts, message.WithOccurredAt(parsed))
	} else if !record.Timestamp.IsZero() {
		opts = append(opts, message.WithOccurredAt(record.Timestamp))
	}

	reserved := reservedHeaders(cfg.headerPrefix)
	for _, header := range record.Headers {
		if _, ok := reserved[header.Key]; ok {
			continue
		}
		opts = append(opts, message.WithHeader(header.Key, string(header.Value)))
	}

	return message.New(kind, payload, opts...)
}

func defaultKindResolver(prefix string) KindResolver {
	return func(record *kgo.Record) (message.Kind, error) {
		if record == nil {
			return "", fmt.Errorf("kafka record is nil")
		}
		if kind := headerValue(record.Headers, headerKey(prefix, headerMessageKind)); kind != "" {
			return message.Kind(kind), nil
		}
		if record.Topic != "" {
			return message.Kind(record.Topic), nil
		}
		return "", message.ErrEmptyKind
	}
}

func reservedHeaders(prefix string) map[string]struct{} {
	return map[string]struct{}{
		headerKey(prefix, headerMessageID):         {},
		headerKey(prefix, headerMessageKind):       {},
		headerKey(prefix, headerMessageOccurredAt): {},
		headerKey(prefix, headerRetryAttempt):      {},
		headerKey(prefix, headerOriginalTopic):     {},
		headerKey(prefix, headerOriginalPartition): {},
		headerKey(prefix, headerOriginalOffset):    {},
		headerKey(prefix, headerFailedStage):       {},
		headerKey(prefix, headerFirstError):        {},
		headerKey(prefix, headerLastError):         {},
	}
}

func cloneRecord(record *kgo.Record) *kgo.Record {
	copied := *record
	copied.Key = append([]byte(nil), record.Key...)
	copied.Value = append([]byte(nil), record.Value...)
	copied.Headers = append([]kgo.RecordHeader(nil), record.Headers...)
	for i := range copied.Headers {
		copied.Headers[i].Value = append([]byte(nil), copied.Headers[i].Value...)
	}
	return &copied
}

func addFailureHeaders(record *kgo.Record, cfg options, ctx ErrorContext, attempt int) {
	record.Headers = appendHeader(record.Headers, headerKey(cfg.headerPrefix, headerRetryAttempt), strconv.Itoa(attempt))
	record.Headers = appendHeader(record.Headers, headerKey(cfg.headerPrefix, headerOriginalTopic), ctx.Record.Topic)
	record.Headers = appendHeader(record.Headers, headerKey(cfg.headerPrefix, headerOriginalPartition), strconv.Itoa(int(ctx.Record.Partition)))
	record.Headers = appendHeader(record.Headers, headerKey(cfg.headerPrefix, headerOriginalOffset), strconv.FormatInt(ctx.Record.Offset, 10))
	record.Headers = appendHeader(record.Headers, headerKey(cfg.headerPrefix, headerFailedStage), string(ctx.Stage))
	if headerValue(record.Headers, headerKey(cfg.headerPrefix, headerFirstError)) == "" {
		record.Headers = appendHeader(record.Headers, headerKey(cfg.headerPrefix, headerFirstError), ctx.Err.Error())
	}
	record.Headers = appendHeader(record.Headers, headerKey(cfg.headerPrefix, headerLastError), ctx.Err.Error())
}
```

- [ ] **Step 4: Run record mapping tests**

Run:

```bash
cd message/kafka && go test ./... -run 'Test(MessageToRecord|RecordToMessage)' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit record mapping**

```bash
git add message/kafka
git commit -m "feat: map kafka records to integration messages"
```

## Task 3: Publisher

**Files:**
- Create: `message/kafka/publisher.go`
- Test: `message/kafka/publisher_test.go`

- [ ] **Step 1: Write failing publisher tests**

Create `message/kafka/publisher_test.go`:

```go
package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProducerClient struct {
	records []*kgo.Record
	err     error
}

func (f *fakeProducerClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.records = append(f.records, records...)
	results := make(kgo.ProduceResults, 0, len(records))
	for _, record := range records {
		results = append(results, kgo.ProduceResult{Record: record, Err: f.err})
	}
	return results
}

// Intent: publishing should hand a correctly mapped Kafka record to the franz-go producer boundary.
func TestPublisherPublishProducesRecord(t *testing.T) {
	client := &fakeProducerClient{}
	publisher := newPublisher(client, defaultOptions())
	msg := mustMessage(t, "order.payment.v1.OrderPaid")

	err := publisher.Publish(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, client.records, 1)
	require.Equal(t, "order.payment.v1.OrderPaid", client.records[0].Topic)
	require.Equal(t, []byte("order-1"), client.records[0].Key)
}

// Intent: producer failures must be visible to callers so direct publish does not pretend the broker accepted the message.
func TestPublisherPublishReturnsProduceError(t *testing.T) {
	produceErr := errors.New("produce failed")
	client := &fakeProducerClient{err: produceErr}
	publisher := newPublisher(client, defaultOptions())

	err := publisher.Publish(context.Background(), mustMessage(t, "order.payment.v1.OrderPaid"))

	require.ErrorIs(t, err, produceErr)
}

// Intent: the exported constructor should reject nil clients before returning a publisher that can panic at runtime.
func TestNewPublisherRejectsNilClient(t *testing.T) {
	publisher, ok := NewPublisher(nil).(interface{ Publish(context.Context, message.Message) error })

	require.True(t, ok)
	err := publisher.Publish(context.Background(), mustMessage(t, "order.payment.v1.OrderPaid"))
	require.ErrorIs(t, err, ErrNilClient)
}
```

- [ ] **Step 2: Run publisher tests to verify failure**

Run:

```bash
cd message/kafka && go test ./... -run 'TestPublisher|TestNewPublisher' -count=1
```

Expected: FAIL because publisher implementation does not exist.

- [ ] **Step 3: Implement publisher**

Create `message/kafka/publisher.go`:

```go
package kafka

import (
	"context"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type producerClient interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

type publisher struct {
	client producerClient
	cfg    options
}

func NewPublisher(client *kgo.Client, opts ...Option) message.Publisher {
	return newPublisher(client, applyOptions(opts))
}

func newPublisher(client producerClient, cfg options) message.Publisher {
	return &publisher{client: client, cfg: cfg}
}

func (p *publisher) Publish(ctx context.Context, msg message.Message) error {
	if p.client == nil {
		return ErrNilClient
	}
	record, err := messageToRecord(msg, p.cfg)
	if err != nil {
		return err
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

func NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(opts...)
}
```

- [ ] **Step 4: Run publisher tests**

Run:

```bash
cd message/kafka && go test ./... -run 'TestPublisher|TestNewPublisher' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit publisher**

```bash
git add message/kafka
git commit -m "feat: add kafka message publisher"
```

## Task 4: Retry And DLQ Policy

**Files:**
- Create: `message/kafka/retry.go`
- Test: `message/kafka/retry_test.go`

- [ ] **Step 1: Write failing retry/DLQ tests**

Create `message/kafka/retry_test.go`:

```go
package kafka

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Intent: handler failures below max attempts should move to retry so temporary handler-path failures do not immediately become dead letters.
func TestFailureActionRetriesHandleErrorBelowMaxAttempts(t *testing.T) {
	cfg := defaultOptions()
	record := &kgo.Record{Topic: "orders", Headers: nil}
	ctx := ErrorContext{Stage: StageHandle, Record: record, Err: errors.New("handler unavailable")}

	action := failureAction(ctx, cfg)

	require.Equal(t, actionRetry, action.kind)
	require.Equal(t, 1, action.attempt)
	require.Equal(t, "orders.retry", action.topic)
}

// Intent: repeated handler failures should eventually move to DLQ so a poison message does not block the source partition forever.
func TestFailureActionDLQAtMaxAttempts(t *testing.T) {
	cfg := defaultOptions()
	record := &kgo.Record{
		Topic: "orders",
		Headers: []kgo.RecordHeader{
			{Key: "jimu-retry-attempt", Value: []byte("2")},
		},
	}
	ctx := ErrorContext{Stage: StageHandle, Record: record, Err: errors.New("handler unavailable")}

	action := failureAction(ctx, cfg)

	require.Equal(t, actionDLQ, action.kind)
	require.Equal(t, 3, action.attempt)
	require.Equal(t, "orders.dlq", action.topic)
}

// Intent: decode failures should go directly to DLQ because retrying the same invalid bytes cannot create a valid protobuf payload.
func TestFailureActionDLQDecodeError(t *testing.T) {
	cfg := defaultOptions()
	record := &kgo.Record{Topic: "orders"}
	ctx := ErrorContext{Stage: StageDecode, Record: record, Err: errors.New("bad protobuf")}

	action := failureAction(ctx, cfg)

	require.Equal(t, actionDLQ, action.kind)
	require.Equal(t, "orders.dlq", action.topic)
}

// Intent: retry records should preserve raw source bytes while changing only the destination topic and failure metadata.
func TestBuildFailureRecordPreservesRawRecord(t *testing.T) {
	source := &kgo.Record{
		Topic:     "orders",
		Partition: 1,
		Offset:    42,
		Key:       []byte("order-1"),
		Value:     []byte("raw"),
	}
	ctx := ErrorContext{Stage: StageHandle, Record: source, Err: errors.New("temporary")}
	action := failureDecision{kind: actionRetry, topic: "orders.retry", attempt: 1}

	record := buildFailureRecord(ctx, action, defaultOptions())

	require.Equal(t, "orders.retry", record.Topic)
	require.Equal(t, []byte("order-1"), record.Key)
	require.Equal(t, []byte("raw"), record.Value)
	require.Equal(t, "1", headerValue(record.Headers, "jimu-retry-attempt"))
	require.Equal(t, "orders", headerValue(record.Headers, "jimu-original-topic"))
	require.Equal(t, "1", headerValue(record.Headers, "jimu-original-partition"))
	require.Equal(t, "42", headerValue(record.Headers, "jimu-original-offset"))
	require.Equal(t, "handle", headerValue(record.Headers, "jimu-failed-stage"))
}
```

- [ ] **Step 2: Run retry/DLQ tests to verify failure**

Run:

```bash
cd message/kafka && go test ./... -run 'TestFailureAction|TestBuildFailureRecord' -count=1
```

Expected: FAIL because retry helpers do not exist.

- [ ] **Step 3: Implement retry/DLQ helpers**

Create `message/kafka/retry.go`:

```go
package kafka

type failureActionKind int

const (
	actionNone failureActionKind = iota
	actionRetry
	actionDLQ
)

type failureDecision struct {
	kind    failureActionKind
	topic   string
	attempt int
	err     error
}

func failureAction(ctx ErrorContext, cfg options) failureDecision {
	current := retryAttempt(ctx.Record.Headers, cfg.headerPrefix)
	next := current + 1
	if cfg.retryPolicy.Retryable(ctx) && next < cfg.retryPolicy.MaxAttempts {
		topic, err := cfg.retryTopicResolver(ctx)
		return failureDecision{kind: actionRetry, topic: topic, attempt: next, err: err}
	}
	topic, err := cfg.dlqTopicResolver(ctx)
	return failureDecision{kind: actionDLQ, topic: topic, attempt: next, err: err}
}

func buildFailureRecord(ctx ErrorContext, decision failureDecision, cfg options) *kgo.Record {
	record := cloneRecord(ctx.Record)
	record.Topic = decision.topic
	addFailureHeaders(record, cfg, ctx, decision.attempt)
	return record
}
```

Add this import to `message/kafka/retry.go`:

```go
import "github.com/twmb/franz-go/pkg/kgo"
```

- [ ] **Step 4: Run retry/DLQ tests**

Run:

```bash
cd message/kafka && go test ./... -run 'TestFailureAction|TestBuildFailureRecord' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit retry/DLQ helpers**

```bash
git add message/kafka
git commit -m "feat: add kafka retry and dlq policy"
```

## Task 5: Consumer Record Processing

**Files:**
- Create: `message/kafka/consumer.go`
- Test: `message/kafka/consumer_test.go`

- [ ] **Step 1: Write failing consumer processing tests**

Create `message/kafka/consumer_test.go`:

```go
package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type fakeConsumerClient struct {
	fakeProducerClient
	committed []*kgo.Record
	commitErr error
}

func (f *fakeConsumerClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.committed = append(f.committed, records...)
	return f.commitErr
}

func (f *fakeConsumerClient) PollFetches(context.Context) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Partitions: []kgo.FetchPartition{{Err: context.Canceled}}}}}}
}

func (f *fakeConsumerClient) Close() {}

func encodedRecord(t *testing.T, kind message.Kind) *kgo.Record {
	t.Helper()
	value, err := proto.Marshal(&testdata.TestModel{Id: 7, Name: "paid"})
	require.NoError(t, err)
	return &kgo.Record{
		Topic: "orders",
		Key:   []byte("order-1"),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "jimu-message-id", Value: []byte("msg-1")},
			{Key: "jimu-message-kind", Value: []byte(kind)},
		},
	}
}

type testHandler struct {
	kinds []message.Kind
	err   error
	calls int
}

func (h *testHandler) Listening() []message.Kind { return h.kinds }

func (h *testHandler) Handle(context.Context, message.Message) error {
	h.calls++
	return h.err
}

func payloadResolver(message.Kind) (proto.Message, error) {
	return &testdata.TestModel{}, nil
}

// Intent: a successfully handled message should commit the source offset and avoid retry/DLQ publication.
func TestConsumerProcessRecordCommitsAfterSuccess(t *testing.T) {
	client := &fakeConsumerClient{}
	consumer := newConsumer(client, applyOptions([]Option{WithPayloadResolver(payloadResolver)}))
	handler := &testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}
	require.NoError(t, consumer.Subscribe(handler))
	record := encodedRecord(t, "order.payment.v1.OrderPaid")

	err := consumer.processRecord(context.Background(), record)

	require.NoError(t, err)
	require.Equal(t, 1, handler.calls)
	require.Equal(t, []*kgo.Record{record}, client.committed)
	require.Empty(t, client.records)
}

// Intent: decode failures should be handed to DLQ and commit only after DLQ publication succeeds.
func TestConsumerProcessRecordDLQDecodeFailure(t *testing.T) {
	client := &fakeConsumerClient{}
	consumer := newConsumer(client, defaultOptions())
	record := &kgo.Record{Topic: "orders", Value: []byte("bad")}

	err := consumer.processRecord(context.Background(), record)

	require.NoError(t, err)
	require.Len(t, client.records, 1)
	require.Equal(t, "orders.dlq", client.records[0].Topic)
	require.Equal(t, []*kgo.Record{record}, client.committed)
}

// Intent: handler errors below max attempts should be handed to retry and commit only after retry publication succeeds.
func TestConsumerProcessRecordRetriesHandlerError(t *testing.T) {
	client := &fakeConsumerClient{}
	consumer := newConsumer(client, applyOptions([]Option{WithPayloadResolver(payloadResolver)}))
	handler := &testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}, err: errors.New("temporary")}
	require.NoError(t, consumer.Subscribe(handler))
	record := encodedRecord(t, "order.payment.v1.OrderPaid")

	err := consumer.processRecord(context.Background(), record)

	require.NoError(t, err)
	require.Len(t, client.records, 1)
	require.Equal(t, "orders.retry", client.records[0].Topic)
	require.Equal(t, []*kgo.Record{record}, client.committed)
}

// Intent: retry publish failure must leave the source offset uncommitted so Kafka can redeliver.
func TestConsumerProcessRecordDoesNotCommitWhenRetryPublishFails(t *testing.T) {
	client := &fakeConsumerClient{}
	client.err = errors.New("retry publish failed")
	consumer := newConsumer(client, applyOptions([]Option{WithPayloadResolver(payloadResolver)}))
	handler := &testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}, err: errors.New("temporary")}
	require.NoError(t, consumer.Subscribe(handler))

	err := consumer.processRecord(context.Background(), encodedRecord(t, "order.payment.v1.OrderPaid"))

	require.ErrorContains(t, err, "retry publish failed")
	require.Empty(t, client.committed)
}

// Intent: commit failures should be returned so the caller can observe uncertain consumer progress.
func TestConsumerProcessRecordReturnsCommitError(t *testing.T) {
	client := &fakeConsumerClient{commitErr: errors.New("commit failed")}
	consumer := newConsumer(client, applyOptions([]Option{WithPayloadResolver(payloadResolver)}))
	handler := &testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}
	require.NoError(t, consumer.Subscribe(handler))

	err := consumer.processRecord(context.Background(), encodedRecord(t, "order.payment.v1.OrderPaid"))

	require.ErrorContains(t, err, "commit failed")
}
```

- [ ] **Step 2: Run consumer processing tests to verify failure**

Run:

```bash
cd message/kafka && go test ./... -run 'TestConsumerProcessRecord' -count=1
```

Expected: FAIL because consumer implementation does not exist.

- [ ] **Step 3: Implement consumer processing**

Create `message/kafka/consumer.go`:

```go
package kafka

import (
	"context"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerClient interface {
	producerClient
	CommitRecords(context.Context, ...*kgo.Record) error
	Close()
}

type Consumer struct {
	client consumerClient
	cfg    options
	router *message.Router
}

func NewConsumer(client *kgo.Client, opts ...Option) *Consumer {
	return newConsumer(client, applyOptions(opts))
}

func newConsumer(client consumerClient, cfg options) *Consumer {
	return &Consumer{
		client: client,
		cfg:    cfg,
		router: message.NewRouter(),
	}
}

func (c *Consumer) Subscribe(handler message.Handler) error {
	return c.router.Subscribe(handler)
}

func (c *Consumer) Close() {
	if c.client != nil && c.cfg.closeClient {
		c.client.Close()
	}
}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) error {
	if c.client == nil {
		return ErrNilClient
	}
	msg, err := recordToMessage(record, c.cfg)
	if err != nil {
		return c.handleFailure(ctx, ErrorContext{Stage: StageDecode, Record: record, Err: err})
	}
	if err = c.router.Handle(ctx, msg); err != nil {
		stage := StageHandle
		if errors.Is(err, message.ErrUnhandledKind) {
			stage = StageUnhandled
		}
		return c.handleFailure(ctx, ErrorContext{Stage: stage, Record: record, Message: msg, Err: err})
	}
	return c.commit(ctx, record)
}

func (c *Consumer) handleFailure(ctx context.Context, failure ErrorContext) error {
	decision := failureAction(failure, c.cfg)
	if decision.err != nil {
		return c.cfg.errorHandler(ctx, ErrorContext{Stage: failure.Stage, Record: failure.Record, Message: failure.Message, Err: decision.err})
	}
	failureRecord := buildFailureRecord(failure, decision, c.cfg)
	if err := c.client.ProduceSync(ctx, failureRecord).FirstErr(); err != nil {
		stage := StageRetryPublish
		if decision.kind == actionDLQ {
			stage = StageDLQPublish
		}
		return c.cfg.errorHandler(ctx, ErrorContext{Stage: stage, Record: failure.Record, Message: failure.Message, Err: err})
	}
	return c.commit(ctx, failure.Record)
}

func (c *Consumer) commit(ctx context.Context, record *kgo.Record) error {
	if err := c.client.CommitRecords(ctx, record); err != nil {
		return c.cfg.errorHandler(ctx, ErrorContext{Stage: StageCommit, Record: record, Err: err})
	}
	return nil
}
```

Add this import to `message/kafka/consumer.go`:

```go
import "errors"
```

- [ ] **Step 4: Run consumer processing tests**

Run:

```bash
cd message/kafka && go test ./... -run 'TestConsumerProcessRecord' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit consumer processing**

```bash
git add message/kafka
git commit -m "feat: process kafka message records"
```

## Task 6: Poll Loop And Fetch Errors

**Files:**
- Modify: `message/kafka/consumer.go`
- Test: `message/kafka/run_test.go`

- [ ] **Step 1: Write failing run loop tests**

Create `message/kafka/run_test.go`:

```go
package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type fakeRunClient struct {
	fakeConsumerClient
	fetches []kgo.Fetches
}

func (f *fakeRunClient) PollFetches(context.Context) kgo.Fetches {
	if len(f.fetches) == 0 {
		return kgo.Fetches{{Topics: []kgo.FetchTopic{{Partitions: []kgo.FetchPartition{{Err: context.Canceled}}}}}}
	}
	next := f.fetches[0]
	f.fetches = f.fetches[1:]
	return next
}

func (f *fakeRunClient) Close() {}

// Intent: Run should process fetched records through the same route/retry/commit path as direct record processing.
func TestConsumerRunProcessesFetchedRecords(t *testing.T) {
	client := &fakeRunClient{}
	record := encodedRecord(t, "order.payment.v1.OrderPaid")
	client.fetches = []kgo.Fetches{
		{{Topics: []kgo.FetchTopic{{Topic: "orders", Partitions: []kgo.FetchPartition{{Records: []*kgo.Record{record}}}}}}},
		{{Topics: []kgo.FetchTopic{{Partitions: []kgo.FetchPartition{{Err: context.Canceled}}}}}},
	}
	consumer := newConsumer(client, applyOptions([]Option{
		WithPayloadResolver(func(message.Kind) (proto.Message, error) { return payloadResolver("") }),
		WithErrorHandler(func(_ context.Context, ctx ErrorContext) error {
			if errors.Is(ctx.Err, context.Canceled) {
				return ctx.Err
			}
			return ctx.Err
		}),
	}))
	require.NoError(t, consumer.Subscribe(&testHandler{kinds: []message.Kind{"order.payment.v1.OrderPaid"}}))

	err := consumer.Run(context.Background())

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []*kgo.Record{record}, client.committed)
}

// Intent: poll errors must be surfaced through ErrorHandler so caller-owned runtime policy can stop or continue.
func TestConsumerRunSurfacesPollError(t *testing.T) {
	pollErr := errors.New("poll failed")
	client := &fakeRunClient{
		fetches: []kgo.Fetches{
			{{Topics: []kgo.FetchTopic{{Partitions: []kgo.FetchPartition{{Err: pollErr}}}}}},
		},
	}
	var stage Stage
	consumer := newConsumer(client, applyOptions([]Option{
		WithErrorHandler(func(_ context.Context, ctx ErrorContext) error {
			stage = ctx.Stage
			return ctx.Err
		}),
	}))

	err := consumer.Run(context.Background())

	require.ErrorIs(t, err, pollErr)
	require.Equal(t, StagePoll, stage)
}
```

- [ ] **Step 2: Run run loop tests to verify failure**

Run:

```bash
cd message/kafka && go test ./... -run 'TestConsumerRun' -count=1
```

Expected: FAIL because `Run` is not implemented.

- [ ] **Step 3: Implement run loop**

Update `message/kafka/consumer.go`:

```go
type pollClient interface {
	consumerClient
	PollFetches(context.Context) kgo.Fetches
}
```

Change the `Consumer.client` type to `pollClient`, and update `newConsumer` to accept `pollClient`.

Add:

```go
func (c *Consumer) Run(ctx context.Context) error {
	if c.client == nil {
		return ErrNilClient
	}
	for {
		fetches := c.client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return c.cfg.errorHandler(ctx, ErrorContext{Stage: StagePoll, Err: err})
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			if err := c.processRecord(ctx, iter.Next()); err != nil {
				return err
			}
		}
	}
}
```

- [ ] **Step 4: Run run loop tests**

Run:

```bash
cd message/kafka && go test ./... -run 'TestConsumerRun' -count=1
```

Expected: PASS.

- [ ] **Step 5: Run all provider tests**

Run:

```bash
cd message/kafka && go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit run loop**

```bash
git add message/kafka
git commit -m "feat: run kafka message consumer loop"
```

## Task 7: README, Release Matrix, And Verification

**Files:**
- Create: `message/kafka/README.md`
- Modify: `.github/workflows/ci.yml`
- Test: repository verification commands

- [ ] **Step 1: Write README**

Create `message/kafka/README.md`:

````markdown
# Kafka Message

Kafka provider for `github.com/go-jimu/components/ddd/message`.

## Install

```bash
go get github.com/go-jimu/contrib/message/kafka@latest
```

## Publisher

```go
client, err := kafka.NewClient(
	kgo.SeedBrokers("localhost:9092"),
	kgo.ProducerBatchCompression(kgo.SnappyCompression()),
)
if err != nil {
	panic(err)
}

publisher := kafka.NewPublisher(client)
msg, err := message.New(
	"order.payment.v1.OrderPaid",
	&orderv1.OrderPaid{OrderId: "order-1"},
	message.WithKey("order-1"),
)
if err != nil {
	panic(err)
}
if err = publisher.Publish(context.Background(), msg); err != nil {
	panic(err)
}
```

`Message.Key()` maps to the Kafka record key. With normal hash partitioning,
records with the same key in the same topic are routed to the same partition,
which preserves per-partition ordering for that key.

## Consumer

```go
client, err := kafka.NewClient(
	kgo.SeedBrokers("localhost:9092"),
	kgo.ConsumerGroup("billing-service"),
	kgo.ConsumeTopics("order.payment.v1.OrderPaid", "order.payment.v1.OrderPaid.retry"),
	kgo.DisableAutoCommit(),
)
if err != nil {
	panic(err)
}

consumer := kafka.NewConsumer(
	client,
	kafka.WithPayloadResolver(func(kind message.Kind) (proto.Message, error) {
		switch kind {
		case "order.payment.v1.OrderPaid":
			return &orderv1.OrderPaid{}, nil
		default:
			return nil, fmt.Errorf("unknown message kind %s", kind)
		}
	}),
)
if err = consumer.Subscribe(orderPaidHandler{}); err != nil {
	panic(err)
}
if err = consumer.Run(context.Background()); err != nil {
	panic(err)
}
```

## Retry And DLQ

Default retry topic is `<source-topic>.retry`.
Default DLQ topic is `<source-topic>.dlq`.
Default max attempts is `3`.

The consumer commits the source offset only after one of these succeeds:

- handler returns nil
- retry record is published
- DLQ record is published

If retry or DLQ publication fails, the source offset is not committed and Kafka
can redeliver the source record.

## Contract Stability

`github.com/go-jimu/components/ddd/message` is experimental. This provider is
also a validation of that API. If real Kafka behavior requires a different core
contract, update `components/ddd/message` before adding adapter workarounds.
````

- [ ] **Step 2: Update release matrix**

In `.github/workflows/ci.yml`, add `message/kafka` to the release package matrix:

```yaml
matrix:
  package: [config/etcd, config/nacos, config/kubernetes, config/apollo, logger/zap, message/kafka]
```

- [ ] **Step 3: Run provider tests**

Run:

```bash
cd message/kafka && go test -race -covermode=atomic -v -coverprofile=coverage.txt ./...
```

Expected: PASS.

- [ ] **Step 4: Run repository tests**

Run:

```bash
make test
```

Expected: PASS.

- [ ] **Step 5: Commit docs and release update**

```bash
git add .github/workflows/ci.yml message/kafka/README.md
git commit -m "docs: document kafka message provider"
```

## Task 8: Project Knowledge Update

**Files:**
- Modify: `docs/project-knowledge/index.md`
- Modify: `docs/project-knowledge/architecture.md`
- Modify: `docs/project-knowledge/features.md`
- Modify: `docs/project-knowledge/tech-stack.md`
- Modify: `docs/project-knowledge/conventions.md`

- [ ] **Step 1: Run memory update**

Run:

```bash
node /home/xuhao/.codex/plugins/cache/skill-workshop-codex/superpowers-memory/1.12.10/hooks/codex-runtime.js lock superpowers-memory:update
```

Then invoke `superpowers-memory:update` and update project knowledge so it reflects:

- `message/kafka` provider module.
- Kafka/franz-go dependency.
- Retry/DLQ defaults.
- `components v0.8.0` for the new provider.
- Release matrix now includes `message/kafka`.

- [ ] **Step 2: Verify memory files**

Run:

```bash
node /home/xuhao/.codex/plugins/cache/skill-workshop-codex/superpowers-memory/1.12.10/hooks/codex-runtime.js verify
```

Expected: JSON output has `"ok": true`.

- [ ] **Step 3: Release memory lock**

Run:

```bash
node /home/xuhao/.codex/plugins/cache/skill-workshop-codex/superpowers-memory/1.12.10/hooks/codex-runtime.js unlock
```

Expected: JSON output has `"ok": true`.

- [ ] **Step 4: Commit project knowledge**

```bash
git add docs/project-knowledge
git commit -m "docs: update project knowledge for kafka message provider"
```

## Task 9: Final Verification

**Files:**
- No new files.

- [ ] **Step 1: Check working tree**

Run:

```bash
git status --short
```

Expected: no unexpected modified files. Existing user-owned untracked files such as `.codex/` may remain untracked.

- [ ] **Step 2: Run full verification**

Run:

```bash
make test
```

Expected: PASS.

- [ ] **Step 3: Inspect recent commits**

Run:

```bash
git log --oneline -8
```

Expected: recent commits include scaffold, record mapping, publisher, retry/DLQ, consumer, docs, and project knowledge commits.

## Self-Review

Spec coverage:

- Module path `message/kafka`: Task 1.
- Components `v0.8.0`: Task 1.
- franz-go `v1.21.1`: Task 1.
- Publisher: Task 3.
- Consumer and Subscriber: Tasks 5 and 6.
- Message/record mapping: Task 2.
- Payload resolver: Task 2.
- Retry and DLQ: Tasks 4 and 5.
- Offset commit behavior: Tasks 5 and 6.
- README examples: Task 7.
- Release matrix and workspace: Tasks 1 and 7.
- Project knowledge update: Task 8.

Placeholder scan:

- No TBD/TODO placeholders.
- Every task has exact files, commands, and expected outcomes.
- Each test has an intent comment and a real boundary label in the test list.

Type consistency:

- Public constructors match the spec: `NewPublisher`, `NewConsumer`, `NewClient`.
- Consumer methods match the spec: `Subscribe`, `Run`, `Close`.
- Stage names match the spec: `poll`, `decode`, `unhandled`, `handle`, `retry_publish`, `dlq_publish`, `commit`.
- Retry/DLQ headers match the spec.
