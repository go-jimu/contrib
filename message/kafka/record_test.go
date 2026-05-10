package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Intent: publishing a message should preserve the full envelope Kafka consumers need to route and trace it.
func TestMessageToRecordMapsMessageEnvelope(t *testing.T) {
	cfg := defaultConfig()
	msg := newTestMessage(t, "order.payment.v1.OrderPaid")

	record, err := messageToRecord(msg, cfg)

	if err != nil {
		t.Fatalf("messageToRecord returned error: %v", err)
	}
	if record.Topic != "order.payment.v1.OrderPaid" {
		t.Fatalf("topic = %q, want %q", record.Topic, "order.payment.v1.OrderPaid")
	}
	if string(record.Key) != "order-1" {
		t.Fatalf("key = %q, want order-1", record.Key)
	}
	if !record.Timestamp.Equal(msg.OccurredAt()) {
		t.Fatalf("timestamp = %v, want %v", record.Timestamp, msg.OccurredAt())
	}
	if len(record.Value) == 0 {
		t.Fatal("value is empty, want protobuf bytes")
	}
	if got := headerValue(record.Headers, cfg.headerNames.MessageID); got != "msg-1" {
		t.Fatalf("message id header = %q, want msg-1", got)
	}
	if got := headerValue(record.Headers, cfg.headerNames.MessageKind); got != "order.payment.v1.OrderPaid" {
		t.Fatalf("message kind header = %q, want order.payment.v1.OrderPaid", got)
	}
	if got := headerValue(record.Headers, cfg.headerNames.OccurredAt); got != msg.OccurredAt().Format(time.RFC3339Nano) {
		t.Fatalf("occurred at header = %q, want %q", got, msg.OccurredAt().Format(time.RFC3339Nano))
	}
	if got := headerValue(record.Headers, "trace_id"); got != "trace-1" {
		t.Fatalf("trace header = %q, want trace-1", got)
	}
}

// Intent: caller-supplied headers must not be able to forge the reserved message envelope written to Kafka.
func TestMessageToRecordReservedHeadersWin(t *testing.T) {
	cfg := defaultConfig()
	msg, err := message.New(
		"order.payment.v1.OrderPaid",
		testPayload(t),
		message.WithID("msg-real"),
		message.WithKey("order-1"),
		message.WithOccurredAt(time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)),
		message.WithHeader(cfg.headerNames.MessageID, "msg-forged"),
		message.WithHeader(cfg.headerNames.MessageKind, "forged.Kind"),
		message.WithHeader(cfg.headerNames.OccurredAt, "2020-01-01T00:00:00Z"),
		message.WithHeader(headerRetryAttempt, "99"),
	)
	if err != nil {
		t.Fatalf("message.New returned error: %v", err)
	}

	record, err := messageToRecord(msg, cfg)

	if err != nil {
		t.Fatalf("messageToRecord returned error: %v", err)
	}
	if got := headerValue(record.Headers, cfg.headerNames.MessageID); got != "msg-real" {
		t.Fatalf("message id header = %q, want msg-real", got)
	}
	if got := headerValue(record.Headers, cfg.headerNames.MessageKind); got != "order.payment.v1.OrderPaid" {
		t.Fatalf("message kind header = %q, want order.payment.v1.OrderPaid", got)
	}
	if got := headerValue(record.Headers, cfg.headerNames.OccurredAt); got != msg.OccurredAt().Format(time.RFC3339Nano) {
		t.Fatalf("occurred at header = %q, want %q", got, msg.OccurredAt().Format(time.RFC3339Nano))
	}
	if got := headerValue(record.Headers, headerRetryAttempt); got != "" {
		t.Fatalf("retry attempt header = %q, want empty", got)
	}
}

// Intent: a message without protobuf payload should fail clearly instead of producing an undecodable Kafka record.
func TestMessageToRecordRejectsNilPayload(t *testing.T) {
	cfg := defaultConfig()

	record, err := messageToRecord(message.Message{}, cfg)

	if !errors.Is(err, ErrNilPayload) {
		t.Fatalf("messageToRecord error = %v, want %v", err, ErrNilPayload)
	}
	if record != nil {
		t.Fatalf("record = %#v, want nil", record)
	}
}

// Intent: custom envelope header names should define the reserved set for both record mapping directions.
func TestRecordMappingUsesCustomHeaderNames(t *testing.T) {
	cfg := defaultConfig()
	defaultHeaders := cfg.headerNames
	customHeaders := HeaderNames{
		MessageID:   "x-message-id",
		MessageKind: "x-message-kind",
		OccurredAt:  "x-occurred-at",
	}
	WithHeaderNames(customHeaders)(&cfg)
	cfg.payloadResolver = message.PayloadResolverFunc(func(kind message.Kind) (proto.Message, error) {
		if kind != "order.payment.v1.OrderPaid" {
			t.Fatalf("payload resolver kind = %q, want order.payment.v1.OrderPaid", kind)
		}
		return &testdata.TestModel{}, nil
	})
	occurredAt := time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC)
	msg, err := message.New(
		"order.payment.v1.OrderPaid",
		testPayload(t),
		message.WithID("msg-custom"),
		message.WithKey("order-custom"),
		message.WithOccurredAt(occurredAt),
		message.WithHeader(customHeaders.MessageID, "forged-id"),
		message.WithHeader(customHeaders.MessageKind, "forged.Kind"),
		message.WithHeader(customHeaders.OccurredAt, "2020-01-01T00:00:00Z"),
		message.WithHeader(defaultHeaders.MessageID, "legacy-id"),
		message.WithHeader(defaultHeaders.MessageKind, "legacy-kind"),
		message.WithHeader(defaultHeaders.OccurredAt, "legacy-time"),
	)
	if err != nil {
		t.Fatalf("message.New returned error: %v", err)
	}

	record, err := messageToRecord(msg, cfg)

	if err != nil {
		t.Fatalf("messageToRecord returned error: %v", err)
	}
	if got := headerValue(record.Headers, customHeaders.MessageID); got != "msg-custom" {
		t.Fatalf("custom message id header = %q, want msg-custom", got)
	}
	if got := headerValue(record.Headers, customHeaders.MessageKind); got != "order.payment.v1.OrderPaid" {
		t.Fatalf("custom message kind header = %q, want order.payment.v1.OrderPaid", got)
	}
	if got := headerValue(record.Headers, customHeaders.OccurredAt); got != occurredAt.Format(time.RFC3339Nano) {
		t.Fatalf("custom occurred at header = %q, want %q", got, occurredAt.Format(time.RFC3339Nano))
	}
	if got := headerValue(record.Headers, defaultHeaders.MessageID); got != "legacy-id" {
		t.Fatalf("default message id header = %q, want legacy-id", got)
	}
	if got := headerValue(record.Headers, defaultHeaders.MessageKind); got != "legacy-kind" {
		t.Fatalf("default message kind header = %q, want legacy-kind", got)
	}
	if got := headerValue(record.Headers, defaultHeaders.OccurredAt); got != "legacy-time" {
		t.Fatalf("default occurred at header = %q, want legacy-time", got)
	}

	decoded, err := recordToMessage(record, cfg)

	if err != nil {
		t.Fatalf("recordToMessage returned error: %v", err)
	}
	if decoded.ID() != "msg-custom" {
		t.Fatalf("decoded id = %q, want msg-custom", decoded.ID())
	}
	if decoded.Kind() != "order.payment.v1.OrderPaid" {
		t.Fatalf("decoded kind = %q, want order.payment.v1.OrderPaid", decoded.Kind())
	}
	if decoded.Key() != "order-custom" {
		t.Fatalf("decoded key = %q, want order-custom", decoded.Key())
	}
	if !decoded.OccurredAt().Equal(occurredAt) {
		t.Fatalf("decoded occurred at = %v, want %v", decoded.OccurredAt(), occurredAt)
	}
	if got := decoded.Headers()[customHeaders.MessageID]; got != "" {
		t.Fatalf("decoded custom reserved message id header = %q, want empty", got)
	}
	if got := decoded.Headers()[defaultHeaders.MessageID]; got != "legacy-id" {
		t.Fatalf("decoded default message id header = %q, want legacy-id", got)
	}
	if got := decoded.Headers()[defaultHeaders.MessageKind]; got != "legacy-kind" {
		t.Fatalf("decoded default message kind header = %q, want legacy-kind", got)
	}
	if got := decoded.Headers()[defaultHeaders.OccurredAt]; got != "legacy-time" {
		t.Fatalf("decoded default occurred at header = %q, want legacy-time", got)
	}
	if !proto.Equal(decoded.Payload(), testPayload(t)) {
		t.Fatalf("decoded payload = %#v, want %#v", decoded.Payload(), testPayload(t))
	}
}

// Intent: consuming a Kafka record should reconstruct the message envelope and protobuf payload for handlers.
func TestRecordToMessageReconstructsMessage(t *testing.T) {
	cfg := defaultConfig()
	cfg.payloadResolver = message.PayloadResolverFunc(func(kind message.Kind) (proto.Message, error) {
		if kind != "order.payment.v1.OrderPaid" {
			t.Fatalf("payload resolver kind = %q, want order.payment.v1.OrderPaid", kind)
		}
		return &testdata.TestModel{}, nil
	})
	occurredAt := time.Date(2026, 5, 10, 12, 30, 0, 123, time.UTC)
	value, err := cfg.codec.Marshal(testPayload(t))
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	record := &kgo.Record{
		Key:       []byte("order-1"),
		Value:     value,
		Timestamp: occurredAt.Add(time.Hour),
		Headers: []kgo.RecordHeader{
			{Key: cfg.headerNames.MessageID, Value: []byte("msg-1")},
			{Key: cfg.headerNames.MessageKind, Value: []byte("order.payment.v1.OrderPaid")},
			{Key: cfg.headerNames.OccurredAt, Value: []byte(occurredAt.Format(time.RFC3339Nano))},
			{Key: "trace_id", Value: []byte("trace-1")},
			{Key: headerRetryAttempt, Value: []byte("2")},
		},
	}

	msg, err := recordToMessage(record, cfg)

	if err != nil {
		t.Fatalf("recordToMessage returned error: %v", err)
	}
	if msg.ID() != "msg-1" {
		t.Fatalf("id = %q, want msg-1", msg.ID())
	}
	if msg.Kind() != "order.payment.v1.OrderPaid" {
		t.Fatalf("kind = %q, want order.payment.v1.OrderPaid", msg.Kind())
	}
	if msg.Key() != "order-1" {
		t.Fatalf("key = %q, want order-1", msg.Key())
	}
	if !msg.OccurredAt().Equal(occurredAt) {
		t.Fatalf("occurred at = %v, want %v", msg.OccurredAt(), occurredAt)
	}
	if got := msg.Headers()["trace_id"]; got != "trace-1" {
		t.Fatalf("trace header = %q, want trace-1", got)
	}
	if got := msg.Headers()[headerRetryAttempt]; got != "" {
		t.Fatalf("reserved retry header = %q, want empty", got)
	}
	if !proto.Equal(msg.Payload(), testPayload(t)) {
		t.Fatalf("payload = %#v, want %#v", msg.Payload(), testPayload(t))
	}
}

// Intent: consumers should fail clearly when no payload resolver exists for allocating a protobuf target.
func TestRecordToMessageRequiresPayloadResolver(t *testing.T) {
	cfg := defaultConfig()
	record := &kgo.Record{
		Headers: []kgo.RecordHeader{{Key: cfg.headerNames.MessageKind, Value: []byte("order.payment.v1.OrderPaid")}},
	}

	msg, err := recordToMessage(record, cfg)

	if !errors.Is(err, ErrNoPayloadResolver) {
		t.Fatalf("recordToMessage error = %v, want %v", err, ErrNoPayloadResolver)
	}
	if msg.Kind() != "" || msg.Payload() != nil {
		t.Fatalf("message = %#v, want zero", msg)
	}
}

// Intent: nil Kafka records should fail with a protocol-level error before kind or payload resolution.
func TestRecordToMessageRejectsNilRecord(t *testing.T) {
	cfg := defaultConfig()

	msg, err := recordToMessage(nil, cfg)

	if !errors.Is(err, ErrNilRecord) {
		t.Fatalf("recordToMessage error = %v, want %v", err, ErrNilRecord)
	}
	if msg.Kind() != "" || msg.Payload() != nil {
		t.Fatalf("message = %#v, want zero", msg)
	}
}

// Intent: malformed occurred-at headers should fail rather than silently changing event time semantics.
func TestRecordToMessageRejectsInvalidOccurredAt(t *testing.T) {
	cfg := defaultConfig()
	cfg.payloadResolver = message.PayloadResolverFunc(func(message.Kind) (proto.Message, error) {
		return &testdata.TestModel{}, nil
	})
	value, err := cfg.codec.Marshal(testPayload(t))
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	record := &kgo.Record{
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: cfg.headerNames.MessageKind, Value: []byte("order.payment.v1.OrderPaid")},
			{Key: cfg.headerNames.OccurredAt, Value: []byte("not-a-time")},
		},
	}

	msg, err := recordToMessage(record, cfg)

	if err == nil {
		t.Fatal("recordToMessage returned nil error, want invalid time error")
	}
	if msg.Kind() != "" || msg.Payload() != nil {
		t.Fatalf("message = %#v, want zero", msg)
	}
}

// Intent: a resolver that cannot allocate a payload should fail before decode so corrupt handler inputs are not created.
func TestRecordToMessageRejectsNilPayloadResolverResult(t *testing.T) {
	cfg := defaultConfig()
	cfg.payloadResolver = message.PayloadResolverFunc(func(message.Kind) (proto.Message, error) {
		return nil, nil
	})
	record := &kgo.Record{
		Headers: []kgo.RecordHeader{{Key: cfg.headerNames.MessageKind, Value: []byte("order.payment.v1.OrderPaid")}},
	}

	msg, err := recordToMessage(record, cfg)

	if !errors.Is(err, message.ErrNilPayloadFactory) {
		t.Fatalf("recordToMessage error = %v, want %v", err, message.ErrNilPayloadFactory)
	}
	if msg.Kind() != "" || msg.Payload() != nil {
		t.Fatalf("message = %#v, want zero", msg)
	}
}

// Intent: typed-nil protobuf resolver results should fail as nil payloads instead of reaching protobuf decode and panicking.
func TestRecordToMessageRejectsTypedNilPayloadResolverResult(t *testing.T) {
	cfg := defaultConfig()
	cfg.payloadResolver = message.PayloadResolverFunc(func(message.Kind) (proto.Message, error) {
		var payload *testdata.TestModel
		return payload, nil
	})
	record := &kgo.Record{
		Headers: []kgo.RecordHeader{{Key: cfg.headerNames.MessageKind, Value: []byte("order.payment.v1.OrderPaid")}},
	}

	msg, err := recordToMessage(record, cfg)

	if !errors.Is(err, message.ErrNilPayloadFactory) {
		t.Fatalf("recordToMessage error = %v, want %v", err, message.ErrNilPayloadFactory)
	}
	if msg.Kind() != "" || msg.Payload() != nil {
		t.Fatalf("message = %#v, want zero", msg)
	}
}

// Intent: retry and DLQ flows need cloned records whose mutable byte slices cannot mutate the original Kafka record.
func TestCloneRecordDeepCopiesRecord(t *testing.T) {
	original := &kgo.Record{
		Topic:     "orders",
		Key:       []byte("order-1"),
		Value:     []byte("payload"),
		Timestamp: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		Headers: []kgo.RecordHeader{
			{Key: "trace_id", Value: []byte("trace-1")},
		},
	}

	cloned := cloneRecord(original)

	if cloned == original {
		t.Fatal("cloneRecord returned the original pointer")
	}
	cloned.Key[0] = 'X'
	cloned.Value[0] = 'X'
	cloned.Headers[0].Value[0] = 'X'

	if string(original.Key) != "order-1" {
		t.Fatalf("original key mutated to %q", original.Key)
	}
	if string(original.Value) != "payload" {
		t.Fatalf("original value mutated to %q", original.Value)
	}
	if string(original.Headers[0].Value) != "trace-1" {
		t.Fatalf("original header mutated to %q", original.Headers[0].Value)
	}
	if cloneRecord(nil) != nil {
		t.Fatal("cloneRecord(nil) returned non-nil")
	}
}

// Intent: retry attempt parsing should treat absent, malformed, and negative headers as first-attempt records.
func TestRetryAttemptParsesHeader(t *testing.T) {
	tests := []struct {
		name    string
		headers []kgo.RecordHeader
		want    int
	}{
		{name: "missing", want: 0},
		{name: "empty", headers: []kgo.RecordHeader{{Key: headerRetryAttempt}}, want: 0},
		{name: "invalid", headers: []kgo.RecordHeader{{Key: headerRetryAttempt, Value: []byte("nope")}}, want: 0},
		{name: "negative", headers: []kgo.RecordHeader{{Key: headerRetryAttempt, Value: []byte("-1")}}, want: 0},
		{name: "valid", headers: []kgo.RecordHeader{{Key: headerRetryAttempt, Value: []byte("3")}}, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryAttempt(tt.headers); got != tt.want {
				t.Fatalf("retryAttempt() = %d, want %d", got, tt.want)
			}
		})
	}
}
