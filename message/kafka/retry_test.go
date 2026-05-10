package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Intent: handler failures below the max attempt boundary should be retried on the retry topic.
func TestFailureActionRetriesHandleErrorBelowMaxAttempts(t *testing.T) {
	cfg := defaultConfig()
	failure := Error{
		Stage:  StageHandle,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("handler failed"),
	}

	decision := failureAction(failure, cfg)

	if decision.err != nil {
		t.Fatalf("failureAction returned error: %v", decision.err)
	}
	if decision.kind != actionRetry {
		t.Fatalf("decision kind = %v, want %v", decision.kind, actionRetry)
	}
	if decision.attempt != 1 {
		t.Fatalf("attempt = %d, want 1", decision.attempt)
	}
	if decision.topic != "orders.retry" {
		t.Fatalf("topic = %q, want orders.retry", decision.topic)
	}
}

// Intent: reaching the max attempt boundary should stop retrying and route the failure to DLQ.
func TestFailureActionDLQAtMaxAttempts(t *testing.T) {
	cfg := defaultConfig()
	failure := Error{
		Stage: StageHandle,
		Record: &kgo.Record{
			Topic:   "orders",
			Headers: []kgo.RecordHeader{{Key: headerRetryAttempt, Value: []byte("2")}},
		},
		Err: errors.New("handler failed again"),
	}

	decision := failureAction(failure, cfg)

	if decision.err != nil {
		t.Fatalf("failureAction returned error: %v", decision.err)
	}
	if decision.kind != actionDLQ {
		t.Fatalf("decision kind = %v, want %v", decision.kind, actionDLQ)
	}
	if decision.attempt != 3 {
		t.Fatalf("attempt = %d, want 3", decision.attempt)
	}
	if decision.topic != "orders.dlq" {
		t.Fatalf("topic = %q, want orders.dlq", decision.topic)
	}
}

// Intent: decode failures should not be retried because the raw record cannot be converted into a handler message.
func TestFailureActionDLQDecodeError(t *testing.T) {
	cfg := defaultConfig()
	failure := Error{
		Stage:  StageDecode,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("decode failed"),
	}

	decision := failureAction(failure, cfg)

	if decision.err != nil {
		t.Fatalf("failureAction returned error: %v", decision.err)
	}
	if decision.kind != actionDLQ {
		t.Fatalf("decision kind = %v, want %v", decision.kind, actionDLQ)
	}
	if decision.attempt != 1 {
		t.Fatalf("attempt = %d, want 1", decision.attempt)
	}
	if decision.topic != "orders.dlq" {
		t.Fatalf("topic = %q, want orders.dlq", decision.topic)
	}
}

// Intent: explicitly disabling DLQ should drop non-retryable failures instead of producing a DLQ record.
func TestFailureActionDLQDisabled(t *testing.T) {
	cfg := defaultConfig()
	WithDLQDisabled()(&cfg)
	failure := Error{
		Stage:  StageDecode,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("decode failed"),
	}

	decision := failureAction(failure, cfg)
	record := buildFailureRecord(failure, decision, cfg)

	if decision.err != nil {
		t.Fatalf("failureAction returned error: %v", decision.err)
	}
	if decision.kind != actionNone {
		t.Fatalf("decision kind = %v, want %v", decision.kind, actionNone)
	}
	if record != nil {
		t.Fatalf("buildFailureRecord = %#v, want nil", record)
	}
}

// Intent: nil source records should be reported as invalid failure context without panicking or building output.
func TestFailureActionReportsNilRecord(t *testing.T) {
	cfg := defaultConfig()
	failure := Error{Stage: StageHandle, Err: errors.New("handler failed")}

	decision := failureAction(failure, cfg)
	record := buildFailureRecord(failure, decision, cfg)

	if !errors.Is(decision.err, ErrNilRecord) {
		t.Fatalf("decision error = %v, want %v", decision.err, ErrNilRecord)
	}
	if decision.kind != actionNone {
		t.Fatalf("decision kind = %v, want %v", decision.kind, actionNone)
	}
	if record != nil {
		t.Fatalf("buildFailureRecord = %#v, want nil", record)
	}
}

// Intent: empty source topics should be reported before a retry or DLQ record with an invalid topic can be built.
func TestFailureActionReportsEmptyTopic(t *testing.T) {
	cfg := defaultConfig()
	failure := Error{
		Stage:  StageHandle,
		Record: &kgo.Record{},
		Err:    errors.New("handler failed"),
	}

	decision := failureAction(failure, cfg)
	record := buildFailureRecord(failure, decision, cfg)

	if !errors.Is(decision.err, ErrNoTopic) {
		t.Fatalf("decision error = %v, want %v", decision.err, ErrNoTopic)
	}
	if decision.kind != actionNone {
		t.Fatalf("decision kind = %v, want %v", decision.kind, actionNone)
	}
	if record != nil {
		t.Fatalf("buildFailureRecord = %#v, want nil", record)
	}
}

// Intent: retry/DLQ records must preserve the raw Kafka payload while adding metadata needed to trace the failure.
func TestBuildFailureRecordPreservesRawRecord(t *testing.T) {
	cfg := defaultConfig()
	source := &kgo.Record{
		Topic:     "orders",
		Partition: 7,
		Offset:    42,
		Key:       []byte("order-1"),
		Value:     []byte("raw-value"),
		Headers:   []kgo.RecordHeader{{Key: "trace_id", Value: []byte("trace-1")}},
	}
	failure := Error{
		Stage:  StageHandle,
		Record: source,
		Err:    errors.New("handler failed"),
	}
	decision := failureDecision{kind: actionRetry, topic: "orders.retry", attempt: 1}

	record := buildFailureRecord(failure, decision, cfg)

	if record == nil {
		t.Fatal("buildFailureRecord returned nil")
	}
	if record.Topic != "orders.retry" {
		t.Fatalf("topic = %q, want orders.retry", record.Topic)
	}
	if string(record.Key) != "order-1" {
		t.Fatalf("key = %q, want order-1", record.Key)
	}
	if string(record.Value) != "raw-value" {
		t.Fatalf("value = %q, want raw-value", record.Value)
	}
	if got := headerValue(record.Headers, "trace_id"); got != "trace-1" {
		t.Fatalf("trace header = %q, want trace-1", got)
	}
	if got := headerValue(record.Headers, headerRetryAttempt); got != "1" {
		t.Fatalf("retry attempt header = %q, want 1", got)
	}
	if got := headerValue(record.Headers, headerOriginalTopic); got != "orders" {
		t.Fatalf("original topic header = %q, want orders", got)
	}
	if got := headerValue(record.Headers, headerOriginalPartition); got != "7" {
		t.Fatalf("original partition header = %q, want 7", got)
	}
	if got := headerValue(record.Headers, headerOriginalOffset); got != "42" {
		t.Fatalf("original offset header = %q, want 42", got)
	}
	if got := headerValue(record.Headers, headerFailedStage); got != "handle" {
		t.Fatalf("failed stage header = %q, want handle", got)
	}
	if got := headerValue(record.Headers, headerFirstError); got != "handler failed" {
		t.Fatalf("first error header = %q, want handler failed", got)
	}
	if got := headerValue(record.Headers, headerLastError); got != "handler failed" {
		t.Fatalf("last error header = %q, want handler failed", got)
	}
}

// Intent: failure record construction must reject decisions whose topic resolution already failed.
func TestBuildFailureRecordRejectsDecisionError(t *testing.T) {
	cfg := defaultConfig()
	failure := Error{
		Stage:  StageHandle,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("handler failed"),
	}
	decision := failureDecision{
		kind:    actionRetry,
		topic:   "orders.retry",
		attempt: 1,
		err:     errors.New("resolve retry topic"),
	}

	record := buildFailureRecord(failure, decision, cfg)

	if record != nil {
		t.Fatalf("buildFailureRecord = %#v, want nil", record)
	}
}

// Intent: failure record construction must reject empty topics so invalid Kafka records are not produced.
func TestBuildFailureRecordRejectsEmptyTopic(t *testing.T) {
	cfg := defaultConfig()
	failure := Error{
		Stage:  StageHandle,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("handler failed"),
	}
	decision := failureDecision{kind: actionRetry, attempt: 1}

	record := buildFailureRecord(failure, decision, cfg)

	if record != nil {
		t.Fatalf("buildFailureRecord = %#v, want nil", record)
	}
}

// Intent: the original failure cause should remain stable while the latest failure text tracks the newest error.
func TestBuildFailureRecordPreservesFirstErrorAndUpdatesLastError(t *testing.T) {
	cfg := defaultConfig()
	source := &kgo.Record{
		Topic: "orders",
		Headers: []kgo.RecordHeader{
			{Key: headerFirstError, Value: []byte("first failure")},
			{Key: headerLastError, Value: []byte("old last failure")},
		},
	}
	failure := Error{
		Stage:  StageRetryPublish,
		Record: source,
		Err:    errors.New("new last failure"),
	}
	decision := failureDecision{kind: actionDLQ, topic: "orders.dlq", attempt: 2}

	record := buildFailureRecord(failure, decision, cfg)

	if record == nil {
		t.Fatal("buildFailureRecord returned nil")
	}
	if got := headerValue(record.Headers, headerFirstError); got != "first failure" {
		t.Fatalf("first error header = %q, want first failure", got)
	}
	if got := headerValue(record.Headers, headerLastError); got != "new last failure" {
		t.Fatalf("last error header = %q, want new last failure", got)
	}
	if count := headerCount(record.Headers, headerLastError); count != 1 {
		t.Fatalf("last error header count = %d, want 1", count)
	}
}

// Intent: callers should be able to route retry and DLQ records using provider-specific topic naming rules.
func TestFailureActionUsesCustomTopicResolvers(t *testing.T) {
	cfg := defaultConfig()
	WithRetryTopicResolver(func(failure Error) (string, error) {
		return failure.Record.Topic + "-retry-custom", nil
	})(&cfg)
	WithDLQTopicResolver(func(failure Error) (string, error) {
		return failure.Record.Topic + "-dlq-custom", nil
	})(&cfg)

	retryDecision := failureAction(Error{
		Stage:  StageHandle,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("handler failed"),
	}, cfg)
	dlqDecision := failureAction(Error{
		Stage:  StageDecode,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("decode failed"),
	}, cfg)

	if retryDecision.err != nil {
		t.Fatalf("retry failureAction returned error: %v", retryDecision.err)
	}
	if retryDecision.topic != "orders-retry-custom" {
		t.Fatalf("retry topic = %q, want orders-retry-custom", retryDecision.topic)
	}
	if dlqDecision.err != nil {
		t.Fatalf("dlq failureAction returned error: %v", dlqDecision.err)
	}
	if dlqDecision.topic != "orders-dlq-custom" {
		t.Fatalf("dlq topic = %q, want orders-dlq-custom", dlqDecision.topic)
	}
}

// Intent: topic resolver failures should stop record construction so callers can surface routing errors safely.
func TestFailureActionReportsResolverError(t *testing.T) {
	cfg := defaultConfig()
	resolveErr := errors.New("resolve retry topic")
	WithRetryTopicResolver(func(Error) (string, error) {
		return "", resolveErr
	})(&cfg)
	failure := Error{
		Stage:  StageHandle,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("handler failed"),
	}

	decision := failureAction(failure, cfg)
	record := buildFailureRecord(failure, decision, cfg)

	if !errors.Is(decision.err, resolveErr) {
		t.Fatalf("decision error = %v, want %v", decision.err, resolveErr)
	}
	if decision.kind != actionRetry {
		t.Fatalf("decision kind = %v, want %v", decision.kind, actionRetry)
	}
	if record != nil {
		t.Fatalf("buildFailureRecord = %#v, want nil", record)
	}
}

// Intent: zero-valued retry policy options should not erase the default max attempts or retryable-stage behavior.
func TestRetryPolicyOptionsPreserveDefaultsForZeroValues(t *testing.T) {
	cfg := defaultConfig()
	WithRetryPolicy(RetryPolicy{})(&cfg)
	WithDLQPolicy(DLQPolicy{})(&cfg)

	decision := failureAction(Error{
		Stage:  StageHandle,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("handler failed"),
	}, cfg)

	if decision.err != nil {
		t.Fatalf("failureAction returned error: %v", decision.err)
	}
	if decision.kind != actionRetry {
		t.Fatalf("decision kind = %v, want %v", decision.kind, actionRetry)
	}
	if decision.attempt != 1 {
		t.Fatalf("attempt = %d, want 1", decision.attempt)
	}

	dlqDecision := failureAction(Error{
		Stage:  StageDecode,
		Record: &kgo.Record{Topic: "orders"},
		Err:    errors.New("decode failed"),
	}, cfg)
	if dlqDecision.kind != actionDLQ {
		t.Fatalf("zero-value DLQ policy decision kind = %v, want %v", dlqDecision.kind, actionDLQ)
	}
}

func headerCount(headers []kgo.RecordHeader, key string) int {
	var count int
	for _, header := range headers {
		if header.Key == key {
			count++
		}
	}
	return count
}
