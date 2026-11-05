package kafka

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// logEntry is one record captured by logRecorder, attributes flattened by key.
type logEntry struct {
	level slog.Level
	msg   string
	attrs map[string]slog.Value
	// caller is the function slog reports as the record's source.
	caller string
}

// logRecorder is a slog.Handler that keeps every record, for asserting levels
// and attributes.
type logRecorder struct {
	mu      *sync.Mutex
	entries *[]logEntry
	attrs   []slog.Attr
}

func newLogRecorder() *logRecorder {
	return &logRecorder{mu: &sync.Mutex{}, entries: &[]logEntry{}}
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	e := logEntry{level: rec.Level, msg: rec.Message, attrs: map[string]slog.Value{}}
	if src := rec.Source(); src != nil {
		e.caller = src.Function
	}
	for _, a := range r.attrs {
		e.attrs[a.Key] = a.Value
	}
	rec.Attrs(func(a slog.Attr) bool {
		e.attrs[a.Key] = a.Value
		return true
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	*r.entries = append(*r.entries, e)
	return nil
}

func (r *logRecorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logRecorder{mu: r.mu, entries: r.entries, attrs: append(slices.Clone(r.attrs), attrs...)}
}

func (r *logRecorder) WithGroup(string) slog.Handler { return r }

func (r *logRecorder) find(msg string) []logEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []logEntry
	for _, e := range *r.entries {
		if e.msg == msg {
			out = append(out, e)
		}
	}
	return out
}

func (r *logRecorder) messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range *r.entries {
		out = append(out, e.level.String()+" "+e.msg)
	}
	return out
}

// one asserts exactly one record with msg at level, carrying want (by
// string form) and the keys in has.
func (r *logRecorder) one(t *testing.T, level slog.Level, msg string, want map[string]string, has ...string) logEntry {
	t.Helper()
	got := r.find(msg)
	if len(got) != 1 {
		t.Fatalf("records %q = %d, want 1; all: %v", msg, len(got), r.messages())
	}
	e := got[0]
	if e.level != level {
		t.Fatalf("%q level = %v, want %v", msg, e.level, level)
	}
	for k, v := range want {
		if a, ok := e.attrs[k]; !ok || a.String() != v {
			t.Fatalf("%q attr %s = %v (set %v), want %s", msg, k, a, ok, v)
		}
	}
	for _, k := range has {
		if _, ok := e.attrs[k]; !ok {
			t.Fatalf("%q lacks attr %s; attrs %v", msg, k, e.attrs)
		}
	}
	return e
}

func (r *logRecorder) none(t *testing.T, msg string) {
	t.Helper()
	if got := r.find(msg); len(got) != 0 {
		t.Fatalf("records %q = %d, want none", msg, len(got))
	}
}

// recordKeys are the attributes every message-level record carries.
var recordKeys = []string{"kafka_topic", "kafka_partition", "kafka_offset", "message_kind", "message_id", "stage", "error"}

// Intent: a failure that will be retried is a warning with its attempt and
// backoff; the pause it causes is debug detail.
func TestConsumerLogsRetry(t *testing.T) {
	logs := newLogRecorder()
	h := newHarness(t, RetryPolicy(RetryConfig{}), WithLogger(slog.New(logs)))
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 1, 4)))
	h.subscribe(func(context.Context, message.Message) error { return errors.New("database unavailable") })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	logs.one(t, slog.LevelWarn, "kafka message retrying", map[string]string{
		"kafka_topic": testTopic, "kafka_partition": "1", "kafka_offset": "4",
		"message_kind": string(testKind), "message_id": "p1-o4", "stage": "handle",
		"attempt": "1", "final": "false", "backoff_ms": "100",
	}, recordKeys...)
	logs.one(t, slog.LevelDebug, "kafka partition paused", map[string]string{
		"kafka_topic": testTopic, "kafka_partition": "1", "kafka_offset": "4", "backoff_ms": "100",
	})
	h.resumes.fire(t, 0)
	logs.one(t, slog.LevelDebug, "kafka partition resumed", map[string]string{"kafka_topic": testTopic, "kafka_partition": "1"})
}

// Intent: a dropped message is an error when it is lost, and a warning when
// the Recoverer kept it; a failing Recoverer is a warning that will repeat.
func TestConsumerLogsDrop(t *testing.T) {
	failing := func(context.Context, message.Message) error { return errors.New("fail") }

	t.Run("lost", func(t *testing.T) {
		logs := newLogRecorder()
		h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)))
		h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
		h.subscribe(failing)
		if err := h.run(); err != nil {
			t.Fatalf("Run error = %v", err)
		}
		logs.one(t, slog.LevelError, "kafka message dropped", map[string]string{"final": "true", "attempt": "1"}, recordKeys...)
	})

	t.Run("recovered", func(t *testing.T) {
		logs := newLogRecorder()
		h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)),
			WithRecoverer(func(context.Context, Error) error { return nil }))
		h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
		h.subscribe(failing)
		if err := h.run(); err != nil {
			t.Fatalf("Run error = %v", err)
		}
		logs.one(t, slog.LevelWarn, "kafka message recovered", map[string]string{"final": "true"}, recordKeys...)
		logs.none(t, "kafka message dropped")
	})

	t.Run("recoverer fails", func(t *testing.T) {
		logs := newLogRecorder()
		h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)),
			WithRecoverer(func(context.Context, Error) error { return errors.New("table unavailable") }))
		h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
		h.subscribe(failing)
		if err := h.run(); err != nil {
			t.Fatalf("Run error = %v", err)
		}
		logs.one(t, slog.LevelWarn, "kafka recoverer failed", map[string]string{"attempt": "1", "backoff_ms": "100", "stage": "recover"}, "error", "kafka_offset")
	})
}

// Intent: a policy stop and a commit failure are errors, and so is the
// consumer stopping because of them; a normal start and stop are info.
func TestConsumerLogsLifecycleAndFatalErrors(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		logs := newLogRecorder()
		h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)))
		h.subscribe(func(context.Context, message.Message) error { return nil })
		if err := h.run(); err != nil {
			t.Fatalf("Run error = %v", err)
		}
		logs.one(t, slog.LevelInfo, "kafka consumer started", nil)
		logs.one(t, slog.LevelInfo, "kafka consumer stopped", nil)
		logs.none(t, "kafka poll error")
	})

	t.Run("stop", func(t *testing.T) {
		logs := newLogRecorder()
		stop := FailurePolicyFunc(func(context.Context, Error) FailureAction { return FailureActionStop })
		h := newHarness(t, stop, WithLogger(slog.New(logs)))
		h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
		h.subscribe(func(context.Context, message.Message) error { return errors.New("fail") })
		if err := h.run(); !errors.Is(err, ErrStopped) {
			t.Fatalf("Run error = %v", err)
		}
		logs.one(t, slog.LevelError, "kafka message stopping consumer", map[string]string{"final": "true"}, recordKeys...)
		logs.one(t, slog.LevelError, "kafka consumer stopped", nil, "error")
	})

	t.Run("commit", func(t *testing.T) {
		logs := newLogRecorder()
		h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)))
		h.client.commitErr = errors.New("coordinator unavailable")
		h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
		h.subscribe(func(context.Context, message.Message) error { return nil })
		if err := h.run(); !errors.Is(err, ErrCommitFailed) {
			t.Fatalf("Run error = %v", err)
		}
		logs.one(t, slog.LevelError, "kafka commit failed", map[string]string{"batch_partitions": "1"}, "error")
	})
}

// Intent: poll errors that Run survives are warnings.
func TestConsumerLogsPollErrors(t *testing.T) {
	logs := newLogRecorder()
	h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)))
	h.queue(fetchError(&kgo.ErrGroupSession{Err: errors.New("session lost")}))
	h.subscribe(func(context.Context, message.Message) error { return nil })
	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	logs.one(t, slog.LevelWarn, "kafka poll error", nil, "error")
}

// Intent: every batch leaves a debug summary, and a batch that used more than
// half of the rebalance timeout is a warning: the group is close to dropping
// the member.
func TestConsumerLogsBatches(t *testing.T) {
	logs := newLogRecorder()
	h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)))
	h.consumer.rebalanceTimeout = 40 * time.Millisecond
	cfg := h.consumer.cfg
	h.queue(fetchOf(testRecord(t, cfg, 0, 0), testRecord(t, cfg, 1, 0)))
	h.subscribe(func(context.Context, message.Message) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	})
	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	logs.one(t, slog.LevelDebug, "kafka batch handled", map[string]string{"batch_records": "2", "batch_partitions": "2", "committed_records": "2"}, "elapsed_ms")
	logs.one(t, slog.LevelWarn, "kafka batch slow", map[string]string{"rebalance_timeout_ms": "40"}, "elapsed_ms")
}

// Intent: a handler that outlives its timeout, or shutdown, is a warning:
// its goroutine keeps running beside the consumer.
func TestConsumerLogsHandlerStillRunning(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	logs := newLogRecorder()
	h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)), WithHandlerTimeout(20*time.Millisecond))
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	h.subscribe(func(context.Context, message.Message) error {
		<-release
		return nil
	})
	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	logs.one(t, slog.LevelWarn, "kafka handler still running after timeout", map[string]string{"message_id": "p0-o0", "timeout_ms": "20"}, "kafka_offset")
}

// Intent: the share-group outcomes are logged by what happens to the
// message: a release is a warning, unless it is the last delivery; a reject or
// an accepted failure loses the message and is an error; a record released
// unhandled for its lock is a warning.
func TestShareConsumerLogsOutcomes(t *testing.T) {
	accept := ShareFailurePolicyFunc(func(context.Context, Error) ShareAction { return ShareActionAccept })
	tests := []struct {
		name     string
		policy   ShareFailurePolicy
		count    int32
		deadline time.Duration
		handler  error
		level    slog.Level
		msg      string
		want     map[string]string
	}{
		{"release", ReleaseOrRejectPolicy(), 2, 0, errors.New("x"), slog.LevelWarn, "kafka message released", map[string]string{"attempt": "2", "final": "false"}},
		{"last release", ReleaseOrRejectPolicy(), 5, 0, errors.New("x"), slog.LevelError, "kafka message released on last delivery", map[string]string{"attempt": "5", "final": "true"}},
		{"reject", ReleaseOrRejectPolicy(), 1, 0, NonRetryable(errors.New("x")), slog.LevelError, "kafka message rejected", map[string]string{"final": "true"}},
		{"accepted failure", accept, 1, 0, errors.New("x"), slog.LevelError, "kafka message failure accepted", map[string]string{"final": "true"}},
		{"lock expiring", ReleaseOrRejectPolicy(), 1, time.Second, nil, slog.LevelWarn, "kafka record lock expiring, released unhandled", map[string]string{"kafka_offset": "0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := newLogRecorder()
			h := newShareHarness(t, tt.policy, WithLogger(slog.New(logs)))
			h.consumer.deliveryCount = func(*kgo.Record) int32 { return tt.count }
			if tt.deadline > 0 {
				h.consumer.acquisitionDeadline = func(*kgo.Record) time.Time { return time.Now().Add(tt.deadline) }
			}
			h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, h.consumer.cfg, 0, 0))}
			h.subscribe(func(context.Context, message.Message) error { return tt.handler })
			if err := h.run(); err != nil {
				t.Fatalf("Run error = %v", err)
			}
			logs.one(t, tt.level, tt.msg, tt.want, "kafka_topic", "kafka_partition", "kafka_offset")
			logs.one(t, slog.LevelInfo, "kafka share consumer started", nil)
			logs.one(t, slog.LevelInfo, "kafka share consumer stopped", nil)
		})
	}
}

// Intent: a failed flush of acknowledgements at shutdown is an error, since
// the batch then waits out its acquisition locks.
func TestShareConsumerLogsFlushFailure(t *testing.T) {
	logs := newLogRecorder()
	h := newShareHarness(t, ReleaseOrRejectPolicy(), WithLogger(slog.New(logs)))
	h.client.flushErr = errors.New("broker gone")
	h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, h.consumer.cfg, 0, 0))}
	h.subscribe(func(context.Context, message.Message) error { return nil })
	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	logs.one(t, slog.LevelError, "kafka acks flush failed", nil, "error")
}

// Intent: KgoLogger maps franz-go levels onto slog levels and reports the
// level it was given.
func TestKgoLogger(t *testing.T) {
	logs := newLogRecorder()
	l := KgoLogger(slog.New(logs), kgo.LogLevelInfo)
	if l.Level() != kgo.LogLevelInfo {
		t.Fatalf("Level() = %v, want info", l.Level())
	}
	for level, want := range map[kgo.LogLevel]slog.Level{
		kgo.LogLevelError: slog.LevelError,
		kgo.LogLevelWarn:  slog.LevelWarn,
		kgo.LogLevelInfo:  slog.LevelInfo,
		kgo.LogLevelDebug: slog.LevelDebug,
	} {
		msg := "kgo " + level.String()
		l.Log(level, msg, "broker", "1")
		logs.one(t, want, msg, map[string]string{"broker": "1"})
	}
}

// kgoLogFromWrapper stands in for franz-go's own logger wrapper, which sits
// between a franz-go call site and KgoLogger.
func kgoLogFromWrapper(l kgo.Logger, msg string) {
	l.Log(kgo.LogLevelInfo, msg)
}

// Intent: a franz-go log names the franz-go function that logged it as its
// source, skipping franz-go's logger wrapper and KgoLogger itself, so the
// source of every kgo log is not the adapter's one line.
func TestKgoLoggerSource(t *testing.T) {
	logs := newLogRecorder()
	adapted := kgoLogger{logger: slog.New(logs), level: kgo.LogLevelInfo,
		skip: append(slices.Clone(kgoLogSkip), pkgPath+".kgoLogFromWrapper")}

	adapted.Log(kgo.LogLevelInfo, "direct")
	kgoLogFromWrapper(adapted, "wrapped")

	want := pkgPath + ".TestKgoLoggerSource"
	for _, msg := range []string{"direct", "wrapped"} {
		if got := logs.one(t, slog.LevelInfo, msg, nil).caller; got != want {
			t.Errorf("%q source = %s, want %s", msg, got, want)
		}
	}
}

// Intent: with a real franz-go client, every log's source is franz-go code,
// past its logger wrapper. No broker is needed: dialing a closed port logs.
func TestKgoLoggerSourceWithClient(t *testing.T) {
	logs := newLogRecorder()
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:1"),
		kgo.WithLogger(KgoLogger(slog.New(logs), kgo.LogLevelDebug)),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.Ping(ctx)
	client.Close()

	logs.mu.Lock()
	defer logs.mu.Unlock()
	if len(*logs.entries) == 0 {
		t.Fatal("franz-go logged nothing")
	}
	for _, e := range *logs.entries {
		if !strings.HasPrefix(e.caller, "github.com/twmb/franz-go/pkg/kgo.") ||
			strings.Contains(e.caller, "wrappedLogger") {
			t.Errorf("%q source = %q, want a franz-go call site", e.msg, e.caller)
		}
	}
}

// Intent: the consumers' own logs name the consumer code that logged them,
// not a shared logging helper.
func TestConsumerLogSources(t *testing.T) {
	logs := newLogRecorder()
	h := newHarness(t, DropAndCommitPolicy(), WithLogger(slog.New(logs)))
	h.subscribe(func(context.Context, message.Message) error { return nil })
	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	share := newShareHarness(t, ReleaseOrRejectPolicy(), WithLogger(slog.New(logs)))
	share.subscribe(func(context.Context, message.Message) error { return nil })
	if err := share.run(); err != nil {
		t.Fatalf("share Run error = %v", err)
	}

	for msg, want := range map[string]string{
		"kafka consumer started":       pkgPath + ".(*Consumer).Run",
		"kafka consumer stopped":       pkgPath + ".(*Consumer).Run",
		"kafka share consumer started": pkgPath + ".(*ShareConsumer).Run",
		"kafka share consumer stopped": pkgPath + ".(*ShareConsumer).Run",
	} {
		if got := logs.one(t, slog.LevelInfo, msg, nil).caller; got != want {
			t.Errorf("%q source = %s, want %s", msg, got, want)
		}
	}
}

const pkgPath = "github.com/go-jimu/contrib/message/kafka"

// Intent: without WithLogger the consumers log to slog.Default(), and a nil
// logger keeps it.
func TestWithLoggerDefaults(t *testing.T) {
	cfg := defaultConfig()
	WithLogger(nil)(&cfg)
	if got := newDispatcher(cfg).log; got != slog.Default() {
		t.Fatalf("logger without WithLogger = %p, want slog.Default()", got)
	}

	custom := slog.New(slog.DiscardHandler)
	WithLogger(custom)(&cfg)
	WithLogger(nil)(&cfg)
	if got := newDispatcher(cfg).log; got != custom {
		t.Fatalf("WithLogger(nil) replaced the logger: got %p, want %p", got, custom)
	}
}
