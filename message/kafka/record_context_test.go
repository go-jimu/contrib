package kafka

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type traceKey struct{}

// withTrace is a WithRecordContext fn that copies the traceKey value a kgo
// hook would have put on record.Context, as kotel does with trace context.
func withTrace(ctx context.Context, record *kgo.Record) context.Context {
	if record.Context == nil {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, record.Context.Value(traceKey{}))
}

func tracedRecord(record *kgo.Record, trace string) *kgo.Record {
	record.Context = context.WithValue(context.Background(), traceKey{}, trace)
	return record
}

// ctxTraces records the traceKey value of the ctx each call was made with.
type ctxTraces struct {
	mu     sync.Mutex
	traces map[string]any
}

func newCtxTraces() *ctxTraces { return &ctxTraces{traces: map[string]any{}} }

func (c *ctxTraces) note(what string, ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces[what] = ctx.Value(traceKey{})
}

func (c *ctxTraces) want(t *testing.T, trace string, whats ...string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, what := range whats {
		if got, ok := c.traces[what]; !ok || got != trace {
			t.Errorf("%s ran with trace %v (seen %v), want %s", what, got, ok, trace)
		}
	}
}

// Handle implements slog.Handler, noting each record's message.
func (c *ctxTraces) Handle(ctx context.Context, r slog.Record) error {
	c.note("log "+r.Message, ctx)
	return nil
}
func (c *ctxTraces) Enabled(context.Context, slog.Level) bool { return true }
func (c *ctxTraces) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *ctxTraces) WithGroup(string) slog.Handler            { return c }

// Intent: WithRecordContext derives the ctx of a record's whole work from
// Run's ctx, so the trace a kgo hook extracted reaches the handlers, the
// FailurePolicy, the ErrorObserver, and the failure log, and the handlers'
// ctx still ends with Run.
func TestConsumerRecordContextReachesTheRecordsWork(t *testing.T) {
	traces := newCtxTraces()
	policy := FailurePolicyFunc(func(ctx context.Context, _ Error) FailureAction {
		traces.note("policy", ctx)
		return FailureActionDrop
	})
	h := newHarness(t, policy,
		WithRecordContext(withTrace),
		WithLogger(slog.New(traces)),
		WithErrorObserver(func(ctx context.Context, _ Error) { traces.note("observer", ctx) }),
	)
	h.queue(fetchOf(tracedRecord(testRecord(t, h.consumer.cfg, 0, 0), "trace-1")))
	var handlerCtx context.Context
	h.subscribe(func(ctx context.Context, _ message.Message) error {
		handlerCtx = ctx
		traces.note("handler", ctx)
		return errors.New("fail")
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	traces.want(t, "trace-1", "handler", "policy", "observer", "log kafka message dropped")
	if handlerCtx.Err() == nil {
		t.Error("handler ctx did not end with Run: the record ctx is not derived from Run's ctx")
	}
}

// Intent: the ShareConsumer derives the record ctx the same way.
func TestShareConsumerRecordContextReachesTheRecordsWork(t *testing.T) {
	traces := newCtxTraces()
	policy := ShareFailurePolicyFunc(func(ctx context.Context, _ Error) ShareAction {
		traces.note("policy", ctx)
		return ShareActionReject
	})
	h := newShareHarness(t, policy,
		WithRecordContext(withTrace),
		WithLogger(slog.New(traces)),
		WithErrorObserver(func(ctx context.Context, _ Error) { traces.note("observer", ctx) }),
	)
	h.client.fetches = append(h.client.fetches, fetchOf(tracedRecord(testRecord(t, h.consumer.cfg, 0, 0), "trace-2")))
	h.subscribe(func(ctx context.Context, _ message.Message) error {
		traces.note("handler", ctx)
		return errors.New("fail")
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	traces.want(t, "trace-2", "handler", "policy", "observer", "log kafka message rejected")
}

// Intent: a nil result keeps Run's ctx, so a fn that finds nothing to add
// cannot cut the handlers off from shutdown.
func TestRecordContextNilResultKeepsRunContext(t *testing.T) {
	h := newHarness(t, DropAndCommitPolicy(),
		WithRecordContext(func(context.Context, *kgo.Record) context.Context { return nil }))
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	var handlerCtx context.Context
	h.subscribe(func(ctx context.Context, _ message.Message) error {
		handlerCtx = ctx
		return nil
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if handlerCtx == nil || handlerCtx.Err() == nil {
		t.Fatal("handler ctx did not end with Run")
	}
}
