package kafka

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// startRun runs fn on its own goroutine and returns its result channel.
func startRun(fn func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	return done
}

func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not happen within 5s", what)
		var zero T
		return zero
	}
}

// gracefulHarness is a Consumer harness whose fake client never cancels
// Run's ctx, so only Shutdown stops Run.
func gracefulHarness(t *testing.T, policy FailurePolicy, opts ...Option) *testHarness {
	t.Helper()
	h := newHarness(t, policy, opts...)
	h.client.cancel = nil
	h.client.polled = make(chan context.Context, 1)
	return h
}

func gracefulShareHarness(t *testing.T, policy ShareFailurePolicy, opts ...Option) *shareHarness {
	t.Helper()
	h := newShareHarness(t, policy, opts...)
	h.client.cancel = func() {}
	h.client.polled = make(chan context.Context, 1)
	return h
}

// Intent (#16): a graceful Shutdown does not cancel a running handler: the
// handler finishes on a live ctx, its record is committed, and the partition
// stops before its next record.
func TestConsumerShutdownLetsRunningHandlerFinish(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Minute} {
		t.Run(timeout.String(), func(t *testing.T) {
			h := gracefulHarness(t, DropAndCommitPolicy(), WithHandlerTimeout(timeout))
			cfg := h.consumer.cfg
			h.queue(fetchOf(testRecord(t, cfg, 0, 0), testRecord(t, cfg, 0, 1)))
			started, release := make(chan struct{}), make(chan struct{})
			var handled []string
			var handlerErr error
			h.subscribe(func(ctx context.Context, msg message.Message) error {
				handled = append(handled, msg.ID())
				close(started)
				<-release
				handlerErr = ctx.Err()
				return handlerErr
			})

			runDone := startRun(func() error { return h.consumer.Run(h.ctx) })
			stop := receive(t, h.client.polled, "first poll")
			receive(t, started, "handler start")
			shutdownDone := startRun(func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return h.consumer.Shutdown(ctx)
			})
			receive(t, stop.Done(), "Run's stop")
			close(release)

			if err := receive(t, runDone, "Run return"); err != nil {
				t.Fatalf("Run error = %v, want nil", err)
			}
			if err := receive(t, shutdownDone, "Shutdown return"); err != nil {
				t.Fatalf("Shutdown error = %v, want nil", err)
			}
			if handlerErr != nil {
				t.Fatalf("handler ctx ended during a graceful shutdown: %v", handlerErr)
			}
			if want := []string{"p0-o0"}; !reflect.DeepEqual(handled, want) {
				t.Fatalf("handled = %v, want %v", handled, want)
			}
			if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
				t.Fatalf("commits = %v, want %v", h.client.committedOffsets(), want)
			}
			if len(h.client.setOffsets) != 1 || h.client.setOffsets[0][testTopic][0].Offset != 1 {
				t.Fatalf("SetOffsets = %v, want rewind to offset 1", h.client.setOffsets)
			}
		})
	}
}

// Intent (#16): when Shutdown's ctx ends, the handler's ctx is canceled and
// the record is left uncommitted for redelivery, without the policy.
func TestConsumerShutdownDeadlineCancelsHandler(t *testing.T) {
	var decided atomic.Int32
	policy := FailurePolicyFunc(func(context.Context, Error) FailureAction {
		decided.Add(1)
		return FailureActionDrop
	})
	h := gracefulHarness(t, policy, WithHandlerTimeout(time.Minute))
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	started := make(chan struct{})
	h.subscribe(func(ctx context.Context, _ message.Message) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	runDone := startRun(func() error { return h.consumer.Run(h.ctx) })
	receive(t, started, "handler start")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h.consumer.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want context.DeadlineExceeded", err)
	}

	if err := receive(t, runDone, "Run return"); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if decided.Load() != 0 || len(h.observed.stage(StageHandle)) != 0 {
		t.Fatalf("policy calls = %d, handle observations = %d; want none", decided.Load(), len(h.observed.stage(StageHandle)))
	}
	if len(h.client.commits) != 0 {
		t.Fatalf("commits = %v, want none", h.client.committedOffsets())
	}
	if len(h.client.setOffsets) != 1 || h.client.setOffsets[0][testTopic][0].Offset != 0 {
		t.Fatalf("SetOffsets = %v, want rewind to offset 0", h.client.setOffsets)
	}
}

// Intent (#16): a handler that fails during a graceful Shutdown is a real
// failure: the FailurePolicy and the ErrorObserver see it as usual.
func TestConsumerFailureDuringShutdownFollowsPolicy(t *testing.T) {
	var decided atomic.Int32
	policy := FailurePolicyFunc(func(context.Context, Error) FailureAction {
		decided.Add(1)
		return FailureActionDrop
	})
	h := gracefulHarness(t, policy)
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	started, release := make(chan struct{}), make(chan struct{})
	h.subscribe(func(context.Context, message.Message) error {
		close(started)
		<-release
		return errors.New("boom")
	})

	runDone := startRun(func() error { return h.consumer.Run(h.ctx) })
	stop := receive(t, h.client.polled, "first poll")
	receive(t, started, "handler start")
	shutdownDone := startRun(func() error { return h.consumer.Shutdown(context.Background()) })
	receive(t, stop.Done(), "Run's stop")
	close(release)

	if err := receive(t, runDone, "Run return"); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if err := receive(t, shutdownDone, "Shutdown return"); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	if decided.Load() != 1 || len(h.observed.stage(StageHandle)) != 1 {
		t.Fatalf("policy calls = %d, handle observations = %d; want 1 each", decided.Load(), len(h.observed.stage(StageHandle)))
	}
	if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
		t.Fatalf("commits = %v, want the dropped record committed", h.client.committedOffsets())
	}
}

// Intent (#16): Shutdown before Run makes Run return at once without polling;
// Shutdown after Run returned, or twice, returns nil.
func TestConsumerShutdownBeforeAndAfterRun(t *testing.T) {
	h := gracefulHarness(t, DropAndCommitPolicy())
	if err := h.consumer.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Run = %v, want nil", err)
	}
	if err := h.run(); err != nil {
		t.Fatalf("Run after Shutdown = %v, want nil", err)
	}
	if h.client.pollCount != 0 {
		t.Fatalf("polls = %d, want none after Shutdown", h.client.pollCount)
	}
	if err := h.consumer.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown = %v, want nil", err)
	}
}

// Intent (#16): a graceful Shutdown lets the running handler finish and
// accepts its record; records not started are released.
func TestShareConsumerShutdownLetsRunningHandlerFinish(t *testing.T) {
	h := gracefulShareHarness(t, ReleaseOrRejectPolicy(), WithConcurrency(1))
	cfg := h.consumer.cfg
	h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, cfg, 0, 0), testRecord(t, cfg, 0, 1))}
	started, release := make(chan struct{}), make(chan struct{})
	var handlerErr error
	h.subscribe(func(ctx context.Context, msg message.Message) error {
		if msg.ID() != "p0-o0" {
			t.Errorf("%s was handled after Shutdown", msg.ID())
			return nil
		}
		close(started)
		<-release
		handlerErr = ctx.Err()
		return handlerErr
	})

	runDone := startRun(func() error { return h.consumer.Run(h.ctx) })
	stop := receive(t, h.client.polled, "first poll")
	receive(t, started, "handler start")
	shutdownDone := startRun(func() error { return h.consumer.Shutdown(context.Background()) })
	receive(t, stop.Done(), "Run's stop")
	close(release)

	if err := receive(t, runDone, "Run return"); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if err := receive(t, shutdownDone, "Shutdown return"); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	if handlerErr != nil {
		t.Fatalf("handler ctx ended during a graceful shutdown: %v", handlerErr)
	}
	want := map[string]kgo.AckStatus{"p0-o0": kgo.AckAccept, "p0-o1": kgo.AckRelease}
	for id, status := range want {
		if got, ok := h.acks.get(id); !ok || got != status {
			t.Fatalf("%s ack = %v (set %v), want %v", id, got, ok, status)
		}
	}
	if len(h.client.flushCtxs) != 1 || h.client.flushCtxs[0] != nil {
		t.Fatalf("FlushAcks ctx errors = %v, want one flush on a live ctx", h.client.flushCtxs)
	}
}

// Intent (#16): when Shutdown's ctx ends, the handler's ctx is canceled and
// its record is released without the policy.
func TestShareConsumerShutdownDeadlineReleases(t *testing.T) {
	var decided atomic.Int32
	policy := ShareFailurePolicyFunc(func(context.Context, Error) ShareAction {
		decided.Add(1)
		return ShareActionReject
	})
	h := gracefulShareHarness(t, policy)
	h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, h.consumer.cfg, 0, 0))}
	started := make(chan struct{})
	h.subscribe(func(ctx context.Context, _ message.Message) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	runDone := startRun(func() error { return h.consumer.Run(h.ctx) })
	receive(t, started, "handler start")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h.consumer.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want context.DeadlineExceeded", err)
	}

	if err := receive(t, runDone, "Run return"); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if got, _ := h.acks.get("p0-o0"); got != kgo.AckRelease {
		t.Fatalf("ack = %v, want release", got)
	}
	if decided.Load() != 0 || len(h.observed.stage(StageHandle)) != 0 {
		t.Fatalf("policy calls = %d, handle observations = %d; want none", decided.Load(), len(h.observed.stage(StageHandle)))
	}
}

// Intent (#16): a handler that fails during a graceful Shutdown is acked as
// the ShareFailurePolicy decides, and observed.
func TestShareConsumerFailureDuringShutdownFollowsPolicy(t *testing.T) {
	h := gracefulShareHarness(t, ReleaseOrRejectPolicy())
	h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, h.consumer.cfg, 0, 0))}
	started, release := make(chan struct{}), make(chan struct{})
	h.subscribe(func(context.Context, message.Message) error {
		close(started)
		<-release
		return NonRetryable(errors.New("invalid"))
	})

	runDone := startRun(func() error { return h.consumer.Run(h.ctx) })
	stop := receive(t, h.client.polled, "first poll")
	receive(t, started, "handler start")
	shutdownDone := startRun(func() error { return h.consumer.Shutdown(context.Background()) })
	receive(t, stop.Done(), "Run's stop")
	close(release)

	if err := receive(t, runDone, "Run return"); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if err := receive(t, shutdownDone, "Shutdown return"); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	if got, _ := h.acks.get("p0-o0"); got != kgo.AckReject {
		t.Fatalf("ack = %v, want reject", got)
	}
	if len(h.observed.stage(StageHandle)) != 1 {
		t.Fatalf("handle observations = %d, want 1", len(h.observed.stage(StageHandle)))
	}
}
