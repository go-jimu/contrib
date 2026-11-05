package kafka

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Intent (A1): the consumer loop is only safe with rebalances blocked during a
// batch and manual commits in a group, so a client missing any of these is
// rejected up front, naming what is missing.
func TestNewConsumerValidatesClientOptions(t *testing.T) {
	cfg := testConsumerConfig()
	resolver := WithPayloadResolver(cfg.payloadResolver)
	base := []kgo.Opt{kgo.SeedBrokers("127.0.0.1:1"), kgo.ConsumeTopics(testTopic)}
	group := kgo.ConsumerGroup("g")
	manual := kgo.DisableAutoCommit()
	block := kgo.BlockRebalanceOnPoll()

	tests := []struct {
		name    string
		opts    []kgo.Opt
		missing string
	}{
		{name: "all required options", opts: []kgo.Opt{group, manual, block}},
		// kgo itself rejects DisableAutoCommit without a group.
		{name: "no consumer group", opts: []kgo.Opt{block}, missing: "kgo.ConsumerGroup"},
		{name: "auto commit", opts: []kgo.Opt{group, block}, missing: "kgo.DisableAutoCommit"},
		{name: "rebalance not blocked", opts: []kgo.Opt{group, manual}, missing: "kgo.BlockRebalanceOnPoll"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := kgo.NewClient(append(slices.Clone(base), tt.opts...)...)
			if err != nil {
				t.Fatalf("kgo.NewClient: %v", err)
			}
			defer client.Close()

			_, err = NewConsumer(client, DropAndCommitPolicy(), resolver)

			if tt.missing == "" {
				if err != nil {
					t.Fatalf("NewConsumer error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidClient) {
				t.Fatalf("NewConsumer error = %v, want ErrInvalidClient", err)
			}
			if !strings.Contains(err.Error(), tt.missing) {
				t.Fatalf("NewConsumer error = %q, want it to name %s", err, tt.missing)
			}
		})
	}
}

// Intent: a consumer without a policy or payload resolver fails fast.
func TestNewConsumerRejectsMissingPolicyAndResolver(t *testing.T) {
	if _, err := NewConsumer(nil, nil, WithPayloadResolver(testConsumerConfig().payloadResolver)); !errors.Is(err, ErrNilFailurePolicy) {
		t.Fatalf("nil policy error = %v, want ErrNilFailurePolicy", err)
	}
	if _, err := NewConsumer(nil, DropAndCommitPolicy()); !errors.Is(err, ErrNoPayloadResolver) {
		t.Fatalf("no resolver error = %v, want ErrNoPayloadResolver", err)
	}
}

// Intent: a nil client is reported by Run instead of panicking.
func TestConsumerRunNilClient(t *testing.T) {
	consumer, err := NewConsumer(nil, DropAndCommitPolicy(), WithPayloadResolver(testConsumerConfig().payloadResolver))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := consumer.Run(context.Background()); !errors.Is(err, ErrNilClient) {
		t.Fatalf("Run error = %v, want ErrNilClient", err)
	}
}

// Intent: Close closes the client only when ownership was transferred.
func TestConsumerCloseHonorsClientOwnership(t *testing.T) {
	for _, owned := range []bool{false, true} {
		h := newHarness(t, DropAndCommitPolicy(), WithCloseClient(owned))
		h.consumer.Close()
		if h.client.closed != owned {
			t.Fatalf("WithCloseClient(%v): closed = %v", owned, h.client.closed)
		}
	}
}

// Intent (A2, A3): partitions are handled concurrently, so a slow partition
// does not hold up another, while each partition keeps offset order. Then the
// whole batch is committed in one call with each partition's last record.
func TestConsumerHandlesPartitionsConcurrentlyInOrder(t *testing.T) {
	h := newHarness(t, DropAndCommitPolicy())
	cfg := h.consumer.cfg
	h.queue(fetchOf(
		testRecord(t, cfg, 0, 0), testRecord(t, cfg, 0, 1), testRecord(t, cfg, 0, 2),
		testRecord(t, cfg, 1, 0), testRecord(t, cfg, 1, 1),
	))
	p1Started := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	order := map[string][]string{}
	h.subscribe(func(_ context.Context, msg message.Message) error {
		id := msg.ID()
		partition := id[:2]
		mu.Lock()
		order[partition] = append(order[partition], id)
		mu.Unlock()
		switch id {
		case "p1-o0":
			once.Do(func() { close(p1Started) })
		case "p0-o0":
			// P0 waits for P1: this only completes when partitions run
			// concurrently.
			select {
			case <-p1Started:
			case <-time.After(2 * time.Second):
				return errors.New("p1 never ran while p0 was blocked")
			}
		}
		return nil
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if got := h.observed.stage(StageHandle); len(got) != 0 {
		t.Fatalf("handler failures = %v, want none (partitions ran serially?)", got[0].Err)
	}
	if want := []string{"p0-o0", "p0-o1", "p0-o2"}; !reflect.DeepEqual(order["p0"], want) {
		t.Fatalf("p0 order = %v, want %v", order["p0"], want)
	}
	if want := []string{"p1-o0", "p1-o1"}; !reflect.DeepEqual(order["p1"], want) {
		t.Fatalf("p1 order = %v, want %v", order["p1"], want)
	}
	commits := h.client.committedOffsets()
	if len(commits) != 1 {
		t.Fatalf("commits = %v, want one commit for the batch", commits)
	}
	slices.Sort(commits[0])
	if want := []string{"0@2", "1@1"}; !reflect.DeepEqual(commits[0], want) {
		t.Fatalf("committed = %v, want %v", commits[0], want)
	}
}

// Intent (A4, A5): a retried record stops its partition: the batch commits only
// each partition's handled prefix, then pauses and rewinds the failed
// partition to the failed record, all before rebalancing is allowed again.
func TestConsumerRetryCommitsPrefixPausesAndRewinds(t *testing.T) {
	h := newHarness(t, RetryPolicy(RetryConfig{}))
	cfg := h.consumer.cfg
	h.queue(fetchOf(
		testRecord(t, cfg, 0, 0), testRecord(t, cfg, 0, 1), testRecord(t, cfg, 0, 2),
		testRecord(t, cfg, 1, 0), testRecord(t, cfg, 1, 1), testRecord(t, cfg, 1, 2),
	))
	var mu sync.Mutex
	var handled []string
	h.subscribe(func(_ context.Context, msg message.Message) error {
		mu.Lock()
		handled = append(handled, msg.ID())
		mu.Unlock()
		if msg.ID() == "p1-o1" {
			return errors.New("database unavailable")
		}
		return nil
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if slices.Contains(handled, "p1-o2") {
		t.Fatal("p1-o2 was handled after p1-o1 failed; the partition must stop at the failure")
	}
	commits := h.client.committedOffsets()
	if len(commits) != 1 {
		t.Fatalf("commits = %v, want exactly one", commits)
	}
	slices.Sort(commits[0])
	if want := []string{"0@2", "1@0"}; !reflect.DeepEqual(commits[0], want) {
		t.Fatalf("committed = %v, want %v", commits[0], want)
	}
	wantRewind := map[string]map[int32]kgo.EpochOffset{testTopic: {1: {Epoch: 3, Offset: 1}}}
	if len(h.client.setOffsets) != 1 || !reflect.DeepEqual(h.client.setOffsets[0], wantRewind) {
		t.Fatalf("SetOffsets = %v, want %v", h.client.setOffsets, wantRewind)
	}
	if want := []time.Duration{100 * time.Millisecond}; !reflect.DeepEqual(h.resumes.delays(), want) {
		t.Fatalf("resume delays = %v, want %v", h.resumes.delays(), want)
	}
	wantEvents := []string{"poll", "commit", "pause:orders/1", "setoffsets", "allow"}
	if got := h.client.events[:len(wantEvents)]; !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("events = %v, want prefix %v", h.client.events, wantEvents)
	}
	failures := h.observed.stage(StageHandle)
	if len(failures) != 1 || failures[0].Attempt != 1 || failures[0].Final {
		t.Fatalf("handle failures = %+v, want one with Attempt 1, Final false", failures)
	}
}

// Intent (A6): after the backoff the partition is resumed and the same record
// is handled again; attempts accumulate across polls and drive the backoff.
func TestConsumerRetriesRecordInPlaceAfterResume(t *testing.T) {
	h := newHarness(t, RetryPolicy(RetryConfig{MaxAttempts: 5}))
	cfg := h.consumer.cfg
	record := testRecord(t, cfg, 1, 0)
	h.queue(fetchOf(record), fetchOf(record), fetchOf(record))
	// Each refetch only happens once the previous pause was lifted.
	h.client.pollHooks[2] = func() { h.resumes.fire(t, 0) }
	h.client.pollHooks[3] = func() { h.resumes.fire(t, 1) }
	calls := 0
	h.subscribe(func(context.Context, message.Message) error {
		calls++
		if calls <= 2 {
			return errors.New("database unavailable")
		}
		return nil
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if calls != 3 {
		t.Fatalf("handler calls = %d, want 3", calls)
	}
	if want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}; !reflect.DeepEqual(h.resumes.delays(), want) {
		t.Fatalf("resume delays = %v, want %v", h.resumes.delays(), want)
	}
	failures := h.observed.stage(StageHandle)
	if len(failures) != 2 || failures[0].Attempt != 1 || failures[1].Attempt != 2 || failures[0].Final || failures[1].Final {
		t.Fatalf("handle failures = %+v, want attempts 1 and 2, not final", failures)
	}
	if got := strings.Count(strings.Join(h.client.events, ","), "resume:orders/1"); got != 2 {
		t.Fatalf("resumes of orders/1 = %d, want 2; events %v", got, h.client.events)
	}
	if want := [][]string{{"1@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
		t.Fatalf("commits = %v, want %v", h.client.committedOffsets(), want)
	}
}

// Intent (A7): MaxAttempts is the total number of attempts: attempts below it
// retry, the attempt at it applies OnExhausted. Stop keeps the failed record
// uncommitted while the partition's earlier records stay committed.
func TestConsumerRetryPolicyExhaustion(t *testing.T) {
	handlerErr := errors.New("still failing")

	t.Run("drop", func(t *testing.T) {
		h := newHarness(t, RetryPolicy(RetryConfig{MaxAttempts: 3}))
		record := testRecord(t, h.consumer.cfg, 0, 0)
		h.queue(fetchOf(record), fetchOf(record), fetchOf(record))
		h.subscribe(func(context.Context, message.Message) error { return handlerErr })

		if err := h.run(); err != nil {
			t.Fatalf("Run error = %v", err)
		}

		failures := h.observed.stage(StageHandle)
		if len(failures) != 3 {
			t.Fatalf("failures = %d, want 3", len(failures))
		}
		last := failures[2]
		if last.Attempt != 3 || !last.Final || failures[1].Final {
			t.Fatalf("failures = %+v, want only attempt 3 final", failures)
		}
		if got := len(h.resumes.delays()); got != 2 {
			t.Fatalf("scheduled backoffs = %d, want 2", got)
		}
		if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
			t.Fatalf("commits = %v, want the dropped record committed once", h.client.committedOffsets())
		}
	})

	t.Run("stop", func(t *testing.T) {
		h := newHarness(t, RetryPolicy(RetryConfig{MaxAttempts: 2, OnExhausted: FailureActionStop}))
		cfg := h.consumer.cfg
		ok, bad := testRecord(t, cfg, 0, 0), testRecord(t, cfg, 0, 1)
		h.queue(fetchOf(ok, bad), fetchOf(bad))
		h.subscribe(func(_ context.Context, msg message.Message) error {
			if msg.ID() == "p0-o1" {
				return handlerErr
			}
			return nil
		})

		err := h.run()

		if !errors.Is(err, ErrStopped) || !errors.Is(err, handlerErr) {
			t.Fatalf("Run error = %v, want ErrStopped wrapping the handler error", err)
		}
		if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
			t.Fatalf("commits = %v, want only the record before the stop", h.client.committedOffsets())
		}
		last := h.client.setOffsets[len(h.client.setOffsets)-1]
		if got := last[testTopic][0].Offset; got != 1 {
			t.Fatalf("rewound to offset %d, want 1", got)
		}
		failures := h.observed.stage(StageHandle)
		if len(failures) != 2 || !failures[1].Final || failures[1].Attempt != 2 {
			t.Fatalf("failures = %+v, want final failure at attempt 2", failures)
		}
	})
}

// Intent (A8): attempts belong to the record at the head of the partition; a
// new head, as when another member settled the old one, starts again at 1.
func TestConsumerAttemptsResetForNewHeadRecord(t *testing.T) {
	h := newHarness(t, RetryPolicy(RetryConfig{MaxAttempts: 5}))
	cfg := h.consumer.cfg
	h.queue(fetchOf(testRecord(t, cfg, 0, 5)), fetchOf(testRecord(t, cfg, 0, 6)))
	h.subscribe(func(context.Context, message.Message) error { return errors.New("fail") })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	failures := h.observed.stage(StageHandle)
	if len(failures) != 2 || failures[1].Record.Offset != 6 || failures[1].Attempt != 1 {
		t.Fatalf("failures = %+v, want offset 6 at attempt 1", failures)
	}
}

// Intent (A9b): a handler error wrapped with NonRetryable skips retries even
// under RetryPolicy, keeps the original error reachable, and is committed.
func TestConsumerNonRetryableErrorIsNotRetried(t *testing.T) {
	h := newHarness(t, RetryPolicy(RetryConfig{}))
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	errInvalid := errors.New("order does not exist")
	h.subscribe(func(context.Context, message.Message) error { return NonRetryable(errInvalid) })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if len(h.client.setOffsets) != 0 || len(h.resumes.delays()) != 0 {
		t.Fatalf("rewinds = %v, backoffs = %v, want none", h.client.setOffsets, h.resumes.delays())
	}
	if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
		t.Fatalf("commits = %v, want %v", h.client.committedOffsets(), want)
	}
	failures := h.observed.stage(StageHandle)
	if len(failures) != 1 || !failures[0].Final || !errors.Is(failures[0].Err, errInvalid) {
		t.Fatalf("failures = %+v, want one final failure wrapping the handler error", failures)
	}
}

// Intent: decode and unhandled failures are classified by stage and, under
// the default RetryPolicy, settled at once rather than retried.
func TestConsumerDecodeAndUnhandledFailuresAreNotRetried(t *testing.T) {
	tests := []struct {
		name   string
		record func(*testing.T, config) *kgo.Record
		stage  Stage
		err    error
	}{
		{
			name: "decode",
			record: func(t *testing.T, cfg config) *kgo.Record {
				r := testRecord(t, cfg, 0, 0)
				r.Headers = nil // no kind header
				return r
			},
			stage: StageDecode,
			err:   ErrNoKind,
		},
		{
			name: "unhandled",
			record: func(t *testing.T, cfg config) *kgo.Record {
				return testKindRecord(t, cfg, "order.payment.v1.Unknown", 0, 0)
			},
			stage: StageUnhandled,
			err:   message.ErrUnhandledKind,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, RetryPolicy(RetryConfig{}))
			h.queue(fetchOf(tt.record(t, h.consumer.cfg)))
			h.subscribe(func(context.Context, message.Message) error { return nil })

			if err := h.run(); err != nil {
				t.Fatalf("Run error = %v", err)
			}

			failures := h.observed.stage(tt.stage)
			if len(failures) != 1 || !errors.Is(failures[0].Err, tt.err) || !failures[0].Final {
				t.Fatalf("%s failures = %+v, want one final failure matching %v", tt.stage, failures, tt.err)
			}
			if len(h.resumes.delays()) != 0 {
				t.Fatalf("backoffs = %v, want none", h.resumes.delays())
			}
			if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
				t.Fatalf("commits = %v, want %v", h.client.committedOffsets(), want)
			}
		})
	}
}

// Intent (A10): the Recoverer gets a dropped record before its offset is
// committed, so a crash between the two cannot lose the record silently.
func TestConsumerRecovererRunsBeforeCommit(t *testing.T) {
	var got []Error
	var h *testHarness
	h = newHarness(t, DropAndCommitPolicy(), WithRecoverer(func(_ context.Context, failure Error) error {
		got = append(got, failure)
		h.client.event("recover")
		return nil
	}))
	record := testRecord(t, h.consumer.cfg, 0, 0)
	h.queue(fetchOf(record))
	h.subscribe(func(context.Context, message.Message) error { return errors.New("fail") })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if len(got) != 1 || got[0].Record != record || !got[0].Final || got[0].Attempt != 1 || got[0].Stage != StageHandle {
		t.Fatalf("recoverer got %+v, want the final StageHandle failure of the source record", got)
	}
	events := strings.Join(h.client.events, ",")
	if !strings.Contains(events, "recover,commit") {
		t.Fatalf("events = %s, want recover immediately before commit", events)
	}
}

// Intent (A11): a failing Recoverer keeps the record uncommitted; after the
// backoff only the Recoverer runs again, not the handlers, then the record is
// committed.
func TestConsumerRecovererFailureRetriesRecovererOnly(t *testing.T) {
	recoverCalls := 0
	h := newHarness(t, DropAndCommitPolicy(), WithRecoverer(func(context.Context, Error) error {
		recoverCalls++
		if recoverCalls == 1 {
			return errors.New("failed-messages table unavailable")
		}
		return nil
	}))
	record := testRecord(t, h.consumer.cfg, 0, 0)
	h.queue(fetchOf(record), fetchOf(record))
	h.client.pollHooks[2] = func() { h.resumes.fire(t, 0) }
	handlerCalls := 0
	h.subscribe(func(context.Context, message.Message) error {
		handlerCalls++
		return errors.New("fail")
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if handlerCalls != 1 || recoverCalls != 2 {
		t.Fatalf("handler calls = %d, recoverer calls = %d; want 1 and 2", handlerCalls, recoverCalls)
	}
	recoverFailures := h.observed.stage(StageRecover)
	if len(recoverFailures) != 1 || !errors.Is(recoverFailures[0].Err, ErrRecoverFailed) || recoverFailures[0].Attempt != 1 {
		t.Fatalf("recover failures = %+v, want one matching ErrRecoverFailed", recoverFailures)
	}
	if want := []time.Duration{100 * time.Millisecond}; !reflect.DeepEqual(h.resumes.delays(), want) {
		t.Fatalf("backoffs = %v, want %v", h.resumes.delays(), want)
	}
	if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
		t.Fatalf("commits = %v, want one commit after the recoverer succeeded", h.client.committedOffsets())
	}
}

// Intent (A12): Stop keeps the record in Kafka, so the Recoverer is not called.
func TestConsumerStopDoesNotCallRecoverer(t *testing.T) {
	recoverCalls := 0
	stop := FailurePolicyFunc(func(context.Context, Error) FailureAction { return FailureActionStop })
	h := newHarness(t, stop, WithRecoverer(func(context.Context, Error) error {
		recoverCalls++
		return nil
	}))
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	h.subscribe(func(context.Context, message.Message) error { return errors.New("fail") })

	if err := h.run(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Run error = %v, want ErrStopped", err)
	}
	if recoverCalls != 0 || len(h.client.commits) != 0 {
		t.Fatalf("recoverer calls = %d, commits = %d; want 0 and 0", recoverCalls, len(h.client.commits))
	}
}

// Intent (A13): a handler that overruns WithHandlerTimeout fails at
// StageHandle with ErrHandlerTimeout, whether it ignores ctx or returns its
// error, and the consumer moves on.
func TestConsumerHandlerTimeout(t *testing.T) {
	tests := []struct {
		name    string
		handler func(release <-chan struct{}) func(context.Context, message.Message) error
	}{
		{
			name: "handler ignores ctx",
			handler: func(release <-chan struct{}) func(context.Context, message.Message) error {
				return func(context.Context, message.Message) error {
					<-release
					return nil
				}
			},
		},
		{
			name: "handler returns ctx error",
			handler: func(<-chan struct{}) func(context.Context, message.Message) error {
				return func(ctx context.Context, _ message.Message) error {
					<-ctx.Done()
					return ctx.Err()
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			h := newHarness(t, DropAndCommitPolicy(), WithHandlerTimeout(20*time.Millisecond))
			h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
			h.subscribe(tt.handler(release))

			if err := h.run(); err != nil {
				t.Fatalf("Run error = %v", err)
			}

			failures := h.observed.stage(StageHandle)
			if len(failures) != 1 || !errors.Is(failures[0].Err, ErrHandlerTimeout) {
				t.Fatalf("failures = %+v, want one matching ErrHandlerTimeout", failures)
			}
			if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
				t.Fatalf("commits = %v, want %v", h.client.committedOffsets(), want)
			}
		})
	}
}

// Intent (A14): a handler cut short by shutdown is not a failure: the policy
// and observer are skipped and the record stays uncommitted for redelivery,
// with or without a handler timeout.
func TestConsumerShutdownDuringHandlerLeavesRecord(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Minute} {
		t.Run(timeout.String(), func(t *testing.T) {
			decided := 0
			policy := FailurePolicyFunc(func(context.Context, Error) FailureAction {
				decided++
				return FailureActionDrop
			})
			h := newHarness(t, policy, WithHandlerTimeout(timeout))
			h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
			h.subscribe(func(ctx context.Context, _ message.Message) error {
				h.cancel()
				<-ctx.Done()
				return ctx.Err()
			})

			if err := h.run(); err != nil {
				t.Fatalf("Run error = %v, want nil on shutdown", err)
			}

			if decided != 0 || len(h.observed.stage(StageHandle)) != 0 {
				t.Fatalf("policy calls = %d, handle observations = %d; want none", decided, len(h.observed.stage(StageHandle)))
			}
			if len(h.client.commits) != 0 {
				t.Fatalf("commits = %v, want none", h.client.committedOffsets())
			}
			if len(h.client.setOffsets) != 1 || h.client.setOffsets[0][testTopic][0].Offset != 0 {
				t.Fatalf("SetOffsets = %v, want rewind to offset 0", h.client.setOffsets)
			}
		})
	}
}

// Intent (A15): when ctx ends, each partition stops before its next record and
// the records already handled are still committed, on a ctx that is not
// cancelled.
func TestConsumerShutdownStopsPartitionAndCommitsHandled(t *testing.T) {
	h := newHarness(t, DropAndCommitPolicy())
	cfg := h.consumer.cfg
	h.queue(fetchOf(testRecord(t, cfg, 0, 0), testRecord(t, cfg, 0, 1), testRecord(t, cfg, 0, 2)))
	var handled []string
	h.subscribe(func(_ context.Context, msg message.Message) error {
		handled = append(handled, msg.ID())
		h.cancel()
		return nil
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if want := []string{"p0-o0"}; !reflect.DeepEqual(handled, want) {
		t.Fatalf("handled = %v, want %v", handled, want)
	}
	if want := [][]string{{"0@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
		t.Fatalf("commits = %v, want %v", h.client.committedOffsets(), want)
	}
	if err := h.client.commitCtxErrs[0]; err != nil {
		t.Fatalf("final commit ran on a cancelled ctx: %v", err)
	}
}

// Intent (A15b, found by TestIntegrationRebalanceDuringHandling): shutdown
// that arrives while a commit is in flight must not cancel that commit, or the
// handled records are silently left uncommitted.
func TestConsumerShutdownDuringCommitDoesNotCancelIt(t *testing.T) {
	h := newHarness(t, DropAndCommitPolicy())
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	h.client.commitHook = h.cancel
	h.subscribe(func(context.Context, message.Message) error { return nil })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if len(h.client.commitCtxErrs) != 1 || h.client.commitCtxErrs[0] != nil {
		t.Fatalf("commit ctx errors = %v, want the commit ctx to survive shutdown", h.client.commitCtxErrs)
	}
}

// Intent (A16): a panic in a partition goroutine is recovered as a StageHandle
// failure instead of crashing the process.
func TestConsumerRecoversHandlerPanic(t *testing.T) {
	h := newHarness(t, DropAndCommitPolicy())
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	h.subscribe(func(context.Context, message.Message) error { panic("boom") })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	failures := h.observed.stage(StageHandle)
	var panicErr *PanicError
	if len(failures) != 1 || !errors.Is(failures[0].Err, ErrHandlerPanic) || !errors.As(failures[0].Err, &panicErr) || panicErr.Value != "boom" {
		t.Fatalf("failures = %+v, want a recovered PanicError with value boom", failures)
	}
	if len(panicErr.Stack) == 0 {
		t.Fatal("PanicError.Stack is empty")
	}
}

// Intent: a handler that panics with an error keeps that error reachable.
func TestPanicErrorUnwrapsErrorValue(t *testing.T) {
	cause := errors.New("cause")
	err := error(&PanicError{Value: cause})
	if !errors.Is(err, ErrHandlerPanic) || !errors.Is(err, cause) {
		t.Fatalf("PanicError %v does not match ErrHandlerPanic and its cause", err)
	}
}

// Intent (A17): a failed commit stops Run and is observed.
func TestConsumerReturnsCommitFailure(t *testing.T) {
	h := newHarness(t, DropAndCommitPolicy())
	brokerErr := errors.New("coordinator unavailable")
	h.client.commitErr = brokerErr
	h.queue(fetchOf(testRecord(t, h.consumer.cfg, 0, 0)))
	h.subscribe(func(context.Context, message.Message) error { return nil })

	err := h.run()

	if !errors.Is(err, ErrCommitFailed) || !errors.Is(err, brokerErr) {
		t.Fatalf("Run error = %v, want ErrCommitFailed wrapping the broker error", err)
	}
	if got := h.observed.stage(StageCommit); len(got) != 1 {
		t.Fatalf("commit observations = %d, want 1", len(got))
	}
}

// Intent: a poll error with no records stops Run and is observed; every poll,
// including a failed one, lets the group rebalance again.
func TestConsumerPollErrorWithoutRecordsStopsRun(t *testing.T) {
	h := newHarness(t, DropAndCommitPolicy())
	fetchErr := errors.New("broker unreachable")
	h.queue(fetchError(fetchErr))
	h.subscribe(func(context.Context, message.Message) error { return nil })

	if err := h.run(); !errors.Is(err, fetchErr) {
		t.Fatalf("Run error = %v, want %v", err, fetchErr)
	}
	if got := h.observed.stage(StagePoll); len(got) != 1 {
		t.Fatalf("poll observations = %d, want 1", len(got))
	}
	if h.client.allowCount != h.client.pollCount {
		t.Fatalf("AllowRebalance calls = %d, polls = %d; every poll must allow rebalancing", h.client.allowCount, h.client.pollCount)
	}
}

// Intent: a poll takes at most WithMaxPollRecords records, DefaultMaxPollRecords
// by default, because the batch size bounds how long rebalances are blocked.
func TestConsumerPollsAtMostMaxPollRecords(t *testing.T) {
	for _, tt := range []struct {
		opts []Option
		want int
	}{
		{nil, DefaultMaxPollRecords},
		{[]Option{WithMaxPollRecords(8)}, 8},
		{[]Option{WithMaxPollRecords(-1)}, 0},
	} {
		h := newHarness(t, DropAndCommitPolicy(), tt.opts...)
		h.subscribe(func(context.Context, message.Message) error { return nil })
		if err := h.run(); err != nil {
			t.Fatalf("Run error = %v", err)
		}
		if h.client.pollMax[0] != tt.want {
			t.Fatalf("poll limit = %d, want %d", h.client.pollMax[0], tt.want)
		}
	}
}

// Intent: records in a partial fetch are handled even though another
// partition reported an error.
func TestConsumerHandlesRecordsFromMixedFetch(t *testing.T) {
	h := newHarness(t, DropAndCommitPolicy())
	partitionErr := errors.New("partition fetch failed")
	mixed := fetchOf(testRecord(t, h.consumer.cfg, 1, 0))
	mixed[0].Topics[0].Partitions = append(mixed[0].Topics[0].Partitions, kgo.FetchPartition{Partition: 0, Err: partitionErr})
	h.queue(mixed)
	h.subscribe(func(context.Context, message.Message) error { return nil })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if want := [][]string{{"1@0"}}; !reflect.DeepEqual(h.client.committedOffsets(), want) {
		t.Fatalf("commits = %v, want %v", h.client.committedOffsets(), want)
	}
	if got := h.observed.stage(StagePoll); len(got) == 0 || !errors.Is(got[0].Err, partitionErr) {
		t.Fatalf("poll observations = %+v, want the partition error first", got)
	}
}
