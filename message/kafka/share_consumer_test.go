package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeShareClient serves queued fetches and cancels Run's ctx when they run
// out, like fakeConsumerClient.
type fakeShareClient struct {
	t         *testing.T
	mu        sync.Mutex
	fetches   []kgo.Fetches
	pollHooks map[int]func()
	cancel    context.CancelFunc
	flushCtxs []error
	flushErr  error
	closed    bool
	pollCount int
	pollMax   []int
	// acks and lastPolled check that every record of a poll is acknowledged
	// before the next poll: the real client auto-accepts the rest.
	acks       *ackRecorder
	lastPolled []*kgo.Record
}

func (f *fakeShareClient) PollRecords(ctx context.Context, maxRecords int) kgo.Fetches {
	f.mu.Lock()
	f.pollCount++
	f.pollMax = append(f.pollMax, maxRecords)
	hook := f.pollHooks[f.pollCount]
	f.checkAcked()
	f.mu.Unlock()
	if hook != nil {
		hook()
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.fetches) == 0 {
		f.cancel()
		return fetchError(ctx.Err())
	}
	fetches := f.fetches[0]
	f.fetches = f.fetches[1:]
	f.lastPolled = fetches.Records()
	return fetches
}

func (f *fakeShareClient) checkAcked() {
	for _, r := range f.lastPolled {
		if _, ok := f.acks.get(recordID(r.Partition, r.Offset)); !ok {
			f.t.Errorf("%s was not acknowledged before the next poll; the client would auto-accept it", recordID(r.Partition, r.Offset))
		}
	}
	f.lastPolled = nil
}

func (f *fakeShareClient) FlushAcks(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkAcked()
	f.flushCtxs = append(f.flushCtxs, ctx.Err())
	return f.flushErr
}

func (f *fakeShareClient) Close() { f.closed = true }

// ackRecorder records the acknowledgement of each record and flags a record
// acknowledged twice.
type ackRecorder struct {
	t    *testing.T
	mu   sync.Mutex
	acks map[string]kgo.AckStatus
}

func (a *ackRecorder) ack(r *kgo.Record, status kgo.AckStatus) {
	a.mu.Lock()
	defer a.mu.Unlock()
	id := recordID(r.Partition, r.Offset)
	if prev, ok := a.acks[id]; ok {
		a.t.Errorf("%s acknowledged twice: %v then %v", id, prev, status)
	}
	a.acks[id] = status
}

func (a *ackRecorder) get(id string) (kgo.AckStatus, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	status, ok := a.acks[id]
	return status, ok
}

type shareHarness struct {
	t        *testing.T
	ctx      context.Context
	cancel   context.CancelFunc
	client   *fakeShareClient
	acks     *ackRecorder
	observed *observed
	consumer *ShareConsumer
}

func newShareHarness(t *testing.T, policy ShareFailurePolicy, opts ...Option) *shareHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := &shareHarness{
		t:        t,
		ctx:      ctx,
		cancel:   cancel,
		acks:     &ackRecorder{t: t, acks: map[string]kgo.AckStatus{}},
		observed: &observed{},
	}
	h.client = &fakeShareClient{t: t, cancel: cancel, acks: h.acks, pollHooks: map[int]func(){}}
	cfg := testConsumerConfig()
	WithErrorObserver(h.observed.observe)(&cfg)
	for _, opt := range opts {
		opt(&cfg)
	}
	consumer, err := newShareConsumer(h.client, policy, cfg)
	if err != nil {
		t.Fatalf("newShareConsumer: %v", err)
	}
	consumer.ack = h.acks.ack
	consumer.deliveryCount = func(*kgo.Record) int32 { return 1 }
	consumer.acquisitionDeadline = func(*kgo.Record) time.Time { return time.Time{} }
	h.consumer = consumer
	return h
}

func (h *shareHarness) subscribe(fn func(context.Context, message.Message) error) {
	h.t.Helper()
	if err := h.consumer.Subscribe(funcHandler{fn: fn}); err != nil {
		h.t.Fatalf("Subscribe: %v", err)
	}
}

func (h *shareHarness) run() error {
	h.t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- h.consumer.Run(h.ctx) }()
	select {
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Second):
		h.t.Fatal("Run did not return within 5s")
		return nil
	}
}

// Intent (B1): a ShareConsumer needs a share-group client.
func TestNewShareConsumerValidatesClient(t *testing.T) {
	resolver := WithPayloadResolver(testConsumerConfig().payloadResolver)

	plain, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer plain.Close()
	if _, err := NewShareConsumer(plain, ReleaseOrRejectPolicy(), resolver); !errors.Is(err, ErrInvalidClient) || !strings.Contains(err.Error(), "kgo.ShareGroup") {
		t.Fatalf("NewShareConsumer error = %v, want ErrInvalidClient naming kgo.ShareGroup", err)
	}

	share, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.ShareGroup("g"), kgo.ConsumeTopics(testTopic))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer share.Close()
	if _, err := NewShareConsumer(share, ReleaseOrRejectPolicy(), resolver); err != nil {
		t.Fatalf("NewShareConsumer error = %v, want nil", err)
	}
	if _, err := NewShareConsumer(share, nil, resolver); !errors.Is(err, ErrNilFailurePolicy) {
		t.Fatalf("nil policy error = %v, want ErrNilFailurePolicy", err)
	}
}

// Intent (B2, B6): each outcome maps to the acknowledgement the broker acts
// on; only a release is not final, and Attempt is the delivery count.
func TestShareConsumerAckMapping(t *testing.T) {
	accept := ShareFailurePolicyFunc(func(context.Context, Error) ShareAction { return ShareActionAccept })
	tests := []struct {
		name    string
		policy  ShareFailurePolicy
		record  func(*testing.T, config) *kgo.Record
		handler error
		want    kgo.AckStatus
		stage   Stage
		final   bool
	}{
		{name: "success", want: kgo.AckAccept},
		{name: "handler error", handler: errors.New("database unavailable"), want: kgo.AckRelease, stage: StageHandle},
		{name: "non-retryable", handler: NonRetryable(errors.New("bad input")), want: kgo.AckReject, stage: StageHandle, final: true},
		{
			name: "decode failure",
			record: func(t *testing.T, cfg config) *kgo.Record {
				r := testRecord(t, cfg, 0, 0)
				r.Headers = nil
				return r
			},
			want: kgo.AckReject, stage: StageDecode, final: true,
		},
		{
			name: "unhandled kind",
			record: func(t *testing.T, cfg config) *kgo.Record {
				return testKindRecord(t, cfg, "order.payment.v1.Unknown", 0, 0)
			},
			want: kgo.AckReject, stage: StageUnhandled, final: true,
		},
		{name: "policy accepts failure", policy: accept, handler: errors.New("ignored"), want: kgo.AckAccept, stage: StageHandle, final: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := tt.policy
			if policy == nil {
				policy = ReleaseOrRejectPolicy()
			}
			h := newShareHarness(t, policy)
			h.consumer.deliveryCount = func(*kgo.Record) int32 { return 3 }
			record := testRecord(t, h.consumer.cfg, 0, 0)
			if tt.record != nil {
				record = tt.record(t, h.consumer.cfg)
			}
			h.client.fetches = []kgo.Fetches{fetchOf(record)}
			h.subscribe(func(context.Context, message.Message) error { return tt.handler })

			if err := h.run(); err != nil {
				t.Fatalf("Run error = %v", err)
			}

			if got, _ := h.acks.get("p0-o0"); got != tt.want {
				t.Fatalf("ack = %v, want %v", got, tt.want)
			}
			if tt.stage == "" {
				return
			}
			failures := h.observed.stage(tt.stage)
			if len(failures) != 1 || failures[0].Final != tt.final || failures[0].Attempt != 3 {
				t.Fatalf("%s failures = %+v, want one with Final %v and Attempt 3", tt.stage, failures, tt.final)
			}
		})
	}
}

// Intent (B3): records are handled concurrently, never more than
// WithConcurrency at once, and every record is acknowledged. All handlers
// block on a gate: two must start, and a third must not start within a short
// observation window while they hold their slots.
func TestShareConsumerConcurrencyLimit(t *testing.T) {
	h := newShareHarness(t, ReleaseOrRejectPolicy(), WithConcurrency(2))
	cfg := h.consumer.cfg
	h.client.fetches = []kgo.Fetches{fetchOf(
		testRecord(t, cfg, 0, 0), testRecord(t, cfg, 0, 1), testRecord(t, cfg, 1, 0), testRecord(t, cfg, 1, 1),
	)}
	started := make(chan struct{}, 4)
	gate := make(chan struct{})
	h.subscribe(func(context.Context, message.Message) error {
		started <- struct{}{}
		<-gate
		return nil
	})

	go func() {
		defer close(gate)
		for range 2 {
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Error("two handlers did not start concurrently")
				return
			}
		}
		select {
		case <-started:
			t.Error("a third handler started while two held the concurrency slots")
		case <-time.After(100 * time.Millisecond):
		}
	}()
	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	for _, id := range []string{"p0-o0", "p0-o1", "p1-o0", "p1-o1"} {
		if got, ok := h.acks.get(id); !ok || got != kgo.AckAccept {
			t.Fatalf("%s ack = %v (set %v), want accept", id, got, ok)
		}
	}
}

// Intent (B4): a handler timeout is a retryable failure, so it is released.
func TestShareConsumerHandlerTimeoutReleases(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	h := newShareHarness(t, ReleaseOrRejectPolicy(), WithHandlerTimeout(20*time.Millisecond))
	h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, h.consumer.cfg, 0, 0))}
	h.subscribe(func(context.Context, message.Message) error {
		<-release
		return nil
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if got, _ := h.acks.get("p0-o0"); got != kgo.AckRelease {
		t.Fatalf("ack = %v, want release", got)
	}
	if failures := h.observed.stage(StageHandle); len(failures) != 1 || !errors.Is(failures[0].Err, ErrHandlerTimeout) {
		t.Fatalf("failures = %+v, want one matching ErrHandlerTimeout", failures)
	}
}

// Intent (B5): at shutdown, records not started and records cut short are
// released without the policy, so the broker redelivers them; the handled
// record is accepted and acknowledgements are flushed on a live ctx.
func TestShareConsumerShutdownReleasesAndFlushes(t *testing.T) {
	decided := 0
	policy := ShareFailurePolicyFunc(func(context.Context, Error) ShareAction {
		decided++
		return ShareActionReject
	})
	h := newShareHarness(t, policy, WithConcurrency(1))
	cfg := h.consumer.cfg
	h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, cfg, 0, 0), testRecord(t, cfg, 0, 1), testRecord(t, cfg, 0, 2))}
	h.subscribe(func(ctx context.Context, msg message.Message) error {
		switch msg.ID() {
		case "p0-o0":
			return nil
		case "p0-o1":
			h.cancel()
			return ctx.Err()
		}
		t.Errorf("%s was handled after shutdown", msg.ID())
		return nil
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	want := map[string]kgo.AckStatus{"p0-o0": kgo.AckAccept, "p0-o1": kgo.AckRelease, "p0-o2": kgo.AckRelease}
	for id, status := range want {
		if got, ok := h.acks.get(id); !ok || got != status {
			t.Fatalf("%s ack = %v (set %v), want %v", id, got, ok, status)
		}
	}
	if decided != 0 || len(h.observed.stage(StageHandle)) != 0 {
		t.Fatalf("policy calls = %d, observations = %d; want none at shutdown", decided, len(h.observed.stage(StageHandle)))
	}
	if len(h.client.flushCtxs) != 1 || h.client.flushCtxs[0] != nil {
		t.Fatalf("FlushAcks ctx errors = %v, want one flush on a live ctx", h.client.flushCtxs)
	}
}

// Intent: a poll error with no records stops Run, and pending
// acknowledgements are still flushed.
func TestShareConsumerPollErrorFlushes(t *testing.T) {
	h := newShareHarness(t, ReleaseOrRejectPolicy())
	fetchErr := errors.New("broker unreachable")
	h.client.fetches = []kgo.Fetches{fetchError(fetchErr)}
	h.subscribe(func(context.Context, message.Message) error { return nil })

	if err := h.run(); !errors.Is(err, fetchErr) {
		t.Fatalf("Run error = %v, want %v", err, fetchErr)
	}
	if len(h.client.flushCtxs) != 1 {
		t.Fatalf("FlushAcks calls = %d, want 1", len(h.client.flushCtxs))
	}
}

// Intent (review #1): a record whose acquisition lock has run out, or nearly,
// is released without running the handlers: the broker may already have
// handed it to another member.
func TestShareConsumerReleasesRecordPastAcquisitionDeadline(t *testing.T) {
	h := newShareHarness(t, ReleaseOrRejectPolicy())
	h.consumer.acquisitionDeadline = func(*kgo.Record) time.Time { return time.Now().Add(time.Second) }
	h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, h.consumer.cfg, 0, 0))}
	handled := 0
	h.subscribe(func(context.Context, message.Message) error { handled++; return nil })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if got, _ := h.acks.get("p0-o0"); got != kgo.AckRelease || handled != 0 {
		t.Fatalf("ack = %v, handled = %d; want release without handling", got, handled)
	}
}

// Intent (review #1): the handlers' ctx ends before the acquisition lock
// does, even without WithHandlerTimeout, so the ack still reaches the broker
// in time.
func TestShareConsumerHandlerCtxEndsBeforeAcquisitionDeadline(t *testing.T) {
	h := newShareHarness(t, ReleaseOrRejectPolicy())
	deadline := time.Now().Add(shareLockMargin + 50*time.Millisecond)
	h.consumer.acquisitionDeadline = func(*kgo.Record) time.Time { return deadline }
	h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, h.consumer.cfg, 0, 0))}
	h.subscribe(func(ctx context.Context, _ message.Message) error {
		<-ctx.Done()
		return ctx.Err()
	})

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	failures := h.observed.stage(StageHandle)
	if len(failures) != 1 || !errors.Is(failures[0].Err, ErrHandlerTimeout) {
		t.Fatalf("failures = %+v, want one matching ErrHandlerTimeout", failures)
	}
}

// Intent (review #1): by default a ShareConsumer polls one record per
// concurrent handler, so no record waits in a batch while its lock runs.
func TestShareConsumerDefaultBatchIsConcurrency(t *testing.T) {
	for _, tt := range []struct {
		opts []Option
		want int
	}{
		{[]Option{WithConcurrency(3)}, 3},
		{[]Option{WithConcurrency(3), WithMaxPollRecords(50)}, 50},
	} {
		h := newShareHarness(t, ReleaseOrRejectPolicy(), tt.opts...)
		h.subscribe(func(context.Context, message.Message) error { return nil })
		if err := h.run(); err != nil {
			t.Fatalf("Run error = %v", err)
		}
		if h.client.pollMax[0] != tt.want {
			t.Fatalf("poll limit = %d, want %d", h.client.pollMax[0], tt.want)
		}
	}
}

// Intent (review #4): records polled together with an error, when ctx ends
// meanwhile, are released rather than left for the next poll to auto-accept.
func TestShareConsumerReleasesPolledRecordsWhenShutdownMeetsPollError(t *testing.T) {
	h := newShareHarness(t, ReleaseOrRejectPolicy())
	mixed := fetchOf(testRecord(t, h.consumer.cfg, 1, 7))
	mixed[0].Topics[0].Partitions = append(mixed[0].Topics[0].Partitions, kgo.FetchPartition{Partition: 0, Err: errors.New("not leader")})
	h.client.fetches = []kgo.Fetches{mixed}
	h.client.pollHooks[1] = h.cancel
	h.subscribe(func(context.Context, message.Message) error { return nil })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if got, ok := h.acks.get("p1-o7"); !ok || got != kgo.AckRelease {
		t.Fatalf("ack = %v (set %v), want release", got, ok)
	}
}

// Intent (review #5): a release on the last delivery the broker allows is
// final: the broker archives the record next.
func TestShareConsumerReleaseAtDeliveryLimitIsFinal(t *testing.T) {
	for _, tt := range []struct {
		opts  []Option
		count int32
		final bool
	}{
		{nil, 4, false},
		{nil, 5, true}, // Kafka's default share.delivery.count.limit
		{[]Option{WithDeliveryLimit(8)}, 5, false},
		{[]Option{WithDeliveryLimit(8)}, 8, true},
	} {
		h := newShareHarness(t, ReleaseOrRejectPolicy(), tt.opts...)
		h.consumer.deliveryCount = func(*kgo.Record) int32 { return tt.count }
		h.client.fetches = []kgo.Fetches{fetchOf(testRecord(t, h.consumer.cfg, 0, 0))}
		h.subscribe(func(context.Context, message.Message) error { return errors.New("database unavailable") })

		if err := h.run(); err != nil {
			t.Fatalf("Run error = %v", err)
		}
		failures := h.observed.stage(StageHandle)
		if len(failures) != 1 || failures[0].Final != tt.final {
			t.Fatalf("delivery %d, opts %d: failures = %+v, want Final %v", tt.count, len(tt.opts), failures, tt.final)
		}
	}
}

// Intent (review #6, #7): a shutdown is not observed as a poll failure, and
// informational poll errors do not stop Run.
func TestShareConsumerPollErrorHandling(t *testing.T) {
	h := newShareHarness(t, ReleaseOrRejectPolicy())
	h.client.fetches = []kgo.Fetches{
		fetchError(&kgo.ErrGroupSession{Err: errors.New("session lost")}),
		fetchOf(testRecord(t, h.consumer.cfg, 0, 0)),
	}
	h.subscribe(func(context.Context, message.Message) error { return nil })

	if err := h.run(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if got := h.observed.stage(StagePoll); len(got) != 1 || errors.Is(got[0].Err, context.Canceled) {
		t.Fatalf("poll observations = %+v, want only the group session error", got)
	}
	if got, _ := h.acks.get("p0-o0"); got != kgo.AckAccept {
		t.Fatalf("ack = %v, want accept", got)
	}
}
