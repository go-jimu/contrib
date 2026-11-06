package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const testTopic = "orders"

const testKind message.Kind = "order.payment.v1.OrderPaid"

// fakeConsumerClient is a pollClient that serves queued fetches and records
// every call in order. When the queue is empty it cancels Run's ctx through
// cancel, so a test ends after its last fetch.
type fakeConsumerClient struct {
	mu        sync.Mutex
	fetches   []kgo.Fetches
	pollHooks map[int]func()
	cancel    context.CancelFunc
	commitErr error
	// commitHook runs inside CommitRecords, before the ctx is inspected.
	commitHook func()
	// polled receives the ctx of the first poll, so a test can wait for
	// Run's stop.
	polled chan context.Context

	pollCount     int
	pollMax       []int
	events        []string
	commits       [][]*kgo.Record
	commitCtxErrs []error
	setOffsets    []map[string]map[int32]kgo.EpochOffset
	allowCount    int
	closed        bool
}

func (f *fakeConsumerClient) event(e string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
}

func (f *fakeConsumerClient) PollRecords(ctx context.Context, maxRecords int) kgo.Fetches {
	f.mu.Lock()
	f.pollCount++
	f.pollMax = append(f.pollMax, maxRecords)
	hook := f.pollHooks[f.pollCount]
	if f.polled != nil && f.pollCount == 1 {
		f.polled <- ctx
	}
	f.mu.Unlock()
	if hook != nil {
		hook()
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "poll")
	if len(f.fetches) == 0 {
		if f.cancel != nil {
			f.cancel()
		}
		if ctx.Err() != nil {
			return fetchError(ctx.Err())
		}
		return fetchError(context.Canceled)
	}
	fetches := f.fetches[0]
	f.fetches = f.fetches[1:]
	return fetches
}

func (f *fakeConsumerClient) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	if f.commitHook != nil {
		f.commitHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "commit")
	f.commits = append(f.commits, records)
	f.commitCtxErrs = append(f.commitCtxErrs, ctx.Err())
	return f.commitErr
}

func (f *fakeConsumerClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "setoffsets")
	f.setOffsets = append(f.setOffsets, offsets)
}

func (f *fakeConsumerClient) PauseFetchPartitions(tps map[string][]int32) map[string][]int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	for topic, partitions := range tps {
		for _, p := range partitions {
			f.events = append(f.events, fmt.Sprintf("pause:%s/%d", topic, p))
		}
	}
	return tps
}

func (f *fakeConsumerClient) ResumeFetchPartitions(tps map[string][]int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for topic, partitions := range tps {
		for _, p := range partitions {
			f.events = append(f.events, fmt.Sprintf("resume:%s/%d", topic, p))
		}
	}
}

func (f *fakeConsumerClient) AllowRebalance() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowCount++
	f.events = append(f.events, "allow")
}

func (f *fakeConsumerClient) Close() {
	f.closed = true
}

// committedOffsets flattens every commit into "partition@offset" strings.
func (f *fakeConsumerClient) committedOffsets() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]string
	for _, commit := range f.commits {
		var batch []string
		for _, r := range commit {
			batch = append(batch, fmt.Sprintf("%d@%d", r.Partition, r.Offset))
		}
		out = append(out, batch)
	}
	return out
}

// scheduledResume is one call to the consumer's afterFunc.
type scheduledResume struct {
	delay time.Duration
	fn    func()
}

// resumeRecorder replaces time.AfterFunc so a test fires resumes itself.
type resumeRecorder struct {
	mu        sync.Mutex
	scheduled []scheduledResume
}

func (r *resumeRecorder) afterFunc(delay time.Duration, fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scheduled = append(r.scheduled, scheduledResume{delay: delay, fn: fn})
}

func (r *resumeRecorder) delays() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []time.Duration
	for _, s := range r.scheduled {
		out = append(out, s.delay)
	}
	return out
}

func (r *resumeRecorder) fire(t *testing.T, i int) {
	t.Helper()
	r.mu.Lock()
	if i >= len(r.scheduled) {
		r.mu.Unlock()
		t.Fatalf("resume %d was not scheduled; scheduled %d", i, len(r.scheduled))
	}
	fn := r.scheduled[i].fn
	r.mu.Unlock()
	fn()
}

// funcHandler handles testKind (or kinds) with fn.
type funcHandler struct {
	kinds []message.Kind
	fn    func(context.Context, message.Message) error
}

func (h funcHandler) Listening() []message.Kind {
	if len(h.kinds) == 0 {
		return []message.Kind{testKind}
	}
	return h.kinds
}

func (h funcHandler) Handle(ctx context.Context, msg message.Message) error {
	return h.fn(ctx, msg)
}

// observed collects ErrorObserver calls safely.
type observed struct {
	mu     sync.Mutex
	errors []Error
}

func (o *observed) observe(_ context.Context, failure Error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.errors = append(o.errors, failure)
}

func (o *observed) stage(stage Stage) []Error {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []Error
	for _, e := range o.errors {
		if e.Stage == stage {
			out = append(out, e)
		}
	}
	return out
}

func testConsumerConfig() config {
	cfg := defaultConfig()
	WithPayloadResolverFunc(func(message.Kind) (proto.Message, error) {
		return &testdata.TestModel{}, nil
	})(&cfg)
	return cfg
}

// testRecord returns a record of testKind on testTopic whose message ID is
// "p<partition>-o<offset>", so a handler can tell records apart.
func testRecord(t *testing.T, cfg config, partition int32, offset int64) *kgo.Record {
	t.Helper()
	return testKindRecord(t, cfg, testKind, partition, offset)
}

func testKindRecord(t *testing.T, cfg config, kind message.Kind, partition int32, offset int64) *kgo.Record {
	t.Helper()
	msg, err := message.New(kind, testPayload(t),
		message.WithID(recordID(partition, offset)),
		message.WithKey("order-1"),
	)
	if err != nil {
		t.Fatalf("message.New: %v", err)
	}
	record, err := messageToRecord(msg, cfg)
	if err != nil {
		t.Fatalf("messageToRecord: %v", err)
	}
	record.Topic = testTopic
	record.Partition = partition
	record.Offset = offset
	record.LeaderEpoch = 3
	return record
}

func recordID(partition int32, offset int64) string {
	return fmt.Sprintf("p%d-o%d", partition, offset)
}

// fetchOf groups records by partition into one fetch, in the order given.
func fetchOf(records ...*kgo.Record) kgo.Fetches {
	topic := kgo.FetchTopic{Topic: testTopic}
	index := map[int32]int{}
	for _, r := range records {
		i, ok := index[r.Partition]
		if !ok {
			i = len(topic.Partitions)
			index[r.Partition] = i
			topic.Partitions = append(topic.Partitions, kgo.FetchPartition{Partition: r.Partition})
		}
		topic.Partitions[i].Records = append(topic.Partitions[i].Records, r)
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{topic}}}
}

func fetchError(err error) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      testTopic,
		Partitions: []kgo.FetchPartition{{Partition: 0, Err: err}},
	}}}}
}

// testHarness wires a Consumer to a fake client with a controllable resume
// clock and an observer.
type testHarness struct {
	t        *testing.T
	ctx      context.Context
	cancel   context.CancelFunc
	client   *fakeConsumerClient
	resumes  *resumeRecorder
	observed *observed
	consumer *Consumer
}

func newHarness(t *testing.T, policy FailurePolicy, opts ...Option) *testHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := &testHarness{
		t:        t,
		ctx:      ctx,
		cancel:   cancel,
		client:   &fakeConsumerClient{cancel: cancel, pollHooks: map[int]func(){}},
		resumes:  &resumeRecorder{},
		observed: &observed{},
	}
	cfg := testConsumerConfig()
	WithErrorObserver(h.observed.observe)(&cfg)
	WithRetryBackoff(func(attempt int) time.Duration {
		return time.Duration(attempt) * 100 * time.Millisecond
	})(&cfg)
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.afterFunc = h.resumes.afterFunc
	consumer, err := newConsumer(h.client, policy, cfg)
	if err != nil {
		t.Fatalf("newConsumer: %v", err)
	}
	h.consumer = consumer
	return h
}

func (h *testHarness) subscribe(fn func(context.Context, message.Message) error) {
	h.t.Helper()
	if err := h.consumer.Subscribe(funcHandler{fn: fn}); err != nil {
		h.t.Fatalf("Subscribe: %v", err)
	}
}

func (h *testHarness) queue(fetches ...kgo.Fetches) {
	h.client.fetches = append(h.client.fetches, fetches...)
}

// run runs the consumer until the fetch queue drains, failing the test if it
// does not return in time.
func (h *testHarness) run() error {
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
