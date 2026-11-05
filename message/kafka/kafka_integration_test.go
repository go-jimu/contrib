//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"
)

type integrationHandler struct {
	kinds  []message.Kind
	handle func(context.Context, message.Message) error
}

type integrationRun struct {
	cancel context.CancelFunc
	errCh  chan error
	once   sync.Once
	err    error
}

func (h integrationHandler) Listening() []message.Kind {
	return h.kinds
}

func (h integrationHandler) Handle(ctx context.Context, msg message.Message) error {
	return h.handle(ctx, msg)
}

func startIntegrationRun(t *testing.T, consumer message.Runner) *integrationRun {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	run := &integrationRun{
		cancel: cancel,
		errCh:  make(chan error, 1),
	}
	go func() {
		run.errCh <- consumer.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		if err := run.wait(30 * time.Second); err != nil {
			t.Errorf("wait for consumer Run cleanup: %v", err)
		}
	})

	return run
}

func (r *integrationRun) stopAndWait(timeout time.Duration) error {
	r.cancel()
	return r.wait(timeout)
}

func (r *integrationRun) wait(timeout time.Duration) error {
	r.once.Do(func() {
		select {
		case r.err = <-r.errCh:
		case <-time.After(timeout):
			r.err = fmt.Errorf("timed out waiting for consumer Run to exit")
		}
	})
	return r.err
}

// integrationKafkaImage is the broker the Consumer integration tests run
// against. integrationShareKafkaImage runs share groups, which are generally
// available from Kafka 4.2.
const (
	integrationKafkaImage      = "apache/kafka:4.1.2"
	integrationShareKafkaImage = "apache/kafka:4.3.0"
)

// startKafka starts a single-node KRaft broker. The apache/kafka image
// advertises a fixed address, so the test picks a free host port first, has
// the broker listen on that same port inside the container, and advertises
// it. The broker must reach itself at the advertised address: its share-group
// state persister connects to it.
func startKafka(t *testing.T) []string {
	t.Helper()
	return startKafkaImage(t, integrationKafkaImage)
}

func startKafkaImage(t *testing.T, image string) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	port := freePort(t)
	containerPort := strconv.Itoa(port) + "/tcp"
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{containerPort},
			Env: map[string]string{
				"KAFKA_NODE_ID":                                          "1",
				"KAFKA_PROCESS_ROLES":                                    "broker,controller",
				"KAFKA_LISTENERS":                                        fmt.Sprintf("PLAINTEXT://:%d,CONTROLLER://:9093", port),
				"KAFKA_ADVERTISED_LISTENERS":                             fmt.Sprintf("PLAINTEXT://127.0.0.1:%d", port),
				"KAFKA_CONTROLLER_LISTENER_NAMES":                        "CONTROLLER",
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":                   "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                         "1@localhost:9093",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":                 "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":                    "1",
				"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR": "1",
				"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":                 "0",
				"KAFKA_NUM_PARTITIONS":                                   strconv.Itoa(integrationPartitions),
			},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{
					network.MustParsePort(containerPort): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}},
				}
			},
			WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() {
		terminateCtx, terminateCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer terminateCancel()
		if err := container.Terminate(terminateCtx); err != nil {
			t.Errorf("terminate kafka container: %v", err)
		}
	})
	return []string{fmt.Sprintf("127.0.0.1:%d", port)}
}

// integrationPartitions is the partition count of auto-created topics.
const integrationPartitions = 4

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func newIntegrationClient(t *testing.T, brokers []string, group string, topics ...string) *kgo.Client {
	t.Helper()

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	if group != "" {
		opts = append(opts, kgo.ConsumerGroup(group), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	}
	if len(topics) > 0 {
		opts = append(opts, kgo.ConsumeTopics(topics...))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("new kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func integrationPayloadResolver(message.Kind) (proto.Message, error) {
	return &testdata.TestModel{}, nil
}

// Intent: a message published through a real Kafka broker should be consumed with its kind, key, headers, and protobuf payload intact.
func TestIntegrationPublishAndConsumeMessage(t *testing.T) {
	brokers := startKafka(t)
	topic := uniqueIntegrationName("orders-paid")
	group := uniqueIntegrationName("orders-paid-group")
	client := newIntegrationClient(t, brokers, group, topic)
	consumer, err := NewConsumer(client, DropAndCommitPolicy(), WithPayloadResolverFunc(integrationPayloadResolver))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	publisher := NewPublisher(client)

	received := make(chan message.Message, 1)
	if err := consumer.Subscribe(integrationHandler{
		kinds: []message.Kind{message.Kind(topic)},
		handle: func(_ context.Context, msg message.Message) error {
			received <- msg
			return nil
		},
	}); err != nil {
		t.Fatalf("subscribe handler: %v", err)
	}
	run := startIntegrationRun(t, consumer)

	msg, err := message.New(
		message.Kind(topic),
		&testdata.TestModel{Id: 7, Name: "paid"},
		message.WithKey("order-7"),
		message.WithHeader("trace_id", "trace-1"),
	)
	if err != nil {
		t.Fatalf("new message: %v", err)
	}
	if err := publisher.Publish(context.Background(), msg); err != nil {
		t.Fatalf("publish message: %v", err)
	}

	var got message.Message
	select {
	case got = <-received:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for consumed message")
	}

	if got.Kind() != message.Kind(topic) {
		t.Fatalf("kind = %q, want %q", got.Kind(), topic)
	}
	if got.Key() != "order-7" {
		t.Fatalf("key = %q, want order-7", got.Key())
	}
	if got.Headers()["trace_id"] != "trace-1" {
		t.Fatalf("trace header = %q, want trace-1", got.Headers()["trace_id"])
	}
	payload, ok := got.Payload().(*testdata.TestModel)
	if !ok {
		t.Fatalf("payload type = %T, want *testdata.TestModel", got.Payload())
	}
	if payload.GetId() != 7 || payload.GetName() != "paid" {
		t.Fatalf("payload = {Id:%d Name:%q}, want {Id:7 Name:%q}", payload.GetId(), payload.GetName(), "paid")
	}

	if err := run.stopAndWait(30 * time.Second); err != nil {
		t.Fatalf("Run error = %v, want nil on ctx shutdown", err)
	}
}

func uniqueIntegrationName(prefix string) string {
	return fmt.Sprintf("jimu-it-%s-%d", prefix, time.Now().UnixNano())
}

func publishN(t *testing.T, publisher message.Publisher, kind message.Kind, ids []string, key func(i int) string) {
	t.Helper()
	for i, id := range ids {
		msg, err := message.New(kind, &testdata.TestModel{Id: int64(i)}, message.WithID(id), message.WithKey(key(i)))
		if err != nil {
			t.Fatalf("new message: %v", err)
		}
		if err := publisher.Publish(context.Background(), msg); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}
}

// Intent (I1): a member joining mid-stream triggers a rebalance while records
// are being handled; neither Run fails (no commit for a revoked partition),
// and every message is handled at least once.
func TestIntegrationRebalanceDuringHandling(t *testing.T) {
	brokers := startKafka(t)
	topic := uniqueIntegrationName("rebalance")
	group := uniqueIntegrationName("rebalance-group")
	kind := message.Kind(topic)

	const total = 80
	var mu sync.Mutex
	seen := map[string]bool{}
	handledBy := map[string]int{}
	newMember := func(name string) (*Consumer, *observed) {
		client := newIntegrationClient(t, brokers, group, topic)
		obs := &observed{}
		consumer, err := NewConsumer(client, RetryPolicy(RetryConfig{}),
			WithPayloadResolverFunc(integrationPayloadResolver),
			WithErrorObserver(obs.observe),
			// Small batches, so the rebalance is not held until member a
			// has handled everything.
			WithMaxPollRecords(8),
		)
		if err != nil {
			t.Fatalf("new consumer %s: %v", name, err)
		}
		if err := consumer.Subscribe(integrationHandler{
			kinds: []message.Kind{kind},
			handle: func(ctx context.Context, msg message.Message) error {
				select {
				case <-time.After(150 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
				mu.Lock()
				seen[msg.ID()] = true
				handledBy[name]++
				mu.Unlock()
				return nil
			},
		}); err != nil {
			t.Fatalf("subscribe %s: %v", name, err)
		}
		return consumer, obs
	}
	count := func() (int, map[string]int) {
		mu.Lock()
		defer mu.Unlock()
		return len(seen), maps.Clone(handledBy)
	}
	waitFor := func(cond func() bool, timeout time.Duration, what string) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for !cond() {
			if time.Now().After(deadline) {
				n, by := count()
				t.Fatalf("timed out waiting for %s: %d/%d handled, by member %v", what, n, total, by)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	ids := make([]string, total)
	for i := range ids {
		ids[i] = fmt.Sprintf("m-%03d", i)
	}
	producer := newIntegrationClient(t, brokers, "")
	publishN(t, NewPublisher(producer), kind, ids, func(i int) string { return fmt.Sprintf("key-%d", i) })

	consumerA, obsA := newMember("a")
	runA := startIntegrationRun(t, consumerA)
	waitFor(func() bool { n, _ := count(); return n >= 5 }, 60*time.Second, "member a to start handling")

	consumerB, obsB := newMember("b")
	runB := startIntegrationRun(t, consumerB)
	waitFor(func() bool { n, _ := count(); return n == total }, 120*time.Second, "all messages")

	errA, errB := runA.stopAndWait(30*time.Second), runB.stopAndWait(30*time.Second)
	if errA != nil || errB != nil {
		t.Fatalf("Run errors: a = %v, b = %v; want nil", errA, errB)
	}
	for name, obs := range map[string]*observed{"a": obsA, "b": obsB} {
		if commits := obs.stage(StageCommit); len(commits) != 0 {
			t.Fatalf("member %s commit failures: %v", name, commits[0].Err)
		}
	}
	if _, by := count(); by["b"] == 0 {
		t.Fatalf("member b handled nothing (%v); the rebalance did not happen mid-stream", by)
	}
}

// Intent (I2): a retried record is handled again in place after the backoff,
// before the next record of its partition, so the partition keeps its order.
func TestIntegrationRetryInPlaceKeepsOrderAndBackoff(t *testing.T) {
	brokers := startKafka(t)
	topic := uniqueIntegrationName("retry")
	group := uniqueIntegrationName("retry-group")
	kind := message.Kind(topic)
	const backoff = 500 * time.Millisecond

	client := newIntegrationClient(t, brokers, group, topic)
	consumer, err := NewConsumer(client, RetryPolicy(RetryConfig{}),
		WithPayloadResolverFunc(integrationPayloadResolver),
		WithRetryBackoff(func(int) time.Duration { return backoff }),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	type call struct {
		id string
		at time.Time
	}
	calls := make(chan call, 10)
	failedOnce := false
	if err := consumer.Subscribe(integrationHandler{
		kinds: []message.Kind{kind},
		handle: func(_ context.Context, msg message.Message) error {
			calls <- call{id: msg.ID(), at: time.Now()}
			if msg.ID() == "a" && !failedOnce {
				failedOnce = true
				return errors.New("database unavailable")
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Same key, so the same partition, in order a then b.
	publishN(t, NewPublisher(client), kind, []string{"a", "b"}, func(int) string { return "order-1" })
	run := startIntegrationRun(t, consumer)

	var got []call
	for len(got) < 3 {
		select {
		case c := <-calls:
			got = append(got, c)
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out; handled %v", got)
		}
	}
	if err := run.stopAndWait(30 * time.Second); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	ids := []string{got[0].id, got[1].id, got[2].id}
	if want := []string{"a", "a", "b"}; !slices.Equal(ids, want) {
		t.Fatalf("handling order = %v, want %v", ids, want)
	}
	if gap := got[1].at.Sub(got[0].at); gap < backoff {
		t.Fatalf("retry came %v after the failure, want at least %v", gap, backoff)
	}
}

// setShareGroupConfig sets a share-group config with IncrementalAlterConfigs
// on resource type GROUP (32).
func setShareGroupConfig(t *testing.T, client *kgo.Client, group, name, value string) {
	t.Helper()
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceType(32)
	resource.ResourceName = group
	config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	config.Name = name
	config.Op = kmsg.IncrementalAlterConfigOpSet
	config.Value = &value
	resource.Configs = append(resource.Configs, config)
	req.Resources = append(req.Resources, resource)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("alter share group config: %v", err)
	}
	for _, r := range resp.Resources {
		if err := kerrFor(r.ErrorCode); err != nil {
			t.Fatalf("alter share group config %s=%s: %v", name, value, err)
		}
	}
}

func kerrFor(code int16) error {
	if code == 0 {
		return nil
	}
	return fmt.Errorf("kafka error code %d", code)
}

// Intent (B integration): against a real share group, a released record is
// delivered again with its delivery count incremented, and a rejected record
// is not delivered again.
func TestIntegrationShareConsumerReleaseAndReject(t *testing.T) {
	brokers := startKafkaImage(t, integrationShareKafkaImage)
	topic := uniqueIntegrationName("share")
	group := uniqueIntegrationName("share-group")
	kind := message.Kind(topic)

	admin := newIntegrationClient(t, brokers, "")
	setShareGroupConfig(t, admin, group, "share.auto.offset.reset", "earliest")
	publishN(t, NewPublisher(admin), kind, []string{"retry", "reject"}, func(i int) string { return fmt.Sprintf("key-%d", i) })

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ShareGroup(group),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		t.Fatalf("new share client: %v", err)
	}
	t.Cleanup(client.Close)

	obs := &observed{}
	consumer, err := NewShareConsumer(client, ReleaseOrRejectPolicy(),
		WithPayloadResolverFunc(integrationPayloadResolver),
		WithErrorObserver(obs.observe),
	)
	if err != nil {
		t.Fatalf("new share consumer: %v", err)
	}
	var mu sync.Mutex
	calls := map[string]int{}
	retried := make(chan struct{})
	if err := consumer.Subscribe(integrationHandler{
		kinds: []message.Kind{kind},
		handle: func(_ context.Context, msg message.Message) error {
			mu.Lock()
			calls[msg.ID()]++
			n := calls[msg.ID()]
			mu.Unlock()
			switch {
			case msg.ID() == "reject":
				return NonRetryable(errors.New("bad input"))
			case n == 1:
				return errors.New("database unavailable")
			default:
				close(retried)
				return nil
			}
		},
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	run := startIntegrationRun(t, consumer)

	select {
	case <-retried:
	case <-time.After(60 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		obs.mu.Lock()
		defer obs.mu.Unlock()
		t.Fatalf("released record was not delivered again; calls %v; observed %+v", calls, obs.errors)
	}
	// Give the broker a chance to wrongly redeliver the rejected record.
	time.Sleep(3 * time.Second)
	if err := run.stopAndWait(30 * time.Second); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls["retry"] != 2 || calls["reject"] != 1 {
		t.Fatalf("handler calls = %v, want retry 2, reject 1", calls)
	}
	byID := map[string]Error{}
	for _, f := range obs.stage(StageHandle) {
		byID[f.Message.ID()] = f
	}
	if f := byID["retry"]; f.Attempt != 1 || f.Final {
		t.Fatalf("retry failure = Attempt %d Final %v, want first delivery, not final", f.Attempt, f.Final)
	}
	if f := byID["reject"]; !f.Final || f.Attempt != 1 {
		t.Fatalf("reject failure = Attempt %d Final %v, want final first delivery", f.Attempt, f.Final)
	}
}
