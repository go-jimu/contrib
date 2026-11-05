package kafka

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ShareAction is a ShareFailurePolicy decision for one failed record, sent to
// the broker as the record's acknowledgement.
type ShareAction int

const (
	// ShareActionRelease hands the record back for the broker to deliver
	// again, possibly to another member, up to the group's delivery limit.
	ShareActionRelease ShareAction = iota
	// ShareActionReject marks the record unprocessable: the broker archives
	// it or writes it to the group's dead-letter topic.
	ShareActionReject
	// ShareActionAccept acknowledges the record as if it was handled: the
	// failure is dropped and the message is lost.
	ShareActionAccept
)

// ShareFailurePolicy decides how each message-level ShareConsumer failure
// (StageDecode, StageUnhandled, or StageHandle) is acknowledged while Run's
// ctx is live. Error.Attempt is the record's delivery count. An unknown
// ShareAction counts as ShareActionRelease. See Share groups, and the
// concurrency contract in Handlers, in the package documentation.
type ShareFailurePolicy interface {
	Decide(context.Context, Error) ShareAction
}

// ShareFailurePolicyFunc adapts a function into a ShareFailurePolicy.
type ShareFailurePolicyFunc func(context.Context, Error) ShareAction

// Decide calls f(ctx, failure).
func (f ShareFailurePolicyFunc) Decide(ctx context.Context, failure Error) ShareAction {
	return f(ctx, failure)
}

// ReleaseOrRejectPolicy releases a failure DefaultRetryable accepts and
// rejects the others.
func ReleaseOrRejectPolicy() ShareFailurePolicy {
	return ShareFailurePolicyFunc(func(_ context.Context, failure Error) ShareAction {
		if DefaultRetryable(failure) {
			return ShareActionRelease
		}
		return ShareActionReject
	})
}

type shareClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	FlushAcks(context.Context) error
	Close()
}

// ShareConsumer consumes a share group (KIP-932, Kafka 4.2+) in no particular
// order and acknowledges each record to the broker, which owns retries and
// dead letters. It implements message.Subscriber and message.Runner. See
// Share groups in the package documentation.
type ShareConsumer struct {
	dispatcher

	client shareClient
	policy ShareFailurePolicy

	// ack, deliveryCount, and acquisitionDeadline are kgo.Record methods,
	// replaced in tests.
	ack                 func(*kgo.Record, kgo.AckStatus)
	deliveryCount       func(*kgo.Record) int32
	acquisitionDeadline func(*kgo.Record) time.Time
}

// shareLockMargin is how long before its acquisition deadline a record is
// treated as expired: the deadline is computed on the broker before the fetch
// is sent, and the ack still has to travel back.
const shareLockMargin = 2 * time.Second

// NewShareConsumer returns a ShareConsumer for client, which needs
// kgo.ShareGroup and kgo.ConsumeTopics. policy decides message-level
// failures, and WithPayloadResolver is required.
//
// It honors WithCodec, WithKindResolver, WithPayloadResolver, WithHeaderNames,
// WithErrorObserver, WithHandlerTimeout, WithMaxPollRecords, WithConcurrency,
// WithDeliveryLimit, and WithCloseClient; the Consumer's retry and recovery
// options do not apply.
//
// It returns ErrNilFailurePolicy for a nil policy, ErrNoPayloadResolver
// without WithPayloadResolver, and an error matching ErrInvalidClient for a
// client without kgo.ShareGroup or kgo.ConsumeTopics. Run reports a nil
// client as ErrNilClient.
func NewShareConsumer(client *kgo.Client, policy ShareFailurePolicy, opts ...Option) (*ShareConsumer, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	var consumer shareClient
	if client != nil {
		var missing []string
		if group, _ := client.OptValue(kgo.ShareGroup).(string); group == "" {
			missing = append(missing, "kgo.ShareGroup")
		}
		if !consumesTopics(client) {
			missing = append(missing, "kgo.ConsumeTopics")
		}
		if len(missing) > 0 {
			return nil, fmt.Errorf("%w: missing %s", ErrInvalidClient, strings.Join(missing, ", "))
		}
		consumer = client
	}
	return newShareConsumer(consumer, policy, cfg)
}

func newShareConsumer(client shareClient, policy ShareFailurePolicy, cfg config) (*ShareConsumer, error) {
	if policy == nil {
		return nil, ErrNilFailurePolicy
	}
	if cfg.payloadResolver == nil {
		return nil, ErrNoPayloadResolver
	}
	return &ShareConsumer{
		dispatcher:          newDispatcher(cfg),
		client:              client,
		policy:              policy,
		ack:                 (*kgo.Record).Ack,
		deliveryCount:       (*kgo.Record).DeliveryCount,
		acquisitionDeadline: (*kgo.Record).AcquisitionDeadline,
	}, nil
}

// Close closes the franz-go client when WithCloseClient(true) was set, and
// does nothing otherwise. Call it after Run has returned.
func (c *ShareConsumer) Close() {
	if c.cfg.closeClient && c.client != nil {
		c.client.Close()
	}
}

// Run consumes batches until ctx ends, then returns nil. It returns an error
// when the consumer cannot continue: ErrNilClient, or a poll error that stops
// it. Call Run from one goroutine only, and once per client. See Share
// groups, Poll errors, and Shutdown in the package documentation.
func (c *ShareConsumer) Run(ctx context.Context) error {
	if c.client == nil {
		return ErrNilClient
	}
	defer c.flush(ctx)

	for {
		fetches := c.client.PollRecords(ctx, c.batchSize())
		pollErr := c.pollError(ctx, fetches)
		// processBatch acknowledges every record, releasing them when ctx
		// has ended, so the next poll auto-accepts nothing.
		if records := fetches.Records(); len(records) > 0 {
			c.processBatch(ctx, records)
		} else if pollErr != nil && ctx.Err() == nil {
			return pollErr
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// processBatch acknowledges every record exactly once: the share client
// accepts any record left unacknowledged at the next poll, which would lose
// it.
func (c *ShareConsumer) processBatch(ctx context.Context, records []*kgo.Record) {
	sem := make(chan struct{}, c.concurrency())
	var wg sync.WaitGroup
	for _, record := range records {
		acquired := false
		select {
		case sem <- struct{}{}:
			acquired = true
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			if acquired {
				<-sem
			}
			c.ack(record, kgo.AckRelease)
			continue
		}
		wg.Go(func() {
			defer func() { <-sem }()
			c.ack(record, c.processRecord(ctx, record))
		})
	}
	wg.Wait()
}

func (c *ShareConsumer) processRecord(ctx context.Context, record *kgo.Record) kgo.AckStatus {
	timeout, expired := c.handlerTimeout(record)
	if expired {
		// The broker may already have handed the record to another member.
		return kgo.AckRelease
	}
	msg, stage, err := c.dispatch(ctx, record, timeout)
	if err == nil {
		return kgo.AckAccept
	}
	if ctx.Err() != nil {
		// Shutdown, not a failure of the record: deliver it again.
		return kgo.AckRelease
	}

	failure := Error{
		Stage:   stage,
		Record:  record,
		Message: msg,
		Err:     err,
		Attempt: int(c.deliveryCount(record)),
	}
	action := c.policy.Decide(ctx, failure)
	failure.Final = action != ShareActionRelease ||
		(c.cfg.deliveryLimit > 0 && failure.Attempt >= c.cfg.deliveryLimit)
	c.observe(ctx, failure)

	switch action {
	case ShareActionReject:
		return kgo.AckReject
	case ShareActionAccept:
		return kgo.AckAccept
	default:
		return kgo.AckRelease
	}
}

// handlerTimeout bounds the handlers of record by WithHandlerTimeout and by
// its acquisition deadline, less shareLockMargin. It reports expired when the
// deadline leaves no time.
func (c *ShareConsumer) handlerTimeout(record *kgo.Record) (time.Duration, bool) {
	timeout := c.cfg.handlerTimeout
	deadline := c.acquisitionDeadline(record)
	if deadline.IsZero() {
		return timeout, false
	}
	left := time.Until(deadline) - shareLockMargin
	if left <= 0 {
		return 0, true
	}
	if timeout <= 0 || left < timeout {
		timeout = left
	}
	return timeout, false
}

// batchSize is WithMaxPollRecords, or by default one record per concurrent
// handler, so no record waits in a batch while its acquisition lock runs.
func (c *ShareConsumer) batchSize() int {
	if c.cfg.maxPollRecordsSet {
		return c.cfg.maxPollRecords
	}
	return c.concurrency()
}

func (c *ShareConsumer) concurrency() int {
	if c.cfg.concurrency > 0 {
		return c.cfg.concurrency
	}
	return runtime.GOMAXPROCS(0)
}

// flush sends the pending acknowledgements, so a shutdown does not leave the
// last batch to the acquisition lock timeout.
func (c *ShareConsumer) flush(ctx context.Context) {
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.commitTimeout)
	defer cancel()
	if err := c.client.FlushAcks(flushCtx); err != nil {
		c.observe(ctx, Error{Stage: StageCommit, Err: fmt.Errorf("%w: flush acks: %w", ErrCommitFailed, err)})
	}
}
