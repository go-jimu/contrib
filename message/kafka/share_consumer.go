package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ShareAction is a ShareFailurePolicy decision for one failed record, sent to
// the broker as the record's acknowledgement.
type ShareAction int

const (
	// ShareActionRelease hands the record back to the broker, which delivers
	// it again, possibly to another member, with its delivery count
	// incremented. Past the group's share.delivery.count.limit the
	// broker archives it, or writes it to the group's dead-letter topic when
	// one is configured (KIP-1191).
	ShareActionRelease ShareAction = iota
	// ShareActionReject marks the record as unprocessable: the broker
	// archives it, or writes it to the dead-letter topic, without delivering
	// it again.
	ShareActionReject
	// ShareActionAccept acknowledges the record as if it was handled: the
	// failure is dropped and the message is lost.
	ShareActionAccept
)

// ShareFailurePolicy decides how a message-level ShareConsumer failure is
// acknowledged: StageDecode, StageUnhandled, or StageHandle, while Run's ctx
// is live. Error.Attempt is the broker's delivery count of the record. Decide
// runs concurrently and must be safe for concurrent use. An unknown
// ShareAction counts as ShareActionRelease.
type ShareFailurePolicy interface {
	Decide(context.Context, Error) ShareAction
}

// ShareFailurePolicyFunc adapts a function into a ShareFailurePolicy.
type ShareFailurePolicyFunc func(context.Context, Error) ShareAction

// Decide calls f(ctx, failure).
func (f ShareFailurePolicyFunc) Decide(ctx context.Context, failure Error) ShareAction {
	return f(ctx, failure)
}

// ReleaseOrRejectPolicy releases a failure DefaultRetryable retries, so the
// broker delivers it again until its delivery limit, and rejects the others:
// decode and unhandled failures, and errors matching ErrNonRetryable.
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

// ShareConsumer consumes records with a franz-go share-group client (KIP-932,
// Kafka 4.2+), decodes them into message.Message values, routes them to
// subscribed handlers, and acknowledges each record to the broker. It
// implements message.Subscriber and message.Runner.
//
// Unlike Consumer, it does not keep any order: the records of a poll are
// handled concurrently, and the broker may deliver records of one partition
// to several members. Retries and dead letters are the broker's: a released
// record is delivered again with its delivery count incremented, and a
// rejected record, or one past the group's delivery limit, is archived or
// written to the group's dead-letter topic when the broker supports KIP-1191
// and the group is configured for it. The consumer itself produces nothing.
// Choose it when messages need no order and a native dead-letter queue is
// wanted.
type ShareConsumer struct {
	dispatcher

	client shareClient
	policy ShareFailurePolicy

	// ack and deliveryCount are kgo.Record methods, replaced in tests.
	ack           func(*kgo.Record, kgo.AckStatus)
	deliveryCount func(*kgo.Record) int32
}

// NewShareConsumer returns a ShareConsumer for client, which must be
// configured with kgo.ShareGroup and kgo.ConsumeTopics. policy decides
// message-level failures. WithPayloadResolver is required to decode payloads.
//
// It honors WithCodec, WithKindResolver, WithPayloadResolver, WithHeaderNames,
// WithErrorObserver, WithHandlerTimeout, WithMaxPollRecords, WithConcurrency,
// and WithCloseClient; the Consumer's retry and recovery options do not apply.
//
// It returns ErrNilFailurePolicy for a nil policy, ErrNoPayloadResolver
// without WithPayloadResolver, and an error matching ErrInvalidClient when the
// client has no share group. A nil client is reported later, by Run, as
// ErrNilClient.
func NewShareConsumer(client *kgo.Client, policy ShareFailurePolicy, opts ...Option) (*ShareConsumer, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	var consumer shareClient
	if client != nil {
		if group, _ := client.OptValue(kgo.ShareGroup).(string); group == "" {
			return nil, fmt.Errorf("%w: missing kgo.ShareGroup", ErrInvalidClient)
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
		dispatcher:    newDispatcher(cfg),
		client:        client,
		policy:        policy,
		ack:           (*kgo.Record).Ack,
		deliveryCount: (*kgo.Record).DeliveryCount,
	}, nil
}

// Close closes the franz-go client when WithCloseClient(true) was set, and
// does nothing otherwise. Call it after Run has returned.
func (c *ShareConsumer) Close() {
	if c.cfg.closeClient && c.client != nil {
		c.client.Close()
	}
}

// Run consumes records until ctx ends, then returns nil. It returns an error
// when the consumer cannot continue: ErrNilClient, or a poll error with no
// records.
//
// Each poll's records are handled concurrently, at most WithConcurrency at a
// time, and every record is acknowledged before the next poll: Accept when
// its handlers succeed, otherwise what the ShareFailurePolicy decides. Keep
// handlers within the group's acquisition lock
// (group.share.record.lock.duration.ms, 30s by default) with
// WithHandlerTimeout, or the broker delivers the record again meanwhile.
//
// When ctx ends, records not yet handled, and records whose handlers were cut
// short, are released for redelivery without the policy or the observer, and
// Run flushes the acknowledgements on a ctx that is not cancelled. Call Run
// from one goroutine only.
func (c *ShareConsumer) Run(ctx context.Context) error {
	if c.client == nil {
		return ErrNilClient
	}
	defer c.flush(ctx)

	err := c.run(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return nil
	}
	return err
}

func (c *ShareConsumer) run(ctx context.Context) error {
	for {
		fetches := c.client.PollRecords(ctx, c.cfg.maxPollRecords)

		var pollErr error
		for _, fetchErr := range fetches.Errors() {
			err := fetchErr.Err
			c.observe(ctx, Error{Stage: StagePoll, Err: err})
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if pollErr == nil {
				pollErr = err
			}
		}

		records := fetches.Records()
		if len(records) == 0 {
			if pollErr != nil {
				return pollErr
			}
			continue
		}
		c.processBatch(ctx, records)
		if err := ctx.Err(); err != nil {
			return err
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
	msg, stage, err := c.dispatch(ctx, record)
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
	failure.Final = action != ShareActionRelease
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
