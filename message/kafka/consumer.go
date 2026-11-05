package kafka

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type pollClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	PauseFetchPartitions(map[string][]int32) map[string][]int32
	ResumeFetchPartitions(map[string][]int32)
	AllowRebalance()
	Close()
}

// Consumer consumes a consumer group, keeping each partition in order, and
// commits offsets. It implements message.Subscriber and message.Runner. See
// Consumer failures, Recoverer, and Consumer offsets and rebalances in the
// package documentation.
type Consumer struct {
	dispatcher

	client        pollClient
	failurePolicy FailurePolicy

	mu    sync.Mutex
	heads map[topicPartition]*headState
}

type topicPartition struct {
	topic     string
	partition int32
}

// headState tracks the failing record at the head of a partition. A partition
// handles its records in order and stops at a failure, so at most one record
// per partition has state; a different head offset means the record was
// settled elsewhere, as after a rebalance, and the state no longer applies.
type headState struct {
	offset          int64
	attempts        int
	recoverAttempts int
	recoverPending  bool
	failure         Error
}

// NewConsumer returns a Consumer for client, which needs kgo.ConsumerGroup,
// kgo.ConsumeTopics, kgo.DisableAutoCommit, and kgo.BlockRebalanceOnPoll.
// policy decides message-level failures, and WithPayloadResolver is required.
//
// It returns ErrNilFailurePolicy for a nil policy, ErrNoPayloadResolver
// without WithPayloadResolver, and an error matching ErrInvalidClient naming
// each missing client option. Run reports a nil client as ErrNilClient.
// NewConsumer starts nothing.
func NewConsumer(client *kgo.Client, policy FailurePolicy, opts ...Option) (*Consumer, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	var consumer pollClient
	if client != nil {
		if err := validateGroupClient(client); err != nil {
			return nil, err
		}
		consumer = client
	}
	return newConsumer(consumer, policy, cfg)
}

func validateGroupClient(client *kgo.Client) error {
	var missing []string
	if group, _ := client.OptValue(kgo.ConsumerGroup).(string); group == "" {
		missing = append(missing, "kgo.ConsumerGroup")
	}
	if disabled, _ := client.OptValue(kgo.DisableAutoCommit).(bool); !disabled {
		missing = append(missing, "kgo.DisableAutoCommit")
	}
	if block, _ := client.OptValue(kgo.BlockRebalanceOnPoll).(bool); !block {
		missing = append(missing, "kgo.BlockRebalanceOnPoll")
	}
	if !consumesTopics(client) {
		missing = append(missing, "kgo.ConsumeTopics")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing %s", ErrInvalidClient, strings.Join(missing, ", "))
	}
	return nil
}

// consumesTopics reports whether client was given kgo.ConsumeTopics.
// OptValue returns the topics as a map[string]*regexp.Regexp, as of franz-go
// v1.22.2 and its master branch, although its doc comment says []string; the
// slice case keeps the check working should the code follow the doc.
func consumesTopics(client *kgo.Client) bool {
	switch topics := client.OptValue(kgo.ConsumeTopics).(type) {
	case map[string]*regexp.Regexp:
		return len(topics) > 0
	case []string:
		return len(topics) > 0
	default:
		return false
	}
}

func newConsumer(client pollClient, policy FailurePolicy, cfg config) (*Consumer, error) {
	if policy == nil {
		return nil, ErrNilFailurePolicy
	}
	if cfg.payloadResolver == nil {
		return nil, ErrNoPayloadResolver
	}
	return &Consumer{
		dispatcher:    newDispatcher(cfg),
		client:        client,
		failurePolicy: policy,
		heads:         make(map[topicPartition]*headState),
	}, nil
}

// Close closes the franz-go client when WithCloseClient(true) was set, and
// does nothing otherwise. Call it after Run has returned.
func (c *Consumer) Close() {
	if c.cfg.closeClient && c.client != nil {
		c.client.Close()
	}
}

// Run consumes batches until ctx ends, then returns nil. It returns an error
// when the consumer cannot continue: ErrNilClient, a poll error that stops it,
// an error matching ErrCommitFailed, or one matching ErrStopped when the
// FailurePolicy chose FailureActionStop. Call Run from one goroutine only, and
// once per client. See Consumer offsets and rebalances, Poll errors, and
// Shutdown in the package documentation.
func (c *Consumer) Run(ctx context.Context) error {
	if c.client == nil {
		return ErrNilClient
	}
	for {
		shutdown, err := c.pollOnce(ctx)
		if err != nil || shutdown {
			return err
		}
	}
}

// pollOnce handles one poll and reports whether ctx has ended. It always
// lets the group rebalance before it returns, after the batch is committed
// and rewound. Records polled when ctx has already ended are rewound by
// processBatch, not dropped.
func (c *Consumer) pollOnce(ctx context.Context) (bool, error) {
	fetches := c.client.PollRecords(ctx, c.cfg.maxPollRecords)
	defer c.client.AllowRebalance()

	pollErr := c.pollError(ctx, fetches)
	if batches := splitByPartition(fetches); len(batches) > 0 {
		if err := c.processBatch(ctx, batches); err != nil {
			return true, err
		}
	} else if pollErr != nil && ctx.Err() == nil {
		return true, pollErr
	}
	return ctx.Err() != nil, nil
}

type partitionBatch struct {
	tp      topicPartition
	records []*kgo.Record
}

func splitByPartition(fetches kgo.Fetches) []partitionBatch {
	var batches []partitionBatch
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		batches = append(batches, partitionBatch{
			tp:      topicPartition{topic: p.Topic, partition: p.Partition},
			records: p.Records,
		})
	})
	return batches
}

// partitionResult is the outcome of one partition's share of a batch.
type partitionResult struct {
	// done is the last record handled to completion, in an unbroken run from
	// the start of the batch; nil when the first record is not done.
	done *kgo.Record
	// next is the first record not done; nil when all are done.
	next *kgo.Record
	// delay pauses the partition before next is handled again; 0 means no
	// pause.
	delay time.Duration
	// err stops Run.
	err error
}

func (c *Consumer) processBatch(ctx context.Context, batches []partitionBatch) error {
	results := make([]partitionResult, len(batches))
	var stopping atomic.Bool
	var wg sync.WaitGroup
	for i, batch := range batches {
		wg.Go(func() {
			results[i] = c.processPartition(ctx, batch, &stopping)
		})
	}
	wg.Wait()

	return c.settleBatch(ctx, batches, results)
}

func (c *Consumer) processPartition(ctx context.Context, batch partitionBatch, stopping *atomic.Bool) partitionResult {
	var result partitionResult
	for _, record := range batch.records {
		if ctx.Err() != nil || stopping.Load() {
			result.next = record
			return result
		}
		outcome := c.processRecord(ctx, batch.tp, record)
		switch outcome.kind {
		case outcomeDone:
			result.done = record
			continue
		case outcomeStop:
			stopping.Store(true)
			result.err = outcome.err
		}
		result.next = record
		result.delay = outcome.delay
		return result
	}
	return result
}

// settleBatch commits the handled prefix of every partition, then rewinds and
// pauses the partitions with records left. It runs before AllowRebalance, so
// the partitions are still assigned.
func (c *Consumer) settleBatch(ctx context.Context, batches []partitionBatch, results []partitionResult) error {
	var (
		done     []*kgo.Record
		pause    = map[string][]int32{}
		rewind   = map[string]map[int32]kgo.EpochOffset{}
		resumes  []func()
		runErr   error
		hasPause bool
	)
	for i, result := range results {
		if result.done != nil {
			done = append(done, result.done)
		}
		if result.next != nil {
			tp := batches[i].tp
			if rewind[tp.topic] == nil {
				rewind[tp.topic] = map[int32]kgo.EpochOffset{}
			}
			rewind[tp.topic][tp.partition] = kgo.EpochOffset{
				Epoch:  result.next.LeaderEpoch,
				Offset: result.next.Offset,
			}
			if result.delay > 0 {
				hasPause = true
				pause[tp.topic] = append(pause[tp.topic], tp.partition)
				delay := result.delay
				resumes = append(resumes, func() {
					c.cfg.afterFunc(delay, func() {
						c.client.ResumeFetchPartitions(map[string][]int32{tp.topic: {tp.partition}})
					})
				})
			}
		}
		if runErr == nil && result.err != nil {
			runErr = result.err
		}
	}

	commitErr := c.commit(ctx, done)

	// Pause before rewinding, so the client fetches nothing for a paused
	// partition from the rewound offset until its backoff ends.
	if hasPause {
		c.client.PauseFetchPartitions(pause)
	}
	if len(rewind) > 0 {
		c.client.SetOffsets(rewind)
	}
	for _, resume := range resumes {
		resume()
	}

	if commitErr != nil {
		return commitErr
	}
	return runErr
}

func (c *Consumer) commit(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	// Shutdown must not abort a commit, neither the last one nor one that is
	// in flight when ctx ends: the records are handled and must not be
	// handled again. The commit is bounded by its own timeout instead.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.commitTimeout)
	defer cancel()
	if err := c.client.CommitRecords(commitCtx, records...); err != nil {
		wrappedErr := fmt.Errorf("%w: %w", ErrCommitFailed, err)
		c.observe(ctx, Error{Stage: StageCommit, Record: records[0], Err: wrappedErr})
		return wrappedErr
	}
	return nil
}

type outcomeKind int

const (
	// outcomeDone: the record is settled and may be committed.
	outcomeDone outcomeKind = iota
	// outcomeLater: leave the record for a later poll, after delay.
	outcomeLater
	// outcomeStop: leave the record and stop Run with err.
	outcomeStop
)

type recordOutcome struct {
	kind  outcomeKind
	delay time.Duration
	err   error
}

func (c *Consumer) processRecord(ctx context.Context, tp topicPartition, record *kgo.Record) recordOutcome {
	head := c.head(tp, record.Offset)
	if head != nil && head.recoverPending {
		return c.recover(ctx, tp, head, head.failure)
	}

	msg, stage, err := c.dispatch(ctx, record, c.cfg.handlerTimeout)
	if err == nil {
		if head != nil {
			c.clearHead(tp)
		}
		return recordOutcome{kind: outcomeDone}
	}

	// Once Run's ctx has ended, leave the record undecided and uncommitted so
	// Kafka redelivers it: the failure may stem from the ended ctx.
	if ctx.Err() != nil {
		return recordOutcome{kind: outcomeLater}
	}

	if head == nil {
		head = c.newHead(tp, record.Offset)
	}
	head.attempts++
	failure := Error{
		Stage:   stage,
		Record:  record,
		Message: msg,
		Err:     err,
		Attempt: head.attempts,
	}
	action := c.failurePolicy.Decide(ctx, failure)
	if action != FailureActionRetry && action != FailureActionStop {
		action = FailureActionDrop
	}
	failure.Final = action != FailureActionRetry
	c.observe(ctx, failure)

	switch action {
	case FailureActionRetry:
		return recordOutcome{kind: outcomeLater, delay: c.backoff(head.attempts)}
	case FailureActionStop:
		return recordOutcome{kind: outcomeStop, err: fmt.Errorf("%w: %w", ErrStopped, err)}
	default:
		return c.recover(ctx, tp, head, failure)
	}
}

// recover hands a dropped record to the Recoverer, if any. The record is done
// only when the Recoverer returns nil.
func (c *Consumer) recover(ctx context.Context, tp topicPartition, head *headState, failure Error) recordOutcome {
	if c.cfg.recoverer == nil {
		c.clearHead(tp)
		return recordOutcome{kind: outcomeDone}
	}
	err := c.cfg.recoverer(ctx, failure)
	if err == nil {
		c.clearHead(tp)
		return recordOutcome{kind: outcomeDone}
	}
	head.recoverPending = true
	head.failure = failure
	if ctx.Err() != nil {
		return recordOutcome{kind: outcomeLater}
	}
	head.recoverAttempts++
	c.observe(ctx, Error{
		Stage:   StageRecover,
		Record:  failure.Record,
		Message: failure.Message,
		Err:     fmt.Errorf("%w: %w", ErrRecoverFailed, err),
		Attempt: head.recoverAttempts,
	})
	return recordOutcome{kind: outcomeLater, delay: c.backoff(head.recoverAttempts)}
}

func (c *Consumer) backoff(attempt int) time.Duration {
	delay := c.cfg.retryBackoff(attempt)
	if delay <= 0 {
		// A partition must still be rewound and refetched; a zero delay
		// pauses for no time.
		return time.Nanosecond
	}
	return delay
}

// head returns the state of the failing record at offset, or nil when the
// partition has none for it.
func (c *Consumer) head(tp topicPartition, offset int64) *headState {
	c.mu.Lock()
	defer c.mu.Unlock()
	head := c.heads[tp]
	if head == nil {
		return nil
	}
	if head.offset != offset {
		delete(c.heads, tp)
		return nil
	}
	return head
}

func (c *Consumer) newHead(tp topicPartition, offset int64) *headState {
	c.mu.Lock()
	defer c.mu.Unlock()
	head := &headState{offset: offset}
	c.heads[tp] = head
	return head
}

func (c *Consumer) clearHead(tp topicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.heads, tp)
}
