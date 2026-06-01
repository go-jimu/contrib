package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerClient interface {
	producerClient
	CommitRecords(context.Context, ...*kgo.Record) error
	Close()
}

type pollClient interface {
	consumerClient
	PollFetches(context.Context) kgo.Fetches
}

type Consumer struct {
	client        pollClient
	failurePolicy FailurePolicy
	cfg           config
	router        *message.Router
}

func NewConsumer(client *kgo.Client, policy FailurePolicy, opts ...Option) (*Consumer, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	var consumer pollClient
	if client != nil {
		consumer = client
	}
	return newConsumer(consumer, policy, cfg)
}

func newConsumer(client pollClient, policy FailurePolicy, cfg config) (*Consumer, error) {
	if policy == nil {
		return nil, ErrNilFailurePolicy
	}
	return &Consumer{
		client:        client,
		failurePolicy: policy,
		cfg:           cfg,
		router:        message.NewRouter(),
	}, nil
}

func (c *Consumer) Subscribe(handler message.Handler) error {
	return c.router.Subscribe(handler)
}

func (c *Consumer) Close() {
	if c.cfg.closeClient && c.client != nil {
		c.client.Close()
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.client == nil {
		return ErrNilClient
	}

	for {
		fetches := c.client.PollFetches(ctx)
		var pollErr error
		for _, fetchErr := range fetches.Errors() {
			err := fetchErr.Err
			c.observe(ctx, Error{
				Stage: StagePoll,
				Err:   err,
			})
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

		processed := 0
		for iter := fetches.RecordIter(); !iter.Done(); {
			processed++
			if err := c.processRecord(ctx, iter.Next()); err != nil {
				return err
			}
		}
		if pollErr != nil && processed == 0 {
			return pollErr
		}
	}
}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) error {
	if c.client == nil {
		return ErrNilClient
	}

	msg, err := recordToMessage(record, c.cfg)
	if err != nil {
		return c.handleFailure(ctx, Error{
			Stage:  StageDecode,
			Record: record,
			Err:    err,
		})
	}

	if err = c.router.Handle(ctx, msg); err != nil {
		stage := StageHandle
		if errors.Is(err, message.ErrUnhandledKind) {
			stage = StageUnhandled
		}
		return c.handleFailure(ctx, Error{
			Stage:   stage,
			Record:  record,
			Message: msg,
			Err:     err,
		})
	}

	return c.commit(ctx, record)
}

func (c *Consumer) handleFailure(ctx context.Context, failure Error) error {
	c.observe(ctx, failure)
	decision := failureAction(ctx, failure, c.failurePolicy, c.cfg)
	if decision.err != nil {
		return c.handleFailurePipelineError(ctx, failure, decision.action, decision.err)
	}

	if decision.action == FailureActionDrop {
		return c.commit(ctx, failure.Record)
	}

	failureRecord := buildFailureRecord(failure, decision, c.cfg)
	if failureRecord == nil {
		err := fmt.Errorf("build failure record: %w", ErrNoTopic)
		return c.handleFailurePipelineError(ctx, failure, decision.action, err)
	}

	if err := c.client.ProduceSync(ctx, failureRecord).FirstErr(); err != nil {
		return c.handleFailurePipelineError(ctx, failure, decision.action, err)
	}

	return c.commit(ctx, failure.Record)
}

func (c *Consumer) handleFailurePipelineError(ctx context.Context, failure Error, action FailureAction, err error) error {
	stage, wrappedErr := publishFailure(action, err)
	c.observe(ctx, Error{
		Stage:   stage,
		Record:  failure.Record,
		Message: failure.Message,
		Err:     wrappedErr,
	})
	if c.cfg.failureFallback == FailurePublishFallbackDrop {
		return c.commit(ctx, failure.Record)
	}
	return wrappedErr
}

func (c *Consumer) commit(ctx context.Context, record *kgo.Record) error {
	if err := c.client.CommitRecords(ctx, record); err != nil {
		wrappedErr := fmt.Errorf("%w: %w", ErrCommitFailed, err)
		c.observe(ctx, Error{
			Stage:  StageCommit,
			Record: record,
			Err:    wrappedErr,
		})
		return wrappedErr
	}
	return nil
}

func (c *Consumer) observe(ctx context.Context, failure Error) {
	if c.cfg.errorObserver != nil {
		c.cfg.errorObserver(ctx, failure)
	}
}

func publishFailure(action FailureAction, err error) (Stage, error) {
	switch action {
	case FailureActionRetry:
		return StageRetryPublish, fmt.Errorf("%w: %w", ErrRetryPublishFailed, err)
	case FailureActionDLQ:
		return StageDLQPublish, fmt.Errorf("%w: %w", ErrDLQPublishFailed, err)
	default:
		return StageHandle, err
	}
}
