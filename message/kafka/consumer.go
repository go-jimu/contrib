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
	client pollClient
	cfg    config
	router *message.Router
}

func NewConsumer(client *kgo.Client, opts ...Option) *Consumer {
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
	return newConsumer(consumer, cfg)
}

func newConsumer(client pollClient, cfg config) *Consumer {
	return &Consumer{
		client: client,
		cfg:    cfg,
		router: message.NewRouter(),
	}
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
		for _, fetchErr := range fetches.Errors() {
			err := fetchErr.Err
			if handlerErr := c.cfg.errorHandler(ctx, Error{
				Stage: StagePoll,
				Err:   err,
			}); handlerErr != nil {
				return handlerErr
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		}

		for iter := fetches.RecordIter(); !iter.Done(); {
			if err := c.processRecord(ctx, iter.Next()); err != nil {
				return err
			}
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
	decision := failureAction(failure, c.cfg)
	if decision.err != nil {
		handlerErr := c.cfg.errorHandler(ctx, Error{
			Stage:   failure.Stage,
			Record:  failure.Record,
			Message: failure.Message,
			Err:     decision.err,
		})
		if handlerErr != nil {
			return handlerErr
		}
		return decision.err
	}

	if decision.kind == actionNone {
		if err := c.cfg.errorHandler(ctx, failure); err != nil {
			return err
		}
		return c.commit(ctx, failure.Record)
	}

	failureRecord := buildFailureRecord(failure, decision, c.cfg)
	if failureRecord == nil {
		err := fmt.Errorf("build failure record: %w", ErrNoTopic)
		if handlerErr := c.cfg.errorHandler(ctx, Error{
			Stage:   failure.Stage,
			Record:  failure.Record,
			Message: failure.Message,
			Err:     err,
		}); handlerErr != nil {
			return handlerErr
		}
		return err
	}

	if err := c.client.ProduceSync(ctx, failureRecord).FirstErr(); err != nil {
		stage, wrappedErr := publishFailure(decision.kind, err)
		handlerErr := c.cfg.errorHandler(ctx, Error{
			Stage:   stage,
			Record:  failure.Record,
			Message: failure.Message,
			Err:     wrappedErr,
		})
		if handlerErr != nil {
			return handlerErr
		}
		return wrappedErr
	}

	return c.commit(ctx, failure.Record)
}

func (c *Consumer) commit(ctx context.Context, record *kgo.Record) error {
	if err := c.client.CommitRecords(ctx, record); err != nil {
		wrappedErr := fmt.Errorf("%w: %w", ErrCommitFailed, err)
		handlerErr := c.cfg.errorHandler(ctx, Error{
			Stage:  StageCommit,
			Record: record,
			Err:    wrappedErr,
		})
		if handlerErr != nil {
			return handlerErr
		}
		return wrappedErr
	}
	return nil
}

func publishFailure(kind failureActionKind, err error) (Stage, error) {
	switch kind {
	case actionRetry:
		return StageRetryPublish, fmt.Errorf("%w: %w", ErrRetryPublishFailed, err)
	case actionDLQ:
		return StageDLQPublish, fmt.Errorf("%w: %w", ErrDLQPublishFailed, err)
	default:
		return StageHandle, err
	}
}
