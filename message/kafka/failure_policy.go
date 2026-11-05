package kafka

import (
	"context"
	"errors"
)

// FailureAction is a FailurePolicy decision for one failed record.
type FailureAction int

const (
	// FailureActionDrop gives up on the record: the consumer calls the
	// Recoverer, when one is set, then commits the offset and moves on.
	FailureActionDrop FailureAction = iota
	// FailureActionRetry handles the record again in place: the consumer
	// leaves it uncommitted, rewinds its partition to it, and pauses the
	// partition for the WithRetryBackoff delay. Later records of the
	// partition wait, so the partition keeps its order.
	FailureActionRetry
	// FailureActionStop leaves the record uncommitted and makes Run return an
	// error matching ErrStopped and the failure's Err.
	FailureActionStop
)

// FailurePolicy decides how a message-level consumer failure should be
// handled: StageDecode, StageUnhandled, or StageHandle, while Run's ctx is
// live. Error.Attempt counts the failed attempts at the record so far,
// including this one. Decide runs concurrently for different partitions, so it
// must be safe for concurrent use; return quickly. An unknown FailureAction
// counts as FailureActionDrop. The package documentation compares the
// built-in policies.
type FailurePolicy interface {
	Decide(context.Context, Error) FailureAction
}

// FailurePolicyFunc adapts a function into a FailurePolicy.
type FailurePolicyFunc func(context.Context, Error) FailureAction

// Decide calls f(ctx, failure).
func (f FailurePolicyFunc) Decide(ctx context.Context, failure Error) FailureAction {
	return f(ctx, failure)
}

// DropAndCommitPolicy drops every message-level failure: the Recoverer, when
// set, gets the record, then the offset is committed. Without a Recoverer the
// failed message is lost.
func DropAndCommitPolicy() FailurePolicy {
	return FailurePolicyFunc(func(context.Context, Error) FailureAction {
		return FailureActionDrop
	})
}

// RetryConfig configures RetryPolicy.
type RetryConfig struct {
	// MaxAttempts is the number of attempts at a record, counting the first;
	// 0 or less means 3.
	MaxAttempts int
	// Retryable reports whether a failure may be retried. nil means
	// DefaultRetryable.
	Retryable func(Error) bool
	// OnExhausted is the action for a failure that is not retryable or has
	// used MaxAttempts: FailureActionDrop (the zero value) or
	// FailureActionStop. FailureActionRetry counts as FailureActionDrop.
	OnExhausted FailureAction
}

// DefaultRetryable retries StageHandle failures, including a handler timeout
// (ErrHandlerTimeout) and a recovered panic (ErrHandlerPanic), unless the
// error matches ErrNonRetryable. Decode and unhandled failures are not
// retried: the same bytes and handlers fail the same way again.
func DefaultRetryable(failure Error) bool {
	return failure.Stage == StageHandle && !errors.Is(failure.Err, ErrNonRetryable)
}

// RetryPolicy retries Retryable failures in place until the record has had
// MaxAttempts attempts, then applies OnExhausted. A failure that is not
// Retryable gets OnExhausted at once, without using attempts.
func RetryPolicy(cfg RetryConfig) FailurePolicy {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.Retryable == nil {
		cfg.Retryable = DefaultRetryable
	}
	if cfg.OnExhausted != FailureActionStop {
		cfg.OnExhausted = FailureActionDrop
	}

	return FailurePolicyFunc(func(_ context.Context, failure Error) FailureAction {
		if cfg.Retryable(failure) && failure.Attempt < cfg.MaxAttempts {
			return FailureActionRetry
		}
		return cfg.OnExhausted
	})
}
