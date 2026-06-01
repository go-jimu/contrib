package kafka

import "context"

type FailureAction int

const (
	// FailureActionDrop commits the source offset without producing a retry or DLQ record.
	FailureActionDrop FailureAction = iota
	// FailureActionRetry publishes the source record to the configured retry topic before committing.
	FailureActionRetry
	// FailureActionDLQ publishes the source record to the configured DLQ topic before committing.
	FailureActionDLQ
)

// FailurePolicy decides how a message-level consumer failure should be handled.
type FailurePolicy interface {
	Decide(context.Context, Error) FailureAction
}

// FailurePolicyFunc adapts a function into a FailurePolicy.
type FailurePolicyFunc func(context.Context, Error) FailureAction

func (f FailurePolicyFunc) Decide(ctx context.Context, failure Error) FailureAction {
	return f(ctx, failure)
}

// DropAndCommitPolicy drops every message-level failure and commits the source offset.
func DropAndCommitPolicy() FailurePolicy {
	return FailurePolicyFunc(func(context.Context, Error) FailureAction {
		return FailureActionDrop
	})
}

// DLQPolicy sends every message-level failure to DLQ.
func DLQPolicy() FailurePolicy {
	return FailurePolicyFunc(func(context.Context, Error) FailureAction {
		return FailureActionDLQ
	})
}

// RetryThenDLQConfig configures RetryThenDLQPolicy.
type RetryThenDLQConfig struct {
	MaxAttempts int
	Retryable   func(Error) bool
	DLQable     func(Error) bool
}

// RetryThenDLQPolicy retries retryable failures until MaxAttempts, then sends DLQable failures to DLQ.
func RetryThenDLQPolicy(cfg RetryThenDLQConfig) FailurePolicy {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.Retryable == nil {
		cfg.Retryable = func(failure Error) bool {
			return failure.Stage == StageHandle
		}
	}
	if cfg.DLQable == nil {
		cfg.DLQable = func(failure Error) bool {
			switch failure.Stage {
			case StageDecode, StageUnhandled, StageHandle:
				return true
			default:
				return false
			}
		}
	}

	return FailurePolicyFunc(func(_ context.Context, failure Error) FailureAction {
		current := retryAttempt(nil)
		if failure.Record != nil {
			current = retryAttempt(failure.Record.Headers)
		}
		next := current + 1
		if cfg.Retryable(failure) && next < cfg.MaxAttempts {
			return FailureActionRetry
		}
		if cfg.DLQable(failure) {
			return FailureActionDLQ
		}
		return FailureActionDrop
	})
}
