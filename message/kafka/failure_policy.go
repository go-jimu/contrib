package kafka

import "context"

// FailureAction is a FailurePolicy decision for one failed record.
type FailureAction int

const (
	// FailureActionDrop commits the source offset without producing a retry or DLQ record.
	FailureActionDrop FailureAction = iota
	// FailureActionRetry publishes the source record to the configured retry topic before committing.
	FailureActionRetry
	// FailureActionDLQ publishes the source record to the configured DLQ topic before committing.
	FailureActionDLQ
)

// FailurePolicy decides how a message-level consumer failure should be
// handled: StageDecode, StageUnhandled, or StageHandle, while Run's ctx is
// live. Decide runs on the Run goroutine, so return quickly; an unknown FailureAction counts as
// FailureActionDrop. The package documentation compares the built-in
// policies.
type FailurePolicy interface {
	Decide(context.Context, Error) FailureAction
}

// FailurePolicyFunc adapts a function into a FailurePolicy.
type FailurePolicyFunc func(context.Context, Error) FailureAction

// Decide calls f(ctx, failure).
func (f FailurePolicyFunc) Decide(ctx context.Context, failure Error) FailureAction {
	return f(ctx, failure)
}

// DropAndCommitPolicy drops every message-level failure and commits the source
// offset; the failed message is lost. It needs no failure topics.
func DropAndCommitPolicy() FailurePolicy {
	return FailurePolicyFunc(func(context.Context, Error) FailureAction {
		return FailureActionDrop
	})
}

// DLQPolicy sends every message-level failure to DLQ, then commits. It needs
// WithDefaultFailureTopics or WithDLQTopicResolver; without one, every
// failure is a DLQ publish failure.
func DLQPolicy() FailurePolicy {
	return FailurePolicyFunc(func(context.Context, Error) FailureAction {
		return FailureActionDLQ
	})
}

// RetryThenDLQConfig configures RetryThenDLQPolicy.
type RetryThenDLQConfig struct {
	// MaxAttempts is the number of handler attempts, counting the first;
	// 0 or less means 3.
	MaxAttempts int
	// Retryable reports whether a failure may be retried. nil retries
	// StageHandle failures only.
	Retryable func(Error) bool
	// DLQable reports whether a failure that is not retried goes to DLQ;
	// otherwise it is dropped. nil sends StageDecode, StageUnhandled, and
	// StageHandle failures to DLQ.
	DLQable func(Error) bool
}

// RetryThenDLQPolicy retries Retryable failures by publishing them to the
// retry topic until the record has had MaxAttempts handler attempts, read from
// its jimu-retry-attempt header; then it sends DLQable failures to DLQ and
// drops the rest. It needs retry and DLQ topic resolvers, and the consumer
// must consume the retry topics for retries to run.
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
