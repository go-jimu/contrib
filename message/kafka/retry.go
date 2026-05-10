package kafka

import "github.com/twmb/franz-go/pkg/kgo"

type failureActionKind int

const (
	actionNone failureActionKind = iota
	actionRetry
	actionDLQ
)

type failureDecision struct {
	kind    failureActionKind
	topic   string
	attempt int
	err     error
}

func failureAction(failure Error, cfg config) failureDecision {
	if failure.Record == nil {
		return failureDecision{kind: actionNone, err: ErrNilRecord}
	}
	if failure.Record.Topic == "" {
		return failureDecision{kind: actionNone, err: ErrNoTopic}
	}

	current := retryAttempt(failure.Record.Headers)
	next := current + 1
	retryPolicy := normalizedRetryPolicy(cfg.retryPolicy)

	if retryPolicy.Retryable(failure) && next < retryPolicy.MaxAttempts {
		topic, err := resolveFailureTopic(cfg.retryTopicResolver, failure)
		return failureDecision{kind: actionRetry, topic: topic, attempt: next, err: err}
	}

	if !cfg.dlqPolicy.Enabled {
		return failureDecision{kind: actionNone, attempt: next}
	}
	topic, err := resolveFailureTopic(cfg.dlqTopicResolver, failure)
	return failureDecision{kind: actionDLQ, topic: topic, attempt: next, err: err}
}

func buildFailureRecord(failure Error, decision failureDecision, cfg config) *kgo.Record {
	if decision.kind == actionNone || failure.Record == nil {
		return nil
	}

	record := cloneRecord(failure.Record)
	record.Topic = decision.topic
	addFailureHeaders(record, cfg, failure, decision.attempt)
	return record
}

func normalizedRetryPolicy(policy RetryPolicy) RetryPolicy {
	defaults := defaultRetryPolicy()
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = defaults.MaxAttempts
	}
	if policy.Retryable == nil {
		policy.Retryable = defaults.Retryable
	}
	return policy
}

func resolveFailureTopic(resolver FailureTopicResolver, failure Error) (string, error) {
	if resolver == nil {
		return "", ErrNoTopic
	}
	topic, err := resolver(failure)
	if err != nil {
		return "", err
	}
	if topic == "" {
		return "", ErrNoTopic
	}
	return topic, nil
}
