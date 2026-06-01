package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type failureDecision struct {
	action  FailureAction
	topic   string
	attempt int
	err     error
}

func failureAction(ctx context.Context, failure Error, policy FailurePolicy, cfg config) failureDecision {
	if failure.Record == nil {
		return failureDecision{action: FailureActionDrop, err: ErrNilRecord}
	}
	if failure.Record.Topic == "" {
		return failureDecision{action: FailureActionDrop, err: ErrNoTopic}
	}

	current := retryAttempt(failure.Record.Headers)
	next := current + 1
	action := policy.Decide(ctx, failure)
	switch action {
	case FailureActionRetry:
		topic, err := resolveFailureTopic(cfg.retryTopicResolver, failure)
		return failureDecision{action: action, topic: topic, attempt: next, err: err}
	case FailureActionDLQ:
		topic, err := resolveFailureTopic(cfg.dlqTopicResolver, failure)
		return failureDecision{action: action, topic: topic, attempt: next, err: err}
	default:
		return failureDecision{action: FailureActionDrop, attempt: next}
	}
}

func buildFailureRecord(failure Error, decision failureDecision, cfg config) *kgo.Record {
	if decision.err != nil || decision.action == FailureActionDrop || decision.topic == "" || failure.Record == nil {
		return nil
	}

	record := cloneRecord(failure.Record)
	record.Topic = decision.topic
	addFailureHeaders(record, cfg, failure, decision.attempt)
	return record
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
