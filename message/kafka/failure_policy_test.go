package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
)

// Intent (A9): the default classification retries only failures a retry can
// fix, and MaxAttempts counts every attempt: below it retries, at it gives up.
func TestRetryPolicyDecisionTable(t *testing.T) {
	transient := errors.New("database unavailable")
	tests := []struct {
		name    string
		cfg     RetryConfig
		failure Error
		want    FailureAction
	}{
		{"handler error, first attempt", RetryConfig{}, Error{Stage: StageHandle, Err: transient, Attempt: 1}, FailureActionRetry},
		{"handler error, below max", RetryConfig{}, Error{Stage: StageHandle, Err: transient, Attempt: 2}, FailureActionRetry},
		{"handler error, at default max 3", RetryConfig{}, Error{Stage: StageHandle, Err: transient, Attempt: 3}, FailureActionDrop},
		{"handler timeout retries", RetryConfig{}, Error{Stage: StageHandle, Err: fmt.Errorf("%w after 1s", ErrHandlerTimeout), Attempt: 1}, FailureActionRetry},
		{"handler panic retries", RetryConfig{}, Error{Stage: StageHandle, Err: &PanicError{Value: "boom"}, Attempt: 1}, FailureActionRetry},
		{"non-retryable handler error", RetryConfig{}, Error{Stage: StageHandle, Err: NonRetryable(transient), Attempt: 1}, FailureActionDrop},
		{"decode failure", RetryConfig{}, Error{Stage: StageDecode, Err: ErrNoKind, Attempt: 1}, FailureActionDrop},
		{"unhandled kind", RetryConfig{}, Error{Stage: StageUnhandled, Err: message.ErrUnhandledKind, Attempt: 1}, FailureActionDrop},
		{"exhausted with stop", RetryConfig{MaxAttempts: 2, OnExhausted: FailureActionStop}, Error{Stage: StageHandle, Err: transient, Attempt: 2}, FailureActionStop},
		{"non-retryable with stop", RetryConfig{OnExhausted: FailureActionStop}, Error{Stage: StageDecode, Err: ErrNoKind, Attempt: 1}, FailureActionStop},
		{"OnExhausted retry counts as drop", RetryConfig{MaxAttempts: 1, OnExhausted: FailureActionRetry}, Error{Stage: StageHandle, Err: transient, Attempt: 1}, FailureActionDrop},
		{"custom retryable", RetryConfig{Retryable: func(Error) bool { return true }}, Error{Stage: StageDecode, Err: ErrNoKind, Attempt: 1}, FailureActionRetry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RetryPolicy(tt.cfg).Decide(context.Background(), tt.failure); got != tt.want {
				t.Fatalf("Decide = %v, want %v", got, tt.want)
			}
		})
	}
}

// Intent: DropAndCommitPolicy never retries.
func TestDropAndCommitPolicyDrops(t *testing.T) {
	got := DropAndCommitPolicy().Decide(context.Background(), Error{Stage: StageHandle, Err: errors.New("x"), Attempt: 1})
	if got != FailureActionDrop {
		t.Fatalf("Decide = %v, want drop", got)
	}
}

// Intent: the default backoff starts at 1s, doubles, and caps at 1 minute
// without overflowing for large attempts.
func TestDefaultRetryBackoff(t *testing.T) {
	tests := map[int]time.Duration{
		0:    time.Second,
		1:    time.Second,
		2:    2 * time.Second,
		3:    4 * time.Second,
		6:    32 * time.Second,
		7:    time.Minute, // 64s is capped
		8:    time.Minute,
		1000: time.Minute,
	}
	for attempt, want := range tests {
		if got := DefaultRetryBackoff(attempt); got != want {
			t.Errorf("DefaultRetryBackoff(%d) = %v, want %v", attempt, got, want)
		}
	}
}

// Intent: NonRetryable marks an error without hiding it.
func TestNonRetryable(t *testing.T) {
	if NonRetryable(nil) != nil {
		t.Fatal("NonRetryable(nil) != nil")
	}
	cause := errors.New("order does not exist")
	err := NonRetryable(cause)
	if !errors.Is(err, ErrNonRetryable) || !errors.Is(err, cause) {
		t.Fatalf("%v does not match ErrNonRetryable and its cause", err)
	}
	if err.Error() != cause.Error() {
		t.Fatalf("Error() = %q, want %q", err.Error(), cause.Error())
	}
}
