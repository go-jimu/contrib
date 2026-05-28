package asynq

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// Intent: ServeMux should adapt an asynq task into a component task and invoke
// the processor registered for that exact task type.
func TestServeMuxProcessTaskDispatchesRegisteredProcessor(t *testing.T) {
	mux := NewServeMux()
	var seen taskqueue.Task
	err := mux.Register(taskqueue.NewProcessor("document.review.v1", func(_ context.Context, task taskqueue.Task) error {
		seen = task
		return nil
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	err = mux.ProcessTask(context.Background(), hibiken.NewTaskWithHeaders("document.review.v1", []byte(`{"id":"doc-1"}`), map[string]string{
		"trace-id": "trace-1",
	}))

	if err != nil {
		t.Fatalf("ProcessTask: %v", err)
	}
	if seen.Type() != taskqueue.TaskType("document.review.v1") {
		t.Fatalf("seen type = %q", seen.Type())
	}
	if string(seen.Payload()) != `{"id":"doc-1"}` {
		t.Fatalf("seen payload = %s", seen.Payload())
	}
	if seen.Headers()["trace-id"] != "trace-1" {
		t.Fatalf("trace header = %q", seen.Headers()["trace-id"])
	}
}

// Intent: processor registration should reject invalid and duplicate task
// types so one task is never processed by ambiguous processors.
func TestServeMuxRegisterValidation(t *testing.T) {
	mux := NewServeMux()
	if err := mux.Register(nil); !errors.Is(err, taskqueue.ErrNilProcessor) {
		t.Fatalf("nil processor error = %v", err)
	}
	if err := mux.Register(taskqueue.NewProcessor("", func(context.Context, taskqueue.Task) error { return nil })); !errors.Is(err, taskqueue.ErrEmptyType) {
		t.Fatalf("empty type error = %v", err)
	}
	if err := mux.Register(taskqueue.NewProcessor("document.review.v1", func(context.Context, taskqueue.Task) error { return nil })); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := mux.Register(taskqueue.NewProcessor("document.review.v1", func(context.Context, taskqueue.Task) error { return nil })); !errors.Is(err, taskqueue.ErrDuplicateProcessor) {
		t.Fatalf("duplicate error = %v", err)
	}
}

// Intent: taskqueue.ErrSkipRetry should be translated into asynq.SkipRetry so
// processor decisions survive the adapter boundary.
func TestServeMuxProcessTaskMapsSkipRetry(t *testing.T) {
	mux := NewServeMux()
	if err := mux.Register(taskqueue.NewProcessor("document.review.v1", func(context.Context, taskqueue.Task) error {
		return taskqueue.ErrSkipRetry
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := mux.ProcessTask(context.Background(), hibiken.NewTask("document.review.v1", nil))

	if !errors.Is(err, taskqueue.ErrSkipRetry) {
		t.Fatalf("error = %v, want taskqueue.ErrSkipRetry", err)
	}
	if !errors.Is(err, hibiken.SkipRetry) {
		t.Fatalf("error = %v, want asynq.SkipRetry", err)
	}
}

// Intent: unregistered asynq task types should return the component unhandled
// error so callers can distinguish routing gaps from processor failures.
func TestServeMuxProcessTaskRejectsUnhandledType(t *testing.T) {
	mux := NewServeMux()

	err := mux.ProcessTask(context.Background(), hibiken.NewTask("missing.type", nil))

	if !errors.Is(err, taskqueue.ErrUnhandledType) {
		t.Fatalf("error = %v, want ErrUnhandledType", err)
	}
}
