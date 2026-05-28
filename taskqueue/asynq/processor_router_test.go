package asynq

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// Intent: ProcessorRouter should adapt an asynq task into a component task and
// invoke the processor registered for that exact task type.
func TestProcessorRouterProcessTaskDispatchesRegisteredProcessor(t *testing.T) {
	router := NewProcessorRouter()
	var seen taskqueue.Task
	err := router.Register(taskqueue.NewProcessor("document.review.v1", func(_ context.Context, task taskqueue.Task) error {
		seen = task
		return nil
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	err = router.ProcessTask(context.Background(), hibiken.NewTaskWithHeaders("document.review.v1", []byte(`{"id":"doc-1"}`), map[string]string{
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

// Intent: ProcessorRouter should expose asynq worker metadata through the
// component execution context so middleware and processors can observe retry
// attempts without depending on asynq.
func TestProcessorRouterProcessTaskAddsExecutionInfo(t *testing.T) {
	router := newProcessorRouter(executionMetadataSource{
		taskID: func(context.Context) (string, bool) {
			return "provider-task-1", true
		},
		queueName: func(context.Context) (string, bool) {
			return "reconcile", true
		},
		retryCount: func(context.Context) (int, bool) {
			return 2, true
		},
		maxRetry: func(context.Context) (int, bool) {
			return 5, true
		},
	})
	var seen taskqueue.ExecutionInfo
	var found bool
	if err := router.Register(taskqueue.NewProcessor("document.review.v1", func(ctx context.Context, task taskqueue.Task) error {
		seen, found = taskqueue.ExecutionInfoFromContext(ctx)
		return nil
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := router.ProcessTask(context.Background(), hibiken.NewTask("document.review.v1", nil))

	if err != nil {
		t.Fatalf("ProcessTask: %v", err)
	}
	if !found {
		t.Fatal("execution info was not added to context")
	}
	if seen.TaskID() != "provider-task-1" {
		t.Fatalf("task id = %q", seen.TaskID())
	}
	if seen.Queue() != "reconcile" {
		t.Fatalf("queue = %q", seen.Queue())
	}
	retryCount, ok := seen.RetryCount()
	if !ok || retryCount != 2 {
		t.Fatalf("retry count = %d, %t; want 2, true", retryCount, ok)
	}
	maxRetry, ok := seen.MaxRetry()
	if !ok || maxRetry != 5 {
		t.Fatalf("max retry = %d, %t; want 5, true", maxRetry, ok)
	}
}

// Intent: processor registration should reject invalid and duplicate task
// types so one task is never processed by ambiguous processors.
func TestProcessorRouterRegisterValidation(t *testing.T) {
	router := NewProcessorRouter()
	if err := router.Register(nil); !errors.Is(err, taskqueue.ErrNilProcessor) {
		t.Fatalf("nil processor error = %v", err)
	}
	if err := router.Register(taskqueue.NewProcessor("", func(context.Context, taskqueue.Task) error { return nil })); !errors.Is(err, taskqueue.ErrEmptyType) {
		t.Fatalf("empty type error = %v", err)
	}
	if err := router.Register(taskqueue.NewProcessor("document.review.v1", func(context.Context, taskqueue.Task) error { return nil })); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := router.Register(taskqueue.NewProcessor("document.review.v1", func(context.Context, taskqueue.Task) error { return nil })); !errors.Is(err, taskqueue.ErrDuplicateProcessor) {
		t.Fatalf("duplicate error = %v", err)
	}
}

// Intent: taskqueue.ErrSkipRetry should be translated into asynq.SkipRetry so
// processor decisions survive the adapter boundary.
func TestProcessorRouterProcessTaskMapsSkipRetry(t *testing.T) {
	router := NewProcessorRouter()
	if err := router.Register(taskqueue.NewProcessor("document.review.v1", func(context.Context, taskqueue.Task) error {
		return taskqueue.ErrSkipRetry
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := router.ProcessTask(context.Background(), hibiken.NewTask("document.review.v1", nil))

	if !errors.Is(err, taskqueue.ErrSkipRetry) {
		t.Fatalf("error = %v, want taskqueue.ErrSkipRetry", err)
	}
	if !errors.Is(err, hibiken.SkipRetry) {
		t.Fatalf("error = %v, want asynq.SkipRetry", err)
	}
}

// Intent: unregistered asynq task types should return the component unhandled
// error so callers can distinguish routing gaps from processor failures.
func TestProcessorRouterProcessTaskRejectsUnhandledType(t *testing.T) {
	router := NewProcessorRouter()

	err := router.ProcessTask(context.Background(), hibiken.NewTask("missing.type", nil))

	if !errors.Is(err, taskqueue.ErrUnhandledType) {
		t.Fatalf("error = %v, want ErrUnhandledType", err)
	}
}
