package asynq

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// Intent: converting a component task to asynq should preserve the envelope
// fields as asynq task data and enqueue options.
func TestTaskToAsynqMapsEnvelopeAndOptions(t *testing.T) {
	processAt := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	deadline := processAt.Add(time.Minute)
	task, err := taskqueue.New(
		taskqueue.Definition{Type: "document.review.v1", Queue: "critical"},
		[]byte(`{"id":"doc-1"}`),
		taskqueue.WithKey("doc-1"),
		taskqueue.WithHeader("trace-id", "trace-1"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	asynqTask, opts, err := taskToAsynq(task,
		taskqueue.WithDelay(30*time.Second),
		taskqueue.WithProcessAt(processAt),
		taskqueue.WithMaxRetry(0),
		taskqueue.WithTimeout(5*time.Second),
		taskqueue.WithDeadline(deadline),
		taskqueue.WithUnique(10*time.Minute),
	)

	if err != nil {
		t.Fatalf("taskToAsynq: %v", err)
	}
	if asynqTask.Type() != "document.review.v1" {
		t.Fatalf("type = %q", asynqTask.Type())
	}
	if string(asynqTask.Payload()) != `{"id":"doc-1"}` {
		t.Fatalf("payload = %s", asynqTask.Payload())
	}
	if got := asynqTask.Headers(); !reflect.DeepEqual(got, map[string]string{"trace-id": "trace-1"}) {
		t.Fatalf("headers = %#v", got)
	}

	values := optionValues(opts)
	if values[hibiken.QueueOpt] != "critical" {
		t.Fatalf("queue option = %#v", values[hibiken.QueueOpt])
	}
	if values[hibiken.TaskIDOpt] != "doc-1" {
		t.Fatalf("task id option = %#v", values[hibiken.TaskIDOpt])
	}
	if values[hibiken.ProcessInOpt] != 30*time.Second {
		t.Fatalf("process in option = %#v", values[hibiken.ProcessInOpt])
	}
	if values[hibiken.ProcessAtOpt] != processAt {
		t.Fatalf("process at option = %#v", values[hibiken.ProcessAtOpt])
	}
	if values[hibiken.MaxRetryOpt] != 0 {
		t.Fatalf("max retry option = %#v", values[hibiken.MaxRetryOpt])
	}
	if values[hibiken.TimeoutOpt] != 5*time.Second {
		t.Fatalf("timeout option = %#v", values[hibiken.TimeoutOpt])
	}
	if values[hibiken.DeadlineOpt] != deadline {
		t.Fatalf("deadline option = %#v", values[hibiken.DeadlineOpt])
	}
	if values[hibiken.UniqueOpt] != 10*time.Minute {
		t.Fatalf("unique option = %#v", values[hibiken.UniqueOpt])
	}
}

// Intent: converting an asynq task should rebuild the component task envelope
// that registered processors expect.
func TestTaskFromAsynqMapsEnvelope(t *testing.T) {
	asynqTask := hibiken.NewTaskWithHeaders("document.review.v1", []byte(`{"id":"doc-1"}`), map[string]string{
		"trace-id": "trace-1",
	})

	task, err := taskFromAsynq(context.Background(), asynqTask)

	if err != nil {
		t.Fatalf("taskFromAsynq: %v", err)
	}
	if task.Type() != taskqueue.TaskType("document.review.v1") {
		t.Fatalf("type = %q", task.Type())
	}
	if string(task.Payload()) != `{"id":"doc-1"}` {
		t.Fatalf("payload = %s", task.Payload())
	}
	if got := task.Headers(); !reflect.DeepEqual(got, map[string]string{"trace-id": "trace-1"}) {
		t.Fatalf("headers = %#v", got)
	}
}

// Intent: nil asynq tasks should fail clearly before processor code receives
// an invalid component task.
func TestTaskFromAsynqRejectsNilTask(t *testing.T) {
	task, err := taskFromAsynq(context.Background(), nil)

	if !errors.Is(err, ErrNilTask) {
		t.Fatalf("error = %v, want ErrNilTask", err)
	}
	if task.Type() != "" {
		t.Fatalf("task = %#v, want zero value", task)
	}
}

func optionValues(opts []hibiken.Option) map[hibiken.OptionType]any {
	values := make(map[hibiken.OptionType]any, len(opts))
	for _, opt := range opts {
		values[opt.Type()] = opt.Value()
	}
	return values
}
