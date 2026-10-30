package asynq

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// Intent: Client should implement the component Enqueuer contract by sending a
// converted asynq task through the configured asynq client.
func TestClientEnqueueUsesAsynqClient(t *testing.T) {
	fake := &fakeEnqueueClient{}
	client := newClient(fake)
	task, err := taskqueue.New(
		taskqueue.Definition{Type: "document.review.v1", Queue: "critical"},
		[]byte(`{"id":"doc-1"}`),
		taskqueue.WithID("doc-1"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = client.Enqueue(context.Background(), task, taskqueue.WithMaxRetry(0))

	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if fake.task == nil {
		t.Fatal("asynq task was not enqueued")
	}
	if fake.task.Type() != "document.review.v1" {
		t.Fatalf("type = %q", fake.task.Type())
	}
	values := optionValues(fake.opts)
	if values[hibiken.QueueOpt] != "critical" {
		t.Fatalf("queue option = %#v", values[hibiken.QueueOpt])
	}
	if values[hibiken.TaskIDOpt] != "doc-1" {
		t.Fatalf("task id option = %#v", values[hibiken.TaskIDOpt])
	}
	if values[hibiken.MaxRetryOpt] != 0 {
		t.Fatalf("max retry option = %#v", values[hibiken.MaxRetryOpt])
	}
}

// Intent: missing asynq client configuration should fail before enqueue
// attempts are silently dropped.
func TestClientEnqueueRejectsNilClient(t *testing.T) {
	client := newClient(nil)
	task, err := taskqueue.New(taskqueue.Definition{Type: "document.review.v1"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = client.Enqueue(context.Background(), task)

	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("error = %v, want ErrNilClient", err)
	}
}

// Intent: Client.Close should delegate to asynq clients that own Redis
// connections so applications can release resources through the adapter.
func TestClientCloseDelegatesToUnderlyingClient(t *testing.T) {
	fake := &fakeEnqueueClient{}
	client := newClient(fake)

	err := client.Close()

	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !fake.closed {
		t.Fatal("underlying client was not closed")
	}
}

// Intent: an asynq task ID conflict is the component ErrDuplicateTask, so
// application code detects it without importing asynq.
func TestClientEnqueueMapsTaskIDConflict(t *testing.T) {
	client := newClient(&fakeEnqueueClient{err: hibiken.ErrTaskIDConflict})
	task, err := taskqueue.New(taskqueue.Definition{Type: "document.review.v1"}, nil, taskqueue.WithID("review:doc-1"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = client.Enqueue(context.Background(), task)

	if !errors.Is(err, taskqueue.ErrDuplicateTask) {
		t.Fatalf("err = %v, want taskqueue.ErrDuplicateTask", err)
	}
	if !errors.Is(err, hibiken.ErrTaskIDConflict) {
		t.Fatalf("err = %v, want asynq.ErrTaskIDConflict", err)
	}
}

type fakeEnqueueClient struct {
	task   *hibiken.Task
	opts   []hibiken.Option
	err    error
	closed bool
}

func (f *fakeEnqueueClient) EnqueueContext(_ context.Context, task *hibiken.Task, opts ...hibiken.Option) (*hibiken.TaskInfo, error) {
	f.task = task
	f.opts = append([]hibiken.Option(nil), opts...)
	if f.err != nil {
		return nil, f.err
	}
	return &hibiken.TaskInfo{ID: "task-1"}, nil
}

func (f *fakeEnqueueClient) Close() error {
	f.closed = true
	return nil
}
