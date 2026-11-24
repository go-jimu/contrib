package asynq

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// Intent: with WithArchivedReplacement, an archived task that holds the ID of
// a new enqueue never runs again, so it is deleted and the new task takes the
// ID; the replacement is logged with the archived task's last error.
func TestEnqueueReplacesArchivedHolderOfTheID(t *testing.T) {
	for name, holderErr := range map[string]error{
		"id conflict":     hibiken.ErrTaskIDConflict,
		"uniqueness lock": hibiken.ErrDuplicateTask,
	} {
		t.Run(name, func(t *testing.T) {
			enqueue := &sequencedEnqueueClient{errs: []error{holderErr, nil}}
			archived := &fakeArchivedTasks{state: hibiken.TaskStateArchived, lastErr: "boom"}
			var logs bytes.Buffer
			client := newClient(enqueue, withArchived(archived, slog.New(slog.NewTextHandler(&logs, nil))))

			if err := client.Enqueue(context.Background(), keyedTask(t, ""), taskqueue.WithMaxRetry(1)); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			if enqueue.calls != 2 {
				t.Fatalf("enqueue calls = %d, want 2", enqueue.calls)
			}
			if archived.deleted != "default/doc-1" {
				t.Fatalf("deleted = %q, want default/doc-1", archived.deleted)
			}
			if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "last_error=boom") {
				t.Fatalf("replacement log = %q", logs.String())
			}
		})
	}
}

// Intent: a holder that still waits, retries or runs stands for the new
// enqueue, so it is kept and Enqueue reports ErrDuplicateTask.
func TestEnqueueKeepsALiveHolderOfTheID(t *testing.T) {
	for _, state := range []hibiken.TaskState{hibiken.TaskStatePending, hibiken.TaskStateScheduled, hibiken.TaskStateRetry, hibiken.TaskStateActive} {
		enqueue := &sequencedEnqueueClient{errs: []error{hibiken.ErrTaskIDConflict}}
		archived := &fakeArchivedTasks{state: state}
		client := newClient(enqueue, withArchived(archived, slog.New(slog.DiscardHandler)))

		err := client.Enqueue(context.Background(), keyedTask(t, "critical"))
		if !errors.Is(err, taskqueue.ErrDuplicateTask) {
			t.Fatalf("%v: err = %v, want ErrDuplicateTask", state, err)
		}
		if archived.deleted != "" || enqueue.calls != 1 {
			t.Fatalf("%v: deleted %q after %d enqueues", state, archived.deleted, enqueue.calls)
		}
		if archived.read != "critical/doc-1" {
			t.Fatalf("%v: read %q, want the task's own queue", state, archived.read)
		}
	}
}

// Intent: a failed delete leaves the ID held; Enqueue reports the duplicate
// and logs the failure at Error instead of retrying without bound.
func TestEnqueueReportsAFailedReplacement(t *testing.T) {
	enqueue := &sequencedEnqueueClient{errs: []error{hibiken.ErrTaskIDConflict}}
	archived := &fakeArchivedTasks{state: hibiken.TaskStateArchived, deleteErr: errors.New("redis down")}
	var logs bytes.Buffer
	client := newClient(enqueue, withArchived(archived, slog.New(slog.NewTextHandler(&logs, nil))))

	err := client.Enqueue(context.Background(), keyedTask(t, ""))
	if !errors.Is(err, taskqueue.ErrDuplicateTask) || enqueue.calls != 1 {
		t.Fatalf("err = %v after %d enqueues", err, enqueue.calls)
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("failure log = %q", logs.String())
	}
}

// Intent: without the option, and for a task without an ID, an archived
// holder is left to asynq tooling and nothing is read.
func TestEnqueueLeavesArchivedHoldersWithoutTheOption(t *testing.T) {
	enqueue := &sequencedEnqueueClient{errs: []error{hibiken.ErrTaskIDConflict}}
	if err := newClient(enqueue).Enqueue(context.Background(), keyedTask(t, "")); !errors.Is(err, taskqueue.ErrDuplicateTask) {
		t.Fatalf("err = %v, want ErrDuplicateTask", err)
	}

	enqueue = &sequencedEnqueueClient{errs: []error{hibiken.ErrDuplicateTask}}
	archived := &fakeArchivedTasks{state: hibiken.TaskStateArchived}
	task, err := taskqueue.New(taskqueue.Definition{Type: "document.review.v1"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := newClient(enqueue, withArchived(archived, nil)).Enqueue(context.Background(), task); !errors.Is(err, taskqueue.ErrDuplicateTask) {
		t.Fatalf("err = %v, want ErrDuplicateTask", err)
	}
	if archived.read != "" {
		t.Fatalf("read %q for a task without an ID", archived.read)
	}
}

// Intent: Close releases the inspector with the client.
func TestCloseClosesTheInspector(t *testing.T) {
	archived := &fakeArchivedTasks{}
	if err := newClient(&fakeEnqueueClient{}, withArchived(archived, nil)).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !archived.closed {
		t.Fatal("inspector was not closed")
	}
}

func withArchived(tasks archivedTasks, logger *slog.Logger) ClientOption {
	return func(c *Client) { c.archived, c.logger = tasks, logger }
}

func keyedTask(t *testing.T, queue string) taskqueue.Task {
	t.Helper()
	task, err := taskqueue.New(taskqueue.Definition{Type: "document.review.v1", Queue: queue}, nil, taskqueue.WithID("doc-1"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return task
}

type sequencedEnqueueClient struct {
	errs  []error
	calls int
}

func (f *sequencedEnqueueClient) EnqueueContext(context.Context, *hibiken.Task, ...hibiken.Option) (*hibiken.TaskInfo, error) {
	f.calls++
	var err error
	if len(f.errs) > 0 {
		err, f.errs = f.errs[0], f.errs[1:]
	}
	if err != nil {
		return nil, err
	}
	return &hibiken.TaskInfo{ID: "doc-1"}, nil
}

type fakeArchivedTasks struct {
	state     hibiken.TaskState
	lastErr   string
	deleteErr error
	read      string
	deleted   string
	closed    bool
}

func (f *fakeArchivedTasks) GetTaskInfo(queue, id string) (*hibiken.TaskInfo, error) {
	f.read = queue + "/" + id
	return &hibiken.TaskInfo{ID: id, Queue: queue, State: f.state, LastErr: f.lastErr}, nil
}

func (f *fakeArchivedTasks) DeleteTask(queue, id string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = queue + "/" + id
	return nil
}

func (f *fakeArchivedTasks) Close() error {
	f.closed = true
	return nil
}
