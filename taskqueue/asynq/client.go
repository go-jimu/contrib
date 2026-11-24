package asynq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/go-jimu/components/sloghelper"
	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

type enqueueClient interface {
	EnqueueContext(context.Context, *hibiken.Task, ...hibiken.Option) (*hibiken.TaskInfo, error)
}

type closeClient interface {
	Close() error
}

// defaultQueue is asynq's queue for a task that names none.
const defaultQueue = "default"

// archivedTasks reads and deletes the task holding an ID; *asynq.Inspector
// implements it.
type archivedTasks interface {
	GetTaskInfo(queue, id string) (*hibiken.TaskInfo, error)
	DeleteTask(queue, id string) error
	Close() error
}

// Client enqueues taskqueue tasks into asynq. It implements
// taskqueue.Enqueuer and is safe for concurrent use.
type Client struct {
	client   enqueueClient
	archived archivedTasks
	logger   *slog.Logger
}

var _ taskqueue.Enqueuer = (*Client)(nil)

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithArchivedReplacement makes Enqueue reclaim a task ID that an archived
// task holds: it deletes that task and enqueues again, logging the
// replacement at Warn with the archived task's last error. Without it an
// archived failure keeps its ID, and every later enqueue of that ID returns
// ErrDuplicateTask until an operator deletes it; see "Task ID and
// duplicates". inspector reads the queues the Client enqueues into, and
// Client.Close closes it. A nil logger means slog.Default().
func WithArchivedReplacement(inspector *hibiken.Inspector, logger *slog.Logger) ClientOption {
	return func(c *Client) {
		if inspector != nil {
			c.archived = inspector
		}
		c.logger = logger
	}
}

// NewClient wraps an existing asynq client. Client.Close closes it.
func NewClient(client *hibiken.Client, opts ...ClientOption) *Client {
	var enqueue enqueueClient
	if client != nil {
		enqueue = client
	}
	return newClient(enqueue, opts...)
}

// NewRedisClient creates an asynq client for conn and wraps it. Call Close
// when the process stops enqueueing.
func NewRedisClient(conn hibiken.RedisConnOpt, opts ...ClientOption) *Client {
	return NewClient(hibiken.NewClient(conn), opts...)
}

func newClient(client enqueueClient, opts ...ClientOption) *Client {
	c := &Client{client: client}
	for _, opt := range opts {
		opt(c)
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c
}

// Enqueue stores task in asynq with opts, without validating opts; see the
// package documentation for the mapping and for unvalidated policies.
//
// It returns nil once Redis has stored the task; an error matching
// taskqueue.ErrDuplicateTask when task.ID() is still kept in its queue (also
// matching asynq.ErrTaskIDConflict) or when WithUnique suppresses a duplicate
// (also matching asynq.ErrDuplicateTask); taskqueue.ErrEmptyType;
// ErrNilClient; or an asynq or Redis error. With WithArchivedReplacement, an
// ID or uniqueness lock held by an archived task with the same ID does not
// count as kept.
func (c *Client) Enqueue(ctx context.Context, task taskqueue.Task, opts ...taskqueue.EnqueueOption) error {
	_, err := c.EnqueueInfo(ctx, task, opts...)
	return err
}

// EnqueueInfo is Enqueue that also returns asynq's record of the stored task,
// such as its ID and state. Its result is an asynq type, so call it from
// runtime composition.
func (c *Client) EnqueueInfo(ctx context.Context, task taskqueue.Task, opts ...taskqueue.EnqueueOption) (*hibiken.TaskInfo, error) {
	if c.client == nil {
		return nil, ErrNilClient
	}

	asynqTask, asynqOpts, err := taskToAsynq(task, opts...)
	if err != nil {
		return nil, err
	}
	info, err := c.client.EnqueueContext(ctx, asynqTask, asynqOpts...)
	// A task with an ID holds both the ID and, with WithUnique, its
	// uniqueness lock, so either refusal may come from an archived holder.
	if isDuplicate(err) && c.reclaimArchived(ctx, task) {
		info, err = c.client.EnqueueContext(ctx, asynqTask, asynqOpts...)
	}
	// An ID conflict and a WithUnique duplicate both mean the work is already
	// queued; the asynq error stays in the chain for callers that check it.
	if isDuplicate(err) {
		return nil, fmt.Errorf("%w: %w", taskqueue.ErrDuplicateTask, err)
	}
	return info, err
}

func isDuplicate(err error) bool {
	return errors.Is(err, hibiken.ErrTaskIDConflict) || errors.Is(err, hibiken.ErrDuplicateTask)
}

// reclaimArchived deletes the archived task holding task's ID, and reports
// whether the ID is free to enqueue again. An archived task never runs
// again, so it cannot stand for the new enqueue.
func (c *Client) reclaimArchived(ctx context.Context, task taskqueue.Task) bool {
	if c.archived == nil || task.ID() == "" {
		return false
	}
	queue := task.Queue()
	if queue == "" {
		queue = defaultQueue
	}
	info, err := c.archived.GetTaskInfo(queue, task.ID())
	if err != nil || info.State != hibiken.TaskStateArchived {
		return false
	}
	attrs := []slog.Attr{slog.String("task_type", string(task.Type())), slog.String("queue", queue), slog.String("task_id", task.ID())}
	if err := c.archived.DeleteTask(queue, task.ID()); err != nil && !errors.Is(err, hibiken.ErrTaskNotFound) {
		c.logger.LogAttrs(ctx, slog.LevelError, "archived task holding the ID could not be deleted", append(attrs, sloghelper.Error(err))...)
		return false
	}
	c.logger.LogAttrs(ctx, slog.LevelWarn, "archived task replaced by a new enqueue of its ID", append(attrs, slog.String("last_error", info.LastErr))...)
	return true
}

// Close closes the wrapped asynq client, including one passed to NewClient,
// and the inspector of WithArchivedReplacement. It returns ErrNilClient when
// there is no client.
func (c *Client) Close() error {
	if c.client == nil {
		return ErrNilClient
	}
	var errs []error
	if closer, ok := c.client.(closeClient); ok {
		errs = append(errs, closer.Close())
	}
	if c.archived != nil {
		errs = append(errs, c.archived.Close())
	}
	return errors.Join(errs...)
}
