package asynq

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

type enqueueClient interface {
	EnqueueContext(context.Context, *hibiken.Task, ...hibiken.Option) (*hibiken.TaskInfo, error)
}

type closeClient interface {
	Close() error
}

// Client enqueues taskqueue tasks into asynq. It implements
// taskqueue.Enqueuer and is safe for concurrent use.
type Client struct {
	client enqueueClient
}

var _ taskqueue.Enqueuer = (*Client)(nil)

// NewClient wraps an existing asynq client. Client.Close closes it.
func NewClient(client *hibiken.Client) *Client {
	var enqueue enqueueClient
	if client != nil {
		enqueue = client
	}
	return newClient(enqueue)
}

// NewRedisClient creates an asynq client for conn and wraps it. Call Close
// when the process stops enqueueing.
func NewRedisClient(conn hibiken.RedisConnOpt) *Client {
	return NewClient(hibiken.NewClient(conn))
}

func newClient(client enqueueClient) *Client {
	return &Client{client: client}
}

// Enqueue stores task in asynq with opts, without validating opts; see the
// package documentation for the mapping and for unvalidated policies.
//
// It returns nil once Redis has stored the task; an error matching
// taskqueue.ErrDuplicateTask when task.ID() is still kept in its queue (also
// matching asynq.ErrTaskIDConflict) or when WithUnique suppresses a duplicate
// (also matching asynq.ErrDuplicateTask); taskqueue.ErrEmptyType;
// ErrNilClient; or an asynq or Redis error.
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
	// An ID conflict and a WithUnique duplicate both mean the work is already
	// queued; the asynq error stays in the chain for callers that check it.
	if errors.Is(err, hibiken.ErrTaskIDConflict) || errors.Is(err, hibiken.ErrDuplicateTask) {
		return nil, fmt.Errorf("%w: %w", taskqueue.ErrDuplicateTask, err)
	}
	return info, err
}

// Close closes the wrapped asynq client, including one passed to NewClient.
// It returns ErrNilClient when there is none.
func (c *Client) Close() error {
	if c.client == nil {
		return ErrNilClient
	}
	if closer, ok := c.client.(closeClient); ok {
		return closer.Close()
	}
	return nil
}
