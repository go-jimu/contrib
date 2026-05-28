package asynq

import (
	"context"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

type enqueueClient interface {
	EnqueueContext(context.Context, *hibiken.Task, ...hibiken.Option) (*hibiken.TaskInfo, error)
}

type closeClient interface {
	Close() error
}

type Client struct {
	client enqueueClient
}

var _ taskqueue.Enqueuer = (*Client)(nil)

func NewClient(client *hibiken.Client) *Client {
	var enqueue enqueueClient
	if client != nil {
		enqueue = client
	}
	return newClient(enqueue)
}

func NewRedisClient(conn hibiken.RedisConnOpt) *Client {
	return NewClient(hibiken.NewClient(conn))
}

func newClient(client enqueueClient) *Client {
	return &Client{client: client}
}

func (c *Client) Enqueue(ctx context.Context, task taskqueue.Task, opts ...taskqueue.EnqueueOption) error {
	_, err := c.EnqueueInfo(ctx, task, opts...)
	return err
}

func (c *Client) EnqueueInfo(ctx context.Context, task taskqueue.Task, opts ...taskqueue.EnqueueOption) (*hibiken.TaskInfo, error) {
	if c.client == nil {
		return nil, ErrNilClient
	}

	asynqTask, asynqOpts, err := taskToAsynq(task, opts...)
	if err != nil {
		return nil, err
	}
	return c.client.EnqueueContext(ctx, asynqTask, asynqOpts...)
}

func (c *Client) Close() error {
	if c.client == nil {
		return ErrNilClient
	}
	if closer, ok := c.client.(closeClient); ok {
		return closer.Close()
	}
	return nil
}
