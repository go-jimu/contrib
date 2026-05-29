package asynq

import (
	"context"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

func taskToAsynq(task taskqueue.Task, opts ...taskqueue.EnqueueOption) (*hibiken.Task, []hibiken.Option, error) {
	return taskToAsynqWithPolicy(task, taskqueue.NewEnqueueOptions(opts...))
}

func taskToAsynqWithPolicy(task taskqueue.Task, policy taskqueue.EnqueueOptions) (*hibiken.Task, []hibiken.Option, error) {
	if task.Type() == "" {
		return nil, nil, taskqueue.ErrEmptyType
	}

	asynqTask := hibiken.NewTaskWithHeaders(string(task.Type()), task.Payload(), task.Headers())
	return asynqTask, enqueueOptions(task, policy), nil
}

func taskFromAsynq(ctx context.Context, task *hibiken.Task) (taskqueue.Task, error) {
	if task == nil {
		return taskqueue.Task{}, ErrNilTask
	}

	options := []taskqueue.Option{
		taskqueue.WithHeaders(task.Headers()),
	}
	if id, ok := hibiken.GetTaskID(ctx); ok {
		options = append(options, taskqueue.WithKey(id))
	}

	queue := ""
	if name, ok := hibiken.GetQueueName(ctx); ok {
		queue = name
	}

	return taskqueue.New(taskqueue.Definition{
		Type:  taskqueue.TaskType(task.Type()),
		Queue: queue,
	}, task.Payload(), options...)
}

func enqueueOptions(task taskqueue.Task, cfg taskqueue.EnqueueOptions) []hibiken.Option {
	options := make([]hibiken.Option, 0, 8)

	if task.Queue() != "" {
		options = append(options, hibiken.Queue(task.Queue()))
	}
	if task.Key() != "" {
		options = append(options, hibiken.TaskID(task.Key()))
	}
	if delay := cfg.Delay(); delay != 0 {
		options = append(options, hibiken.ProcessIn(delay))
	}
	if processAt := cfg.ProcessAt(); !processAt.IsZero() {
		options = append(options, hibiken.ProcessAt(processAt))
	}
	if maxRetry, ok := cfg.MaxRetry(); ok {
		options = append(options, hibiken.MaxRetry(maxRetry))
	}
	if timeout := cfg.Timeout(); timeout != 0 {
		options = append(options, hibiken.Timeout(timeout))
	}
	if deadline := cfg.Deadline(); !deadline.IsZero() {
		options = append(options, hibiken.Deadline(deadline))
	}
	if uniqueTTL := cfg.UniqueTTL(); uniqueTTL != 0 {
		options = append(options, hibiken.Unique(uniqueTTL))
	}

	return options
}
