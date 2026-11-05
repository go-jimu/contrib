package asynq

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// ProcessorRouter is an asynq.Handler that converts asynq tasks to
// taskqueue tasks and routes them to registered processors. Use it to serve
// processors from an asynq.Server you build and start yourself; Worker uses
// one internally. It is safe for concurrent use.
type ProcessorRouter struct {
	router   *taskqueue.Router
	metadata executionMetadataSource
}

var (
	_ taskqueue.Registrar = (*ProcessorRouter)(nil)
	_ hibiken.Handler     = (*ProcessorRouter)(nil)
)

// NewProcessorRouter returns a ProcessorRouter with no processors.
func NewProcessorRouter() *ProcessorRouter {
	return newProcessorRouter(defaultExecutionMetadataSource())
}

func newProcessorRouter(metadata executionMetadataSource) *ProcessorRouter {
	return &ProcessorRouter{
		router:   taskqueue.NewRouter(),
		metadata: metadata,
	}
}

// Register adds processor for its task type and returns the taskqueue.Router
// errors.
func (r *ProcessorRouter) Register(processor taskqueue.Processor) error {
	return r.router.Register(processor)
}

// ProcessTask converts task, stores taskqueue.ExecutionInfo in ctx, and runs
// the processor for its type. It returns the processor's error; an error
// wrapping taskqueue.ErrSkipRetry is returned wrapped with asynq.SkipRetry as
// well, so asynq archives the task without retrying. A type with no processor
// returns taskqueue.ErrUnhandledType, and a nil task ErrNilTask.
func (r *ProcessorRouter) ProcessTask(ctx context.Context, task *hibiken.Task) error {
	ctx = contextWithExecutionInfo(ctx, r.metadata)
	componentTask, err := taskFromAsynq(ctx, task)
	if err != nil {
		return err
	}
	err = r.router.Process(ctx, componentTask)
	if err == nil {
		return nil
	}
	if errors.Is(err, taskqueue.ErrSkipRetry) {
		return fmt.Errorf("%w: %w", hibiken.SkipRetry, err)
	}
	return err
}

type executionMetadataSource struct {
	taskID     func(context.Context) (string, bool)
	queueName  func(context.Context) (string, bool)
	retryCount func(context.Context) (int, bool)
	maxRetry   func(context.Context) (int, bool)
}

func defaultExecutionMetadataSource() executionMetadataSource {
	return executionMetadataSource{
		taskID:     hibiken.GetTaskID,
		queueName:  hibiken.GetQueueName,
		retryCount: hibiken.GetRetryCount,
		maxRetry:   hibiken.GetMaxRetry,
	}
}

func contextWithExecutionInfo(ctx context.Context, source executionMetadataSource) context.Context {
	opts := make([]taskqueue.ExecutionInfoOption, 0, 4)
	if source.taskID != nil {
		if id, ok := source.taskID(ctx); ok {
			opts = append(opts, taskqueue.WithExecutionTaskID(id))
		}
	}
	if source.queueName != nil {
		if queue, ok := source.queueName(ctx); ok {
			opts = append(opts, taskqueue.WithExecutionQueue(queue))
		}
	}
	if source.retryCount != nil {
		if retryCount, ok := source.retryCount(ctx); ok {
			opts = append(opts, taskqueue.WithExecutionRetryCount(retryCount))
		}
	}
	if source.maxRetry != nil {
		if maxRetry, ok := source.maxRetry(ctx); ok {
			opts = append(opts, taskqueue.WithExecutionMaxRetry(maxRetry))
		}
	}
	if len(opts) != 0 {
		return taskqueue.ContextWithExecutionInfo(ctx, taskqueue.NewExecutionInfo(opts...))
	}
	return ctx
}
