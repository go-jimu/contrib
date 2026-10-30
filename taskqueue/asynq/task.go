package asynq

import (
	"context"
	"maps"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// PayloadCodecHeader is the asynq task header that carries
// taskqueue.Task.PayloadCodec. It is reserved: the adapter sets it on enqueue
// and removes it from the headers processors see.
const PayloadCodecHeader = "jimu-payload-codec"

func taskToAsynq(task taskqueue.Task, opts ...taskqueue.EnqueueOption) (*hibiken.Task, []hibiken.Option, error) {
	return taskToAsynqWithPolicy(task, taskqueue.NewEnqueueOptions(opts...))
}

func taskToAsynqWithPolicy(task taskqueue.Task, policy taskqueue.EnqueueOptions) (*hibiken.Task, []hibiken.Option, error) {
	if task.Type() == "" {
		return nil, nil, taskqueue.ErrEmptyType
	}

	headers := task.Headers()
	if codec := task.PayloadCodec(); codec != "" {
		if headers == nil {
			headers = make(map[string]string, 1)
		}
		headers[PayloadCodecHeader] = codec
	}
	asynqTask := hibiken.NewTaskWithHeaders(string(task.Type()), task.Payload(), headers)
	return asynqTask, enqueueOptions(task, policy), nil
}

func taskFromAsynq(ctx context.Context, task *hibiken.Task) (taskqueue.Task, error) {
	if task == nil {
		return taskqueue.Task{}, ErrNilTask
	}

	// The task ID asynq reports is provider-assigned unless the producer set
	// one, so it reaches processors through taskqueue.ExecutionInfo instead of
	// Task.ID, which is reserved for caller-assigned identities.
	headers := maps.Clone(task.Headers())
	codec := headers[PayloadCodecHeader]
	delete(headers, PayloadCodecHeader)
	options := []taskqueue.Option{
		taskqueue.WithHeaders(headers),
		taskqueue.WithPayloadCodec(codec),
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
	if task.ID() != "" {
		options = append(options, hibiken.TaskID(task.ID()))
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
