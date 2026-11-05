// Package asynq adapts github.com/hibiken/asynq, a Redis-backed task queue,
// to github.com/go-jimu/components/taskqueue.
//
// # Runtime composition
//
// Import this package and github.com/hibiken/asynq in runtime composition
// only, such as an internal/pkg/taskqueue package. Runtime composition builds
// the Client, Worker, and Scheduler, registers processors and periodic tasks
// on them, and starts and stops them. Application code depends on
// taskqueue.Enqueuer, implements taskqueue.Processor, and provides
// taskqueue.PeriodicTask values. EnqueueInfo returns *asynq.TaskInfo, so call
// it from runtime composition.
//
// # Roles
//
//   - Client implements taskqueue.Enqueuer for every process that enqueues.
//     Build it with NewRedisClient, or NewClient around an existing
//     *asynq.Client.
//   - Worker processes tasks. Build it with NewRedisWorker, or NewWorker
//     around an *asynq.Server, register one taskqueue.Processor per TaskType,
//     and start it with Start (for lifecycle hooks) or Run (blocking).
//     asynq.Config.Queues maps each queue the worker reads to its priority
//     weight and defaults to {"default": 1}: list every Definition.Queue the
//     worker processes.
//   - Scheduler enqueues taskqueue.PeriodicTask values on their schedules.
//     Build it with NewRedisScheduler; nil *asynq.SchedulerOpts means asynq
//     defaults. Every running scheduler enqueues on every fire, so run one per
//     deployment, or set WithUnique on each periodic task with a TTL shorter
//     than its interval, which suppresses a duplicate fire yet lets the next
//     one through.
//   - ProcessorRouter is the asynq.Handler inside Worker. Use it directly to
//     serve taskqueue processors from an asynq.Server you start yourself.
//
// # Mapping
//
// Task.Type becomes the asynq task type, Task.Payload the asynq payload, and
// Task.Headers the asynq headers. Task.PayloadCodec travels in the reserved
// header PayloadCodecHeader, which the adapter removes before processing.
// Task.Queue becomes asynq.Queue, "" meaning asynq's "default" queue, and
// Task.ID becomes asynq.TaskID. On the worker, Task.Queue is the queue the
// task came from and Task.ID is empty; taskqueue.ExecutionInfoFromContext
// gives the task ID (the caller's WithID value, or one asynq generated), the
// retry count, and max retry.
//
// Enqueue options map to asynq options: WithDelay to ProcessIn, WithProcessAt
// to ProcessAt, WithMaxRetry to MaxRetry (asynq defaults to 25), WithTimeout
// to Timeout, WithDeadline to Deadline, and WithUnique to Unique. A cron
// schedule becomes its spec, prefixed with CRON_TZ=<location> when WithLocation
// is set; an interval schedule becomes "@every <interval>".
//
// The caller validates enqueue policy, as taskqueue.Enqueuer states, and
// Client passes an unvalidated policy to asynq as is: with both WithDelay and
// WithProcessAt, WithProcessAt wins; a negative WithDelay processes at once; a
// negative WithMaxRetry means no retries; a WithUnique TTL below one second
// makes Enqueue fail with an asynq error.
//
// # Processing outcomes
//
//   - Process returns nil: the task completes and asynq deletes it at once,
//     since this adapter sets no asynq.Retention, freeing its ID.
//   - Process returns an error: asynq retries the task after a backoff
//     (asynq.Config.RetryDelayFunc) until it has been retried MaxRetry times,
//     then archives it. ErrUnhandledType, a timeout, and a passed deadline
//     count as such errors.
//   - Process returns an error wrapping taskqueue.ErrSkipRetry, including a
//     payload decode error: ProcessorRouter adds asynq.SkipRetry, and asynq
//     archives the task at once.
//   - Process panics: asynq recovers the panic and retries the task like an
//     error. Wrap the processor with taskqueue.Recover to get an error
//     matching taskqueue.ErrPanic.
//
// Archived tasks stay in Redis until asynq trims the archive (at 10,000 tasks
// or after 90 days) or an operator deletes or runs them with asynq tooling.
//
// # Task ID and duplicates
//
// asynq scopes a task ID to its queue. Within a queue it keeps the ID while
// the task is pending, scheduled, active, waiting to retry, or archived, and
// frees it when the task completes. While asynq keeps the ID, Enqueue returns
// an error matching taskqueue.ErrDuplicateTask. Because no Retention is set,
// an archived failure keeps its ID too: enqueueing that ID again returns
// ErrDuplicateTask until the archived task is deleted, run, or trimmed. Treat
// ErrDuplicateTask as "this work is already scheduled or has failed", and
// resolve archived failures with asynq tooling.
//
// WithUnique(ttl) compares task type, payload, and queue. asynq holds the
// uniqueness lock until the TTL expires or the task completes successfully.
// Enqueue reports a suppressed duplicate as an error matching
// taskqueue.ErrDuplicateTask, the same as an ID conflict: either way the work
// is already queued, so application code handles both with one errors.Is.
//
// # Lifecycle
//
// Construction and registration start nothing, and Worker.Register and
// Scheduler.RegisterPeriodicTask work before and after Start. Run reports
// only startup errors. Failures after startup surface through asynq hooks:
// asynq.Config.ErrorHandler for processor errors,
// asynq.Config.HealthCheckFunc for Redis connectivity, and
// asynq.SchedulerOpts.EnqueueErrorHandler for periodic enqueue failures.
//
// Shut down the Worker and Scheduler first, then Close the Client once
// nothing enqueues through it.
package asynq
