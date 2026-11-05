// Package asynq adapts github.com/hibiken/asynq, a Redis-backed task queue,
// to github.com/go-jimu/components/taskqueue.
//
// # Where it belongs
//
// Import this package and github.com/hibiken/asynq only in runtime
// composition code, such as an internal/pkg/taskqueue package, which builds
// the Client, Worker, and Scheduler, registers processors and periodic tasks
// on them, and starts and stops them. Application code depends on
// taskqueue.Enqueuer, implements taskqueue.Processor, and provides
// taskqueue.PeriodicTask values; it imports neither package. Two results still
// carry asynq types: EnqueueInfo returns *asynq.TaskInfo, and a WithUnique
// duplicate is reported as asynq.ErrDuplicateTask (see Errors). Keep code that
// needs them in the runtime layer.
//
// # Roles
//
//   - Client implements taskqueue.Enqueuer for every process that enqueues.
//     Build it with NewRedisClient, or NewClient around an existing
//     *asynq.Client. It is safe for concurrent use; Close closes the asynq
//     client in both cases.
//   - Worker processes tasks. Build it with NewRedisWorker, or NewWorker
//     around an *asynq.Server, register one taskqueue.Processor per TaskType,
//     and start it with Start (for lifecycle hooks) or Run (blocking). A
//     worker reads only the queues listed in asynq.Config.Queues, which
//     defaults to the "default" queue alone: list every Definition.Queue the
//     worker should process. A worker starts once; after Shutdown, Start
//     fails.
//   - Scheduler enqueues taskqueue.PeriodicTask values on their schedules.
//     Build it with NewRedisScheduler. Every running scheduler enqueues on
//     every fire, so run one per deployment, use leader election, or set
//     WithUnique on each periodic task.
//   - ProcessorRouter is the asynq.Handler inside Worker. Use it directly to
//     serve taskqueue processors from an asynq.Server you start yourself.
//
// # Mapping
//
// Task.Type becomes the asynq task type, Task.Payload the asynq payload, and
// Task.Headers the asynq headers. Task.PayloadCodec travels in the reserved
// header PayloadCodecHeader, which processors do not see. Task.Queue becomes
// asynq.Queue, "" meaning asynq's "default" queue, and Task.ID becomes
// asynq.TaskID. On the worker, Task.Queue is the queue the task came from and
// Task.ID is empty: read the asynq task ID, retry count, and max retry from
// taskqueue.ExecutionInfoFromContext.
//
// Enqueue options map to asynq options: WithDelay to ProcessIn, WithProcessAt
// to ProcessAt, WithMaxRetry to MaxRetry (asynq defaults to 25), WithTimeout
// to Timeout, WithDeadline to Deadline, and WithUnique to Unique. A cron
// schedule becomes its spec, prefixed with CRON_TZ=<location> when WithLocation
// is set; an interval schedule becomes "@every <interval>".
//
// Client does not call EnqueueOptions.Validate; the caller validates, as
// taskqueue.Enqueuer states. An unvalidated policy reaches asynq as is: with
// both WithDelay and WithProcessAt, WithProcessAt wins; a negative WithDelay
// processes at once; a negative WithMaxRetry means no retries; a WithUnique
// TTL below one second makes Enqueue fail with an asynq error.
//
// # Processing outcomes
//
//   - Process returns nil: the task completes and asynq deletes it at once,
//     since this adapter sets no asynq.Retention, freeing its ID.
//   - Process returns an error: asynq retries the task after a backoff
//     (asynq.Config.RetryDelayFunc) until it has been retried MaxRetry times,
//     then archives it. ErrUnhandledType, a timeout, and a passed deadline
//     count as such errors.
//   - Process returns an error wrapping taskqueue.ErrSkipRetry, including any
//     decode error: ProcessorRouter translates it to asynq.SkipRetry, and
//     asynq archives the task at once without retrying.
//   - Process panics: asynq recovers the panic and treats it as an error, so
//     the task is retried. Wrap the processor with taskqueue.Recover to get an
//     error matching taskqueue.ErrPanic instead.
//
// Archived tasks stay in Redis until asynq trims the archive (at 10,000 tasks
// or after 90 days) or an operator deletes or runs them with asynq tooling.
//
// # Task ID and duplicates
//
// asynq scopes a task ID to its queue. Within a queue it keeps the ID while
// the task is pending, scheduled, active, waiting to retry, or archived, and
// frees it when the task completes. Enqueue returns an error matching both
// taskqueue.ErrDuplicateTask and asynq.ErrTaskIDConflict while asynq keeps the
// ID. Because no Retention is set, an archived failure keeps blocking its ID:
// enqueueing the same ID again returns ErrDuplicateTask until the archived
// task is deleted, run, or trimmed. Treat ErrDuplicateTask as "already
// scheduled or failed", and resolve archived failures through asynq tooling.
//
// WithUnique(ttl) compares task type, payload, and queue. asynq holds the
// uniqueness lock until the TTL expires or the task completes successfully,
// and reports a duplicate as asynq.ErrDuplicateTask, which does not match
// taskqueue.ErrDuplicateTask.
//
// # Lifecycle and runtime errors
//
// Construction and registration start nothing. Worker.Register and
// Scheduler.RegisterPeriodicTask work before and after Start. Start returns
// after the runtime has started; Run starts it, blocks until ctx ends, shuts
// down, and returns nil, so Run reports only startup errors. Shutdown stops
// fetching new work and waits for running tasks up to
// asynq.Config.ShutdownTimeout (8 seconds by default); asynq returns tasks
// still running to their queue. Shutdown returns ctx.Err() if ctx ends first.
//
// Failures after startup surface through asynq hooks, not through this
// package: asynq.Config.ErrorHandler for processor errors,
// asynq.Config.HealthCheckFunc for Redis connectivity, and
// asynq.SchedulerOpts.EnqueueErrorHandler for periodic enqueue failures.
//
// # Errors
//
// ErrNilClient, ErrNilWorker, and ErrNilScheduler report a component built
// around a nil asynq value. Enqueue also returns taskqueue.ErrEmptyType for a
// task without a type, an error matching taskqueue.ErrDuplicateTask for an ID
// conflict, asynq.ErrDuplicateTask for a WithUnique duplicate, and asynq or
// Redis errors otherwise. RegisterPeriodicTask returns the
// taskqueue.PeriodicTask validation errors and
// taskqueue.ErrDuplicatePeriodicTask for a name already registered on that
// scheduler.
package asynq
