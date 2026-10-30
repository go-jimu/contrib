# Asynq Task Queue

Asynq provider for `github.com/go-jimu/components/taskqueue`.

The package adapts `components/taskqueue` task contracts to
`github.com/hibiken/asynq`. Use it when an application wants to enqueue and
process component tasks through asynq without importing asynq from business
application code.

## Install

```sh
go get github.com/go-jimu/contrib/taskqueue/asynq@latest
```

## Quick Start Checklist

1. Create one `taskqueue.SchemaRegistry` in application composition code.
2. Register each task payload struct with a `taskqueue.Definition`.
3. Create an asynq client/worker in infrastructure wiring.
4. Wrap the asynq client with `taskasynq.NewClient` and enqueue component tasks.
5. Register one `taskqueue.Processor` per `TaskType` on `taskasynq.NewWorker`.
6. Start the worker by calling `Start(ctx)` from lifecycle code, or use
   `Run(ctx)` when a blocking runner is more convenient.
7. For periodic producers, create a `taskqueue.PeriodicTask` and register it
   on `taskasynq.NewRedisScheduler` from infrastructure/runtime wiring.

This package does not start HTTP services, Redis processes, or background
workers during construction or registration. `Start(ctx)` and `Run(ctx)` start
an asynq worker or scheduler, and they must be called explicitly by application
lifecycle code. `Shutdown(ctx)` stops accepting new task processing or periodic
enqueueing and delegates to asynq graceful shutdown.

## Mapping

| `taskqueue.Task` field | asynq mapping |
| --- | --- |
| `Type()` | asynq task type |
| `Payload()` | asynq task payload bytes |
| `Headers()` | asynq task headers |
| `PayloadCodec()` | the reserved asynq header `jimu-payload-codec` (`PayloadCodecHeader`), removed again before processors see the headers |
| `Queue()` | `asynq.Queue` enqueue option |
| `ID()` | `asynq.TaskID` enqueue option. Processors receive the ID asynq assigned, or the one the producer set, through `taskqueue.ExecutionInfo.TaskID()`; `Task.ID()` on the worker is empty |

| `taskqueue.Schedule` field | asynq scheduler mapping |
| --- | --- |
| `CronSchedule(spec)` | asynq cron spec |
| `CronSchedule(spec, WithLocation(location))` | `CRON_TZ=<location> <spec>` |
| `IntervalSchedule(interval)` | `@every <duration>` |
| `PeriodicTask.EnqueuePolicy()` | asynq enqueue options for each periodic enqueue |

`taskqueue.EnqueueOption` values map to asynq enqueue options:
`WithDelay`, `WithProcessAt`, `WithMaxRetry`, `WithTimeout`, `WithDeadline`,
and `WithUnique`.

### Task ID

asynq scopes a task ID to its queue: the same ID in two queues never
conflicts. Within a queue, asynq keeps the ID while the task is pending,
scheduled, active, retrying, or archived, and frees it as soon as the task
completes, since this adapter sets no `asynq.Retention`. Archived tasks stay
until asynq trims the archive (at 10,000 tasks or 90 days); delete or run them
from asynq tooling to reuse their IDs. Enqueueing an ID that asynq still keeps
returns an error that matches both `taskqueue.ErrDuplicateTask` and
`asynq.ErrTaskIDConflict` with `errors.Is`.

`WithUnique(ttl)` compares task type, payload, and queue. asynq holds the
uniqueness lock until the TTL expires or the original task completes
successfully, whichever comes first, and reports a duplicate as
`asynq.ErrDuplicateTask`.

### Registration and runtime errors

`Worker.Register` and `Scheduler.RegisterPeriodicTask` work before and after
`Start`. `Run` returns only startup and shutdown errors. Failures after
startup surface through asynq's own hooks: `asynq.Config.ErrorHandler` for
processor errors, `asynq.Config.HealthCheckFunc` for Redis connectivity, and
`asynq.SchedulerOpts.EnqueueErrorHandler` for periodic enqueue failures.

## Enqueue

```go
package main

import (
	"context"
	"time"

	"github.com/go-jimu/components/taskqueue"
	taskasynq "github.com/go-jimu/contrib/taskqueue/asynq"
	"github.com/hibiken/asynq"
)

type ReviewDocument struct {
	ID string `json:"id"`
}

func enqueue(ctx context.Context) error {
	def := taskqueue.Definition{Type: "document.review.v1", Queue: "reconcile"}

	registry := taskqueue.NewSchemaRegistry()
	if err := registry.Register(def, func() any { return &ReviewDocument{} }); err != nil {
		return err
	}

	client := taskasynq.NewRedisClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
	defer client.Close()

	task, err := registry.NewJSONTask(&ReviewDocument{ID: "doc-1"}, taskqueue.WithID("review:doc-1"))
	if err != nil {
		return err
	}

	return client.Enqueue(ctx, task, taskqueue.WithDelay(30*time.Second), taskqueue.WithMaxRetry(3))
}
```

## Process

```go
package main

import (
	"context"

	"github.com/go-jimu/components/taskqueue"
	taskasynq "github.com/go-jimu/contrib/taskqueue/asynq"
	"github.com/hibiken/asynq"
)

type ReviewDocument struct {
	ID string `json:"id"`
}

func run(ctx context.Context) error {
	def := taskqueue.Definition{Type: "document.review.v1", Queue: "reconcile"}

	registry := taskqueue.NewSchemaRegistry()
	if err := registry.Register(def, func() any { return &ReviewDocument{} }); err != nil {
		return err
	}

	worker := taskasynq.NewRedisWorker(
		asynq.RedisClientOpt{Addr: "localhost:6379"},
		asynq.Config{Queues: map[string]int{"reconcile": 1}},
	)

	if err := worker.Register(taskqueue.NewProcessor(def.Type, func(ctx context.Context, task taskqueue.Task) error {
		payload, err := registry.DecodeJSON(task)
		if err != nil {
			return err
		}
		review := payload.(*ReviewDocument)
		_ = review
		return nil
	})); err != nil {
		return err
	}

	return worker.Run(ctx)
}
```

## Periodic Task

A periodic task is a periodic producer for a concrete `taskqueue.Task`. Keep
business work in a task processor or application use case; the scheduler should
only enqueue the semantic task.

`NewRedisScheduler` is the recommended production entry point for the Redis
backed asynq scheduler adapter. Keep it in runtime wiring (for example an
`internal/pkg/taskqueue` package in an application); bounded-context or
Application code should only provide `taskqueue.PeriodicTask` values.

```go
package main

import (
	"context"
	"time"

	"github.com/go-jimu/components/taskqueue"
	taskasynq "github.com/go-jimu/contrib/taskqueue/asynq"
	"github.com/hibiken/asynq"
)

func runScheduler(ctx context.Context, task taskqueue.Task) error {
	scheduler := taskasynq.NewRedisScheduler(
		asynq.RedisClientOpt{Addr: "localhost:6379"},
		&asynq.SchedulerOpts{},
	)

	schedule, err := taskqueue.CronSchedule("0 2 * * *", taskqueue.WithLocation("Asia/Shanghai"))
	if err != nil {
		return err
	}
	periodic, err := taskqueue.NewPeriodicTask(
		"document.daily_review",
		schedule,
		task,
		taskqueue.WithUnique(25*time.Hour),
		taskqueue.WithMaxRetry(3),
	)
	if err != nil {
		return err
	}
	if err := scheduler.RegisterPeriodicTask(periodic); err != nil {
		return err
	}
	return scheduler.Run(ctx)
}
```

To disable a periodic producer from runtime configuration, skip constructing or
registering the `PeriodicTask` in application lifecycle wiring.

`RegisterPeriodicTask` detects duplicate `PeriodicTask.Name()` values within
one scheduler instance. Cross-process or cross-replica duplicate registration
prevention is outside this adapter; run one scheduler, use leader election or a
distributed lock, and keep task processors idempotent when multiple service
replicas may exist.

`taskqueue.ErrSkipRetry` is translated to `asynq.SkipRetry`, so processors can
make non-retryable decisions without importing asynq.

Asynq worker metadata is exposed through `taskqueue.ExecutionInfoFromContext`.
Processors and middleware can read provider task ID, queue, retry count, and
max retry from the context without importing asynq.

This package does not create a global schema registry. Applications own registry
instances and pass tasks/processors through normal dependency injection.
