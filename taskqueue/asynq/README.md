# Asynq Task Queue

Asynq provider for `github.com/go-jimu/components/taskqueue`, built on
`github.com/hibiken/asynq`.

The contracts live in the package documentation, which `go doc -all
github.com/go-jimu/contrib/taskqueue/asynq` prints: where the import belongs,
the Client, Worker, and Scheduler roles, the task and option mapping, the
processing outcomes (retry, `ErrSkipRetry` as `asynq.SkipRetry`, archive),
task ID retention and `ErrDuplicateTask`, lifecycle, and where failures after
startup surface. This README walks through complete programs;
`example_test.go` runs the `ErrSkipRetry` translation and compiles each role.

## Install

```sh
go get github.com/go-jimu/contrib/taskqueue/asynq@latest
```

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

Validate the options first when they come from configuration; the Client
passes them to asynq as they are.

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

`asynq.Config.Queues` must list `reconcile`, or the worker never reads that
queue.

## Periodic Task

Keep business work in a task processor; the scheduler only enqueues the
task. Build the scheduler in runtime wiring; application code provides the
`taskqueue.PeriodicTask`.

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
registering its `PeriodicTask`.
