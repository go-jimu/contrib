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

This package does not start HTTP services, Redis processes, or background
workers during construction or registration. `Start(ctx)` and `Run(ctx)` are
the only helpers that start an asynq worker, and they must be called explicitly
by application lifecycle code. `Shutdown(ctx)` stops accepting new tasks and
delegates to asynq graceful shutdown.

## Mapping

| `taskqueue.Task` field | asynq mapping |
| --- | --- |
| `Type()` | asynq task type |
| `Payload()` | asynq task payload bytes |
| `Headers()` | asynq task headers |
| `Queue()` | `asynq.Queue` enqueue option |
| `Key()` | `asynq.TaskID` enqueue option |

`taskqueue.EnqueueOption` values map to asynq enqueue options:
`WithDelay`, `WithProcessAt`, `WithMaxRetry`, `WithTimeout`, `WithDeadline`,
and `WithUnique`.

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

	task, err := registry.NewJSONTask(&ReviewDocument{ID: "doc-1"}, taskqueue.WithKey("doc-1"))
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

`taskqueue.ErrSkipRetry` is translated to `asynq.SkipRetry`, so processors can
make non-retryable decisions without importing asynq.

Asynq worker metadata is exposed through `taskqueue.ExecutionInfoFromContext`.
Processors and middleware can read provider task ID, queue, retry count, and
max retry from the context without importing asynq.

This package does not create a global schema registry. Applications own registry
instances and pass tasks/processors through normal dependency injection.
