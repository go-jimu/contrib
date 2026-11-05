package asynq_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-jimu/components/taskqueue"
	taskasynq "github.com/go-jimu/contrib/taskqueue/asynq"
	"github.com/hibiken/asynq"
)

type welcomeEmail struct {
	UserID string `json:"user_id"`
}

// ExampleProcessorRouter shows how processor errors reach asynq: an error
// wrapping taskqueue.ErrSkipRetry also matches asynq.SkipRetry, so asynq
// archives the task at once; any other error is retried.
func ExampleProcessorRouter() {
	router := taskasynq.NewProcessorRouter()
	err := router.Register(taskqueue.NewProcessor("email.welcome", func(_ context.Context, task taskqueue.Task) error {
		var payload welcomeEmail
		if err := taskqueue.DecodeJSON(task, &payload); err != nil {
			return err
		}
		if payload.UserID == "deleted" {
			return fmt.Errorf("%w: user is deleted", taskqueue.ErrSkipRetry)
		}
		return errors.New("smtp unavailable")
	}))
	if err != nil {
		panic(err)
	}

	for _, body := range []string{`{"user_id":"deleted"}`, `{"user_id":"user-1"}`} {
		err := router.ProcessTask(context.Background(), asynq.NewTask("email.welcome", []byte(body)))
		fmt.Println(errors.Is(err, asynq.SkipRetry), err)
	}

	// Output:
	// true skip retry for the task: skip retry for task: user is deleted
	// false smtp unavailable
}

// ExampleNewRedisClient composes the Client in runtime code and hands it to
// application code as a taskqueue.Enqueuer. It needs Redis, so it is compiled
// but not run.
func ExampleNewRedisClient() {
	client := taskasynq.NewRedisClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
	defer client.Close()

	var enqueuer taskqueue.Enqueuer = client

	task, err := taskqueue.NewJSONTask(
		taskqueue.Definition{Type: "email.welcome", Queue: "mailers"},
		welcomeEmail{UserID: "user-1"},
		taskqueue.WithID("welcome-email:user-1"),
	)
	if err != nil {
		panic(err)
	}
	opts := []taskqueue.EnqueueOption{taskqueue.WithDelay(time.Minute), taskqueue.WithMaxRetry(3)}
	if err := taskqueue.NewEnqueueOptions(opts...).Validate(); err != nil {
		panic(err) // the caller validates; Enqueue does not
	}

	err = enqueuer.Enqueue(context.Background(), task, opts...)
	if errors.Is(err, taskqueue.ErrDuplicateTask) {
		// Pending, retrying, or archived after failing: already scheduled.
		err = nil
	}
	if err != nil {
		panic(err)
	}
}

// ExampleNewRedisWorker composes a Worker in runtime code. It needs Redis, so
// it is compiled but not run.
func ExampleNewRedisWorker() {
	worker := taskasynq.NewRedisWorker(
		asynq.RedisClientOpt{Addr: "localhost:6379"},
		asynq.Config{
			// List every Definition.Queue this worker processes.
			Queues: map[string]int{"mailers": 1},
			// Processor failures after startup surface here, not from Run.
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				slog.ErrorContext(ctx, "task failed", slog.String("type", task.Type()), slog.Any("error", err))
			}),
		},
	)

	err := worker.Register(taskqueue.NewProcessor("email.welcome", taskqueue.Chain(
		func(ctx context.Context, task taskqueue.Task) error {
			var payload welcomeEmail
			if err := taskqueue.DecodeJSON(task, &payload); err != nil {
				return err
			}
			info, _ := taskqueue.ExecutionInfoFromContext(ctx)
			_ = info.TaskID() // asynq's task ID; task.ID() is empty here
			return nil
		},
		taskqueue.Recover(),
	)))
	if err != nil {
		panic(err)
	}

	// Lifecycle hooks call worker.Start and worker.Shutdown; Run blocks
	// until ctx ends and shuts down.
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if err := worker.Run(ctx); err != nil {
		panic(err)
	}
}

// ExampleNewRedisScheduler composes a Scheduler in runtime code; application
// code supplies the PeriodicTask. Run one scheduler per deployment. It needs
// Redis, so it is compiled but not run.
func ExampleNewRedisScheduler() {
	scheduler := taskasynq.NewRedisScheduler(asynq.RedisClientOpt{Addr: "localhost:6379"}, &asynq.SchedulerOpts{})

	task, err := taskqueue.NewJSONTask(taskqueue.Definition{Type: "reports.daily", Queue: "reports"}, struct{}{})
	if err != nil {
		panic(err)
	}
	schedule, err := taskqueue.CronSchedule("0 2 * * *", taskqueue.WithLocation("UTC"))
	if err != nil {
		panic(err)
	}
	periodic, err := taskqueue.NewPeriodicTask("reports.daily", schedule, task, taskqueue.WithUnique(23*time.Hour))
	if err != nil {
		panic(err)
	}
	if err := scheduler.RegisterPeriodicTask(periodic); err != nil {
		panic(err)
	}

	ctx, stop := context.WithCancel(context.Background())
	stop()
	if err := scheduler.Run(ctx); err != nil {
		panic(err)
	}
}
