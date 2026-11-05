package asynq

import (
	"context"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

type workerRuntime interface {
	Start(hibiken.Handler) error
	Shutdown()
}

type stopper interface {
	Stop()
}

// Worker processes asynq tasks with registered taskqueue processors. It
// implements taskqueue.Registrar, taskqueue.Worker, and taskqueue.Runner.
type Worker struct {
	runtime workerRuntime
	router  *ProcessorRouter
}

var (
	_ taskqueue.Registrar = (*Worker)(nil)
	_ taskqueue.Runner    = (*Worker)(nil)
	_ taskqueue.Worker    = (*Worker)(nil)
	_ hibiken.Handler     = (*Worker)(nil)
)

// NewWorker wraps an asynq server, which Start and Shutdown control. It
// starts nothing.
func NewWorker(backend *hibiken.Server) *Worker {
	var runtime workerRuntime
	if backend != nil {
		runtime = backend
	}
	return newWorker(runtime, NewProcessorRouter())
}

// NewRedisWorker creates an asynq server for conn and cfg and wraps it. List
// every queue the worker should read in cfg.Queues; set cfg.ErrorHandler to
// observe processor failures.
func NewRedisWorker(conn hibiken.RedisConnOpt, cfg hibiken.Config) *Worker {
	return NewWorker(hibiken.NewServer(conn, cfg))
}

func newWorker(runtime workerRuntime, router *ProcessorRouter) *Worker {
	if router == nil {
		router = NewProcessorRouter()
	}
	return &Worker{
		runtime: runtime,
		router:  router,
	}
}

// Register adds processor for its task type, before or after Start. It returns
// the taskqueue.Router errors: ErrNilProcessor, ErrEmptyType, or
// ErrDuplicateProcessor.
func (w *Worker) Register(processor taskqueue.Processor) error {
	return w.router.Register(processor)
}

// ProcessTask implements asynq.Handler; see ProcessorRouter.ProcessTask.
func (w *Worker) ProcessTask(ctx context.Context, task *hibiken.Task) error {
	return w.router.ProcessTask(ctx, task)
}

// Start starts the asynq server and returns once it runs. It returns
// ctx.Err() if ctx is already done, ErrNilWorker, or the asynq error for a
// server that is already running or was shut down.
func (w *Worker) Start(ctx context.Context) error {
	if w.runtime == nil {
		return ErrNilWorker
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.runtime.Start(w.router)
}

// Shutdown stops fetching tasks and waits for running tasks up to the asynq
// ShutdownTimeout; asynq returns unfinished tasks to their queue. It returns
// nil when shutdown completes, or ctx.Err() if ctx ends first while asynq
// keeps shutting down in the background.
func (w *Worker) Shutdown(ctx context.Context) error {
	if w.runtime == nil {
		return ErrNilWorker
	}
	if stopper, ok := w.runtime.(stopper); ok {
		stopper.Stop()
	}

	done := make(chan struct{})
	go func() {
		w.runtime.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts the worker, blocks until ctx ends, then shuts it down and returns
// nil. It returns an error only when the worker cannot start, and nil at once
// when ctx is already done.
func (w *Worker) Run(ctx context.Context) error {
	if w.runtime == nil {
		return ErrNilWorker
	}
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	if err := w.Start(context.Background()); err != nil {
		return err
	}
	<-ctx.Done()
	return w.Shutdown(context.Background())
}
