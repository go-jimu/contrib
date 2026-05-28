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

func NewWorker(backend *hibiken.Server) *Worker {
	var runtime workerRuntime
	if backend != nil {
		runtime = backend
	}
	return newWorker(runtime, NewProcessorRouter())
}

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

func (w *Worker) Register(processor taskqueue.Processor) error {
	return w.router.Register(processor)
}

func (w *Worker) ProcessTask(ctx context.Context, task *hibiken.Task) error {
	return w.router.ProcessTask(ctx, task)
}

func (w *Worker) Start(ctx context.Context) error {
	if w.runtime == nil {
		return ErrNilWorker
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.runtime.Start(w.router)
}

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
