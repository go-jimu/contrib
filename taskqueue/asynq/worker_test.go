package asynq

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// Intent: Worker should expose component registration and runner capabilities
// while delegating lifecycle mechanics to the underlying asynq runtime.
func TestWorkerRunStartsAndShutsDownWithContext(t *testing.T) {
	fake := &fakeWorkerRuntime{startedCh: make(chan struct{})}
	worker := newWorker(fake, NewProcessorRouter())
	if err := worker.Register(taskqueue.NewProcessor("document.review.v1", func(context.Context, taskqueue.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	<-fake.startedCh
	cancel()
	err := <-errCh
	if err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if !fake.started {
		t.Fatal("asynq worker runtime was not started")
	}
	if !fake.shutdown {
		t.Fatal("asynq worker runtime was not shut down")
	}
	if fake.handler == nil {
		t.Fatal("asynq handler was not supplied")
	}
}

// Intent: Worker should expose a Start/Shutdown lifecycle so application
// runtime hooks can start workers and drain them without using Run.
func TestWorkerLifecycleStartsAndShutsDown(t *testing.T) {
	fake := &fakeWorkerRuntime{}
	worker := newWorker(fake, NewProcessorRouter())

	if err := worker.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := worker.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if !fake.started {
		t.Fatal("asynq worker runtime was not started")
	}
	if !fake.stopped {
		t.Fatal("asynq worker runtime was not stopped before shutdown")
	}
	if !fake.shutdown {
		t.Fatal("asynq worker runtime was not shut down")
	}
	if fake.handler == nil {
		t.Fatal("asynq handler was not supplied")
	}
}

// Intent: Runner cancellation is a normal shutdown signal. If the context is
// already canceled, Run should not start a worker only to return ctx.Err().
func TestWorkerRunAlreadyCanceledDoesNotStart(t *testing.T) {
	fake := &fakeWorkerRuntime{}
	worker := newWorker(fake, NewProcessorRouter())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := worker.Run(ctx)

	if err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if fake.started {
		t.Fatal("asynq worker runtime was started for an already canceled context")
	}
	if fake.shutdown {
		t.Fatal("asynq worker runtime was shut down without being started")
	}
}

// Intent: Worker should fail clearly when no asynq worker runtime is configured.
func TestWorkerRunRejectsNilRuntime(t *testing.T) {
	worker := newWorker(nil, NewProcessorRouter())

	err := worker.Run(context.Background())

	if !errors.Is(err, ErrNilWorker) {
		t.Fatalf("error = %v, want ErrNilWorker", err)
	}
}

type fakeWorkerRuntime struct {
	started   bool
	stopped   bool
	shutdown  bool
	handler   hibiken.Handler
	err       error
	startedCh chan struct{}
}

func (f *fakeWorkerRuntime) Start(handler hibiken.Handler) error {
	f.started = true
	f.handler = handler
	if f.startedCh != nil {
		close(f.startedCh)
	}
	return f.err
}

func (f *fakeWorkerRuntime) Stop() {
	f.stopped = true
}

func (f *fakeWorkerRuntime) Shutdown() {
	f.shutdown = true
}
