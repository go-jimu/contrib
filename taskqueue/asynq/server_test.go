package asynq

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jimu/components/taskqueue"
	hibiken "github.com/hibiken/asynq"
)

// Intent: Server should expose component registration and runner capabilities
// while delegating worker lifecycle to the underlying asynq server.
func TestServerRunStartsAndShutsDownWithContext(t *testing.T) {
	fake := &fakeServer{startedCh: make(chan struct{})}
	server := newServer(fake, NewServeMux())
	if err := server.Register(taskqueue.NewProcessor("document.review.v1", func(context.Context, taskqueue.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Run(ctx)
	}()

	<-fake.startedCh
	cancel()
	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if !fake.started {
		t.Fatal("asynq server was not started")
	}
	if !fake.shutdown {
		t.Fatal("asynq server was not shut down")
	}
	if fake.handler == nil {
		t.Fatal("asynq handler was not supplied")
	}
}

// Intent: Server should fail clearly when no asynq server is configured.
func TestServerRunRejectsNilServer(t *testing.T) {
	server := newServer(nil, NewServeMux())

	err := server.Run(context.Background())

	if !errors.Is(err, ErrNilServer) {
		t.Fatalf("error = %v, want ErrNilServer", err)
	}
}

type fakeServer struct {
	started  bool
	shutdown bool
	handler  hibiken.Handler
	err      error
	startedCh chan struct{}
}

func (f *fakeServer) Start(handler hibiken.Handler) error {
	f.started = true
	f.handler = handler
	if f.startedCh != nil {
		close(f.startedCh)
	}
	return f.err
}

func (f *fakeServer) Shutdown() {
	f.shutdown = true
}
