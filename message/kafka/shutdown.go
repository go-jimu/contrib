package kafka

import (
	"context"
	"sync"
)

// lifecycle carries a shutdown from Shutdown to Run. Run consumes until its
// stop ctx ends and hands its handlers ctxs derived from its force ctx:
// Shutdown ends stop at once and force only when its own ctx ends, while
// Run's ctx ending ends both.
type lifecycle struct {
	mu       sync.Mutex
	shutdown bool
	running  bool
	stop     context.CancelFunc
	force    context.CancelFunc
	done     chan struct{}
}

// begin starts a Run on ctx. It returns the stop ctx, the force ctx, and the
// func Run defers to mark its return, or ok false when Shutdown came first.
func (l *lifecycle) begin(ctx context.Context) (stop, force context.Context, end func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.shutdown {
		return nil, nil, nil, false
	}
	force, forceCancel := context.WithCancel(ctx)
	stop, stopCancel := context.WithCancel(force)
	done := make(chan struct{})
	l.running, l.stop, l.force, l.done = true, stopCancel, forceCancel, done
	return stop, force, func() {
		l.mu.Lock()
		l.running = false
		l.mu.Unlock()
		close(done)
		forceCancel()
	}, true
}

// shutdownRun implements Shutdown for both consumers; see Shutdown in the
// package documentation.
func (l *lifecycle) shutdownRun(ctx context.Context) error {
	l.mu.Lock()
	l.shutdown = true
	if !l.running {
		l.mu.Unlock()
		return nil
	}
	stop, force, done := l.stop, l.force, l.done
	l.mu.Unlock()

	stop()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		force()
		return ctx.Err()
	}
}
