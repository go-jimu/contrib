package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

// dispatcher decodes records and runs their handlers; Consumer and
// ShareConsumer share it.
type dispatcher struct {
	cfg    config
	router *message.Router
}

func newDispatcher(cfg config) dispatcher {
	return dispatcher{cfg: cfg, router: message.NewRouter()}
}

// Subscribe registers handler with a message.Router; see message.Router.Subscribe
// for its errors. Subscribe every handler before Run starts: a record whose
// Kind has no handler fails at StageUnhandled.
func (d *dispatcher) Subscribe(handler message.Handler) error {
	return d.router.Subscribe(handler)
}

// dispatch decodes record and runs its handlers within timeout. On failure it returns the
// stage that failed: StageDecode, StageUnhandled, or StageHandle.
func (d *dispatcher) dispatch(ctx context.Context, record *kgo.Record, timeout time.Duration) (message.Message, Stage, error) {
	msg, err := recordToMessage(record, d.cfg)
	if err != nil {
		return msg, StageDecode, err
	}
	err = d.handle(ctx, msg, timeout)
	if err != nil && !errors.Is(err, ErrHandlerPanic) && errors.Is(err, message.ErrUnhandledKind) {
		return msg, StageUnhandled, err
	}
	return msg, StageHandle, err
}

func (d *dispatcher) observe(ctx context.Context, failure Error) {
	if d.cfg.errorObserver != nil {
		d.cfg.errorObserver(ctx, failure)
	}
}

// handle routes msg to its handlers, bounded by timeout when it is positive.
// When ctx ends first, it still waits for the handlers to return, up to the
// rest of the timeout, so that Run returns after them.
func (d *dispatcher) handle(ctx context.Context, msg message.Message, timeout time.Duration) error {
	if timeout <= 0 {
		return d.callHandlers(ctx, msg)
	}

	start := time.Now()
	handlerCtx, cancel := context.WithTimeoutCause(ctx, timeout, ErrHandlerTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- d.callHandlers(handlerCtx, msg)
	}()

	select {
	case err := <-done:
		return timeoutError(ctx, handlerCtx, timeout, err)
	case <-handlerCtx.Done():
	}

	if ctx.Err() != nil {
		wait := time.NewTimer(max(timeout-time.Since(start), 0))
		defer wait.Stop()
		select {
		case err := <-done:
			return err
		case <-wait.C:
			return ctx.Err()
		}
	}
	select {
	case err := <-done:
		return timeoutError(ctx, handlerCtx, timeout, err)
	default:
		return fmt.Errorf("%w after %s", ErrHandlerTimeout, timeout)
	}
}

// timeoutError marks err as a timeout when the handlers failed because their
// timeout, not Run's ctx, ended.
func timeoutError(ctx, handlerCtx context.Context, timeout time.Duration, err error) error {
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(handlerCtx), ErrHandlerTimeout) && !errors.Is(err, ErrHandlerTimeout) {
		return fmt.Errorf("%w after %s: %w", ErrHandlerTimeout, timeout, err)
	}
	return err
}

// pollError observes the errors of a poll and returns the first one that
// stops Run. The ended ctx of a shutdown is not a failure, and data loss and a
// lost group session are informational: franz-go recovers from them.
func (d *dispatcher) pollError(ctx context.Context, fetches kgo.Fetches) error {
	var stop error
	for _, fetchErr := range fetches.Errors() {
		err := fetchErr.Err
		if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			continue
		}
		d.observe(ctx, Error{Stage: StagePoll, Err: err})
		var dataLoss *kgo.ErrDataLoss
		var session *kgo.ErrGroupSession
		if errors.As(err, &dataLoss) || errors.As(err, &session) {
			continue
		}
		if stop == nil {
			stop = err
		}
	}
	return stop
}

// callHandlers recovers a handler panic as a *PanicError, so it follows the
// failure policy instead of ending the process.
func (d *dispatcher) callHandlers(ctx context.Context, msg message.Message) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &PanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	return d.router.Handle(ctx, msg)
}
