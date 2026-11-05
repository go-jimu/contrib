package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

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

// dispatch decodes record and runs its handlers. On failure it returns the
// stage that failed: StageDecode, StageUnhandled, or StageHandle.
func (d *dispatcher) dispatch(ctx context.Context, record *kgo.Record) (message.Message, Stage, error) {
	msg, err := recordToMessage(record, d.cfg)
	if err != nil {
		return msg, StageDecode, err
	}
	err = d.handle(ctx, msg)
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

// handle routes msg to its handlers within the handler timeout, if any.
func (d *dispatcher) handle(ctx context.Context, msg message.Message) error {
	timeout := d.cfg.handlerTimeout
	if timeout <= 0 {
		return d.callHandlers(ctx, msg)
	}

	handlerCtx, cancel := context.WithTimeoutCause(ctx, timeout, ErrHandlerTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- d.callHandlers(handlerCtx, msg)
	}()

	var err error
	select {
	case err = <-done:
	case <-handlerCtx.Done():
		select {
		case err = <-done:
		default:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w after %s", ErrHandlerTimeout, timeout)
		}
	}
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(handlerCtx), ErrHandlerTimeout) && !errors.Is(err, ErrHandlerTimeout) {
		return fmt.Errorf("%w after %s: %w", ErrHandlerTimeout, timeout, err)
	}
	return err
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
