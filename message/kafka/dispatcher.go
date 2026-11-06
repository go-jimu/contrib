package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/go-jimu/components/sloghelper"
	"github.com/twmb/franz-go/pkg/kgo"
)

// dispatcher decodes records and runs their handlers; Consumer and
// ShareConsumer share it.
type dispatcher struct {
	cfg    config
	router *message.Router
	log    *slog.Logger
}

func newDispatcher(cfg config) dispatcher {
	return dispatcher{cfg: cfg, router: message.NewRouter(), log: loggerOrDefault(cfg.logger)}
}

// Subscribe registers handler with a message.Router; see message.Router.Subscribe
// for its errors. Subscribe every handler before Run starts: a record whose
// Kind has no handler fails at StageUnhandled.
func (d *dispatcher) Subscribe(handler message.Handler) error {
	return d.router.Subscribe(handler)
}

// recordContext is the ctx of record's work (see WithRecordContext), derived
// from ctx, Run's force ctx.
func (d *dispatcher) recordContext(ctx context.Context, record *kgo.Record) context.Context {
	if d.cfg.recordContext == nil {
		return ctx
	}
	if derived := d.cfg.recordContext(ctx, record); derived != nil {
		return derived
	}
	return ctx
}

// dispatch decodes record and runs its handlers within timeout, on a ctx
// derived from recordCtx; ctx, Run's force ctx, tells a forced stop apart. On
// failure it returns the stage that failed: StageDecode, StageUnhandled, or
// StageHandle.
func (d *dispatcher) dispatch(ctx, recordCtx context.Context, record *kgo.Record, timeout time.Duration) (message.Message, Stage, error) {
	msg, err := recordToMessage(record, d.cfg)
	if err != nil {
		return msg, StageDecode, err
	}
	err = d.handle(ctx, recordCtx, record, msg, timeout)
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

// handle routes msg to its handlers on a ctx derived from recordCtx, bounded
// by timeout when it is positive. ctx is Run's force ctx. When ctx ends first,
// it still waits for the handlers to return, up to the rest of the timeout,
// so that Run returns after them.
func (d *dispatcher) handle(ctx, recordCtx context.Context, record *kgo.Record, msg message.Message, timeout time.Duration) error {
	if timeout <= 0 {
		return d.callHandlers(recordCtx, msg)
	}

	start := time.Now()
	handlerCtx, cancel := context.WithTimeoutCause(recordCtx, timeout, ErrHandlerTimeout)
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
			d.logStillRunning(recordCtx, "kafka handler still running at shutdown", record, msg, timeout)
			return ctx.Err()
		}
	}
	select {
	case err := <-done:
		return timeoutError(ctx, handlerCtx, timeout, err)
	default:
		d.logStillRunning(recordCtx, "kafka handler still running after timeout", record, msg, timeout)
		return fmt.Errorf("%w after %s", ErrHandlerTimeout, timeout)
	}
}

// timeoutError marks err as a timeout when the handlers failed because their
// timeout, not a forced stop, ended.
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
		d.log.LogAttrs(ctx, slog.LevelWarn, "kafka poll error", sloghelper.Error(err))
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

// logStillRunning warns that a handler goroutine outlives the consumer's wait
// for it.
func (d *dispatcher) logStillRunning(ctx context.Context, msg string, record *kgo.Record, m message.Message, timeout time.Duration) {
	attrs := append(recordAttrs(record),
		slog.String("message_kind", string(m.Kind())),
		slog.String("message_id", m.ID()),
		durationMillis("timeout_ms", timeout),
	)
	d.log.LogAttrs(ctx, slog.LevelWarn, msg, attrs...)
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
