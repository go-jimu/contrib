package asynq

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	hibiken "github.com/hibiken/asynq"
)

// NewLogger adapts logger to asynq.Logger, for asynq.Config.Logger and
// asynq.SchedulerOpts.Logger, so that asynq's own logs, such as Redis
// connection errors, worker shutdown, and lease extensions, reach the same
// place as the application's. Each record's source is the asynq function
// that logged it. A nil logger means slog.Default(). See Logging in the
// package documentation.
func NewLogger(logger *slog.Logger) hibiken.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &slogLogger{logger: logger, exit: os.Exit}
}

type slogLogger struct {
	logger *slog.Logger
	exit   func(int)
}

func (l *slogLogger) Debug(args ...any) { l.log(slog.LevelDebug, args) }
func (l *slogLogger) Info(args ...any)  { l.log(slog.LevelInfo, args) }
func (l *slogLogger) Warn(args ...any)  { l.log(slog.LevelWarn, args) }
func (l *slogLogger) Error(args ...any) { l.log(slog.LevelError, args) }

// Fatal logs at Error and exits with status 1, as asynq.Logger requires.
func (l *slogLogger) Fatal(args ...any) {
	l.log(slog.LevelError, args)
	l.exit(1)
}

func (l *slogLogger) log(level slog.Level, args []any) {
	ctx := context.Background()
	if !l.logger.Enabled(ctx, level) {
		return
	}
	record := slog.NewRecord(time.Now(), level, fmt.Sprint(args...), callerPC())
	_ = l.logger.Handler().Handle(ctx, record)
}

// logSkip names the functions between an asynq call site and slog: this
// adapter and asynq's internal logger.
var logSkip = []string{
	"github.com/go-jimu/contrib/taskqueue/asynq.(*slogLogger).",
	"github.com/hibiken/asynq/internal/log.",
}

// callerPC returns the program counter of the nearest caller outside
// logSkip, for a record's source; 0, no source, when there is none.
func callerPC() uintptr {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:]) // skip runtime.Callers and callerPC
	for _, pc := range pcs[:n] {
		frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()
		if !slices.ContainsFunc(logSkip, func(prefix string) bool {
			return strings.HasPrefix(frame.Function, prefix)
		}) {
			return pc
		}
	}
	return 0
}
