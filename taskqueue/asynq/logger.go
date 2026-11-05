package asynq

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	hibiken "github.com/hibiken/asynq"
)

// NewLogger adapts logger to asynq.Logger, for asynq.Config.Logger and
// asynq.SchedulerOpts.Logger, so that asynq's own logs, such as Redis
// connection errors, worker shutdown, and lease extensions, reach the same
// place as the application's. A nil logger means slog.Default(). See Logging
// in the package documentation.
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
	if l.logger.Enabled(ctx, level) {
		l.logger.LogAttrs(ctx, level, fmt.Sprint(args...))
	}
}
