package kafka

import (
	"context"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/go-jimu/components/sloghelper"
	"github.com/twmb/franz-go/pkg/kgo"
)

// recordAttrs locate a record: kafka_topic, kafka_partition, kafka_offset.
func recordAttrs(record *kgo.Record) []slog.Attr {
	if record == nil {
		return nil
	}
	return []slog.Attr{
		slog.String("kafka_topic", record.Topic),
		slog.Int("kafka_partition", int(record.Partition)),
		slog.Int64("kafka_offset", record.Offset),
	}
}

// failureAttrs describe a message-level failure: where the record is, which
// message it holds once decoded, and the failure itself.
func failureAttrs(failure Error) []slog.Attr {
	attrs := recordAttrs(failure.Record)
	if failure.Message.Kind() != "" {
		attrs = append(attrs,
			slog.String("message_kind", string(failure.Message.Kind())),
			slog.String("message_id", failure.Message.ID()),
		)
	}
	return append(attrs,
		slog.String("stage", string(failure.Stage)),
		slog.Int("attempt", failure.Attempt),
		slog.Bool("final", failure.Final),
		sloghelper.Error(failure.Err),
	)
}

func millis(d time.Duration) slog.Attr {
	return slog.Int64("elapsed_ms", d.Milliseconds())
}

func durationMillis(key string, d time.Duration) slog.Attr {
	return slog.Int64(key, d.Milliseconds())
}

// KgoLogger adapts logger to kgo.Logger, for kgo.WithLogger, so that the
// franz-go client's own logs (group joins, partition assignments, broker
// connections) reach the same place as the consumers'. Each record's source
// is the franz-go function that logged it. level is the most verbose
// franz-go level to log; kgo.LogLevelInfo suits production, since
// kgo.LogLevelDebug logs every request. A nil logger means slog.Default().
func KgoLogger(logger *slog.Logger, level kgo.LogLevel) kgo.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return kgoLogger{logger: logger, level: level, skip: kgoLogSkip}
}

// kgoLogSkip names the functions between a franz-go call site and slog:
// this adapter and franz-go's own logger wrapper.
var kgoLogSkip = []string{
	"github.com/go-jimu/contrib/message/kafka.kgoLogger.",
	"github.com/twmb/franz-go/pkg/kgo.(*wrappedLogger).",
}

type kgoLogger struct {
	logger *slog.Logger
	level  kgo.LogLevel
	skip   []string
}

func (l kgoLogger) Level() kgo.LogLevel { return l.level }

func (l kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var slogLevel slog.Level
	switch level {
	case kgo.LogLevelError:
		slogLevel = slog.LevelError
	case kgo.LogLevelWarn:
		slogLevel = slog.LevelWarn
	case kgo.LogLevelInfo:
		slogLevel = slog.LevelInfo
	default:
		slogLevel = slog.LevelDebug
	}
	ctx := context.Background()
	if !l.logger.Enabled(ctx, slogLevel) {
		return
	}
	record := slog.NewRecord(time.Now(), slogLevel, msg, callerPC(l.skip))
	record.Add(keyvals...)
	_ = l.logger.Handler().Handle(ctx, record)
}

// callerPC returns the program counter of the nearest caller whose function
// matches no prefix in skip, for a record's source; 0, no source, when the
// stack holds no such caller.
func callerPC(skip []string) uintptr {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:]) // skip runtime.Callers and callerPC
	for _, pc := range pcs[:n] {
		frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()
		if !slices.ContainsFunc(skip, func(prefix string) bool {
			return strings.HasPrefix(frame.Function, prefix)
		}) {
			return pc
		}
	}
	return 0
}

func loggerOrDefault(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}
