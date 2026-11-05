package asynq

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	hibiken "github.com/hibiken/asynq"
)

// Intent: asynq's own logs reach slog at the matching level, with the
// arguments joined as asynq's default logger does, and Fatal still exits.
func TestNewLoggerMapsLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	var _ hibiken.Logger = logger

	exited := 0
	logger.(*slogLogger).exit = func(code int) { exited = code }

	logger.Debug("debug ", 1)
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("redis ", "down")
	logger.Fatal("fatal")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	want := []string{
		`level=DEBUG msg="debug 1"`,
		`level=INFO msg=info`,
		`level=WARN msg=warn`,
		`level=ERROR msg="redis down"`,
		`level=ERROR msg=fatal`,
	}
	if len(lines) != len(want) {
		t.Fatalf("logged %d lines, want %d:\n%s", len(lines), len(want), buf.String())
	}
	for i, w := range want {
		if !strings.Contains(lines[i], w) {
			t.Errorf("line %d = %s, want %s", i, lines[i], w)
		}
	}
	if exited != 1 {
		t.Errorf("Fatal exit code = %d, want 1", exited)
	}
}

// Intent: a nil logger means slog.Default().
func TestNewLoggerNilUsesDefault(t *testing.T) {
	if got := NewLogger(nil).(*slogLogger).logger; got != slog.Default() {
		t.Fatalf("logger = %p, want slog.Default()", got)
	}
}

// sourceRecorder keeps the source function of each record by message.
type sourceRecorder struct {
	mu      sync.Mutex
	callers map[string]string
}

func (r *sourceRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *sourceRecorder) Handle(_ context.Context, rec slog.Record) error {
	caller := ""
	if src := rec.Source(); src != nil {
		caller = src.Function
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.callers[rec.Message] = caller
	return nil
}

func (r *sourceRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *sourceRecorder) WithGroup(string) slog.Handler      { return r }

// Intent: a record's source is the code that logged, not this adapter: a
// direct call names its caller.
func TestNewLoggerSourceDirect(t *testing.T) {
	rec := &sourceRecorder{callers: map[string]string{}}
	NewLogger(slog.New(rec)).Info("direct")
	if got, want := rec.callers["direct"], "github.com/go-jimu/contrib/taskqueue/asynq.TestNewLoggerSourceDirect"; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
}

// Intent: with a real asynq server, every log's source is asynq code past its
// internal logger. Start logs before it reaches Redis, so none is needed.
func TestNewLoggerSourceWithServer(t *testing.T) {
	rec := &sourceRecorder{callers: map[string]string{}}
	srv := hibiken.NewServer(hibiken.RedisClientOpt{Addr: "127.0.0.1:1"}, hibiken.Config{
		Logger:          NewLogger(slog.New(rec)),
		LogLevel:        hibiken.DebugLevel,
		ShutdownTimeout: time.Second,
	})
	if err := srv.Start(hibiken.HandlerFunc(func(context.Context, *hibiken.Task) error { return nil })); err != nil {
		t.Fatal(err)
	}
	srv.Shutdown()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if _, ok := rec.callers["Starting processing"]; !ok {
		t.Fatalf("asynq did not log Starting processing; logged %v", rec.callers)
	}
	for msg, caller := range rec.callers {
		if !strings.HasPrefix(caller, "github.com/hibiken/asynq.") {
			t.Errorf("%q source = %q, want an asynq call site", msg, caller)
		}
	}
}
