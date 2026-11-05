package asynq

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

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
