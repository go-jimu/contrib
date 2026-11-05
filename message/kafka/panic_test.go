package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-jimu/components/ddd/message"
)

type panicHandler struct {
	kinds []message.Kind
	value any
}

func (h panicHandler) Listening() []message.Kind { return h.kinds }

func (h panicHandler) Handle(context.Context, message.Message) error { panic(h.value) }

// Intent: a handler panic is a message-level failure, not a process crash:
// the policy decides it, the observer gets the panic value and stack, and the
// consumer moves on.
func TestConsumerProcessRecordRecoversHandlerPanic(t *testing.T) {
	cfg := testConsumerConfig()
	var observed []Error
	WithErrorObserver(func(_ context.Context, failure Error) {
		observed = append(observed, failure)
	})(&cfg)
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, DropAndCommitPolicy(), cfg)
	kind := message.Kind("order.payment.v1.OrderPaid")
	if err := consumer.Subscribe(panicHandler{kinds: []message.Kind{kind}, value: "boom"}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}
	record := testRecord(t, cfg, kind)

	err := consumer.processRecord(context.Background(), record)

	if err != nil {
		t.Fatalf("processRecord returned error: %v", err)
	}
	if len(client.committed) != 1 || client.committed[0] != record {
		t.Fatalf("committed records = %#v, want source record", client.committed)
	}
	if len(observed) != 1 {
		t.Fatalf("observed failures = %d, want 1", len(observed))
	}
	failure := observed[0]
	if failure.Stage != StageHandle {
		t.Fatalf("stage = %q, want %q", failure.Stage, StageHandle)
	}
	if failure.Message.Kind() != kind {
		t.Fatalf("message kind = %q, want %q", failure.Message.Kind(), kind)
	}
	if !errors.Is(failure.Err, ErrHandlerPanic) {
		t.Fatalf("err = %v, want ErrHandlerPanic", failure.Err)
	}
	var panicErr *PanicError
	if !errors.As(failure.Err, &panicErr) {
		t.Fatalf("err = %T, want *PanicError", failure.Err)
	}
	if panicErr.Value != "boom" {
		t.Fatalf("panic value = %v, want boom", panicErr.Value)
	}
	if len(panicErr.Stack) == 0 {
		t.Fatal("panic stack is empty")
	}
}

// Intent: a handler that panics with an error keeps that error reachable
// through errors.Is next to ErrHandlerPanic.
func TestPanicErrorUnwrapsErrorValue(t *testing.T) {
	cause := errors.New("nil map write")
	err := error(&PanicError{Value: cause, Stack: []byte("stack")})

	if !errors.Is(err, ErrHandlerPanic) || !errors.Is(err, cause) {
		t.Fatalf("err = %v, want ErrHandlerPanic and the panic error", err)
	}
	if strings.Contains(err.Error(), "stack") {
		t.Fatalf("Error() = %q, want no stack", err.Error())
	}
}

// Intent: a recovered panic follows the failure policy like a handler error,
// and the failure record headers carry the panic message without the stack.
func TestConsumerProcessRecordRetriesHandlerPanic(t *testing.T) {
	cfg := testConsumerConfig()
	WithDefaultFailureTopics()(&cfg)
	client := &fakeConsumerClient{}
	consumer := newConsumerForTest(t, client, RetryThenDLQPolicy(RetryThenDLQConfig{}), cfg)
	kind := message.Kind("order.payment.v1.OrderPaid")
	if err := consumer.Subscribe(panicHandler{kinds: []message.Kind{kind}, value: "boom"}); err != nil {
		t.Fatalf("Subscribe returned error: %v", err)
	}
	record := testRecord(t, cfg, kind)

	if err := consumer.processRecord(context.Background(), record); err != nil {
		t.Fatalf("processRecord returned error: %v", err)
	}

	if len(client.records) != 1 || client.records[0].Topic != "orders.retry" {
		t.Fatalf("produced records = %#v, want one on orders.retry", client.records)
	}
	lastError := headerValue(client.records[0].Headers, headerLastError)
	if lastError != "message handler panicked: boom" {
		t.Fatalf("last error header = %q, want the panic message without stack", lastError)
	}
}
