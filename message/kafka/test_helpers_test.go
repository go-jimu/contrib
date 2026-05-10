package kafka

import (
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/message"
	testdata "github.com/go-jimu/components/encoding/testdata"
	// Keep the integration-test module pinned until broker tests are added.
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func testPayload(t *testing.T) *testdata.TestModel {
	t.Helper()

	return &testdata.TestModel{Id: 7, Name: "paid"}
}

func newTestMessage(t *testing.T, kind message.Kind) message.Message {
	t.Helper()

	msg, err := message.New(
		kind,
		testPayload(t),
		message.WithID("msg-1"),
		message.WithKey("order-1"),
		message.WithOccurredAt(time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)),
		message.WithHeader("trace_id", "trace-1"),
	)
	if err != nil {
		t.Fatalf("message.New returned error: %v", err)
	}

	return msg
}
