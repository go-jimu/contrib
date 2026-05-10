package kafka

import (
	"testing"

	testdata "github.com/go-jimu/components/encoding/testdata"
	"google.golang.org/protobuf/proto"
)

// Intent: protobuf payloads should survive the default codec round trip without losing contract fields.
func TestProtoCodecRoundTrip(t *testing.T) {
	codec := ProtoCodec{}
	original := &testdata.TestModel{
		Id:    42,
		Name:  "paid",
		Hobby: []string{"kafka", "protobuf"},
		Attrs: map[string]string{"trace_id": "trace-1"},
	}

	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Marshal returned empty bytes")
	}

	decoded := &testdata.TestModel{}
	if err := codec.Unmarshal(data, decoded); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if !proto.Equal(original, decoded) {
		t.Fatalf("decoded payload = %#v, want %#v", decoded, original)
	}
}

// Intent: invalid protobuf bytes must fail decoding so consumers do not treat corrupt Kafka records as valid messages.
func TestProtoCodecUnmarshalRejectsInvalidBytes(t *testing.T) {
	codec := ProtoCodec{}
	decoded := &testdata.TestModel{}

	err := codec.Unmarshal([]byte{0xff, 0xff, 0xff}, decoded)

	if err == nil {
		t.Fatal("Unmarshal returned nil error for invalid protobuf bytes")
	}
	if !proto.Equal(decoded, &testdata.TestModel{}) {
		t.Fatalf("decoded payload = %#v, want zero value after failed decode", decoded)
	}
}
