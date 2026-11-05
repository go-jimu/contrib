package kafka

import (
	"google.golang.org/protobuf/proto"
)

// Codec encodes payloads into record values and decodes them back. It must be
// safe for concurrent use.
type Codec interface {
	Marshal(proto.Message) ([]byte, error)
	Unmarshal([]byte, proto.Message) error
}

// ProtoCodec is the default Codec: protobuf binary encoding.
type ProtoCodec struct{}

// Marshal encodes msg with proto.Marshal.
func (ProtoCodec) Marshal(msg proto.Message) ([]byte, error) {
	return proto.Marshal(msg)
}

// Unmarshal decodes data into msg with proto.Unmarshal.
func (ProtoCodec) Unmarshal(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}
