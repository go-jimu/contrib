package kafka

import (
	"google.golang.org/protobuf/proto"
)

type Codec interface {
	Marshal(proto.Message) ([]byte, error)
	Unmarshal([]byte, proto.Message) error
}

type ProtoCodec struct{}

func (ProtoCodec) Marshal(msg proto.Message) ([]byte, error) {
	return proto.Marshal(msg)
}

func (ProtoCodec) Unmarshal(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}
