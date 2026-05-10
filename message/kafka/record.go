package kafka

import (
	"fmt"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

func messageToRecord(msg message.Message, cfg config) (*kgo.Record, error) {
	if cfg.topicResolver == nil {
		return nil, ErrNoTopic
	}
	topic, err := cfg.topicResolver(msg)
	if err != nil {
		return nil, err
	}
	if topic == "" {
		return nil, ErrNoTopic
	}

	payload := msg.Payload()
	if payload == nil {
		return nil, ErrNilPayload
	}

	value, err := cfg.codec.Marshal(payload)
	if err != nil {
		return nil, err
	}

	record := &kgo.Record{
		Topic:     topic,
		Key:       []byte(msg.Key()),
		Value:     value,
		Timestamp: msg.OccurredAt(),
	}

	reserved := reservedHeaders(cfg)
	for key, value := range msg.Headers() {
		if _, ok := reserved[key]; ok {
			continue
		}
		record.Headers = appendHeader(record.Headers, key, value)
	}

	record.Headers = appendHeader(record.Headers, cfg.headerNames.MessageID, msg.ID())
	record.Headers = appendHeader(record.Headers, cfg.headerNames.MessageKind, string(msg.Kind()))
	record.Headers = appendHeader(record.Headers, cfg.headerNames.OccurredAt, msg.OccurredAt().Format(time.RFC3339Nano))

	return record, nil
}

func recordToMessage(record *kgo.Record, cfg config) (message.Message, error) {
	if record == nil {
		return message.Message{}, ErrNoKind
	}
	if cfg.kindResolver == nil {
		return message.Message{}, ErrNoKind
	}

	kind, err := cfg.kindResolver(record)
	if err != nil {
		return message.Message{}, err
	}
	if kind == "" {
		return message.Message{}, ErrNoKind
	}
	if cfg.payloadResolver == nil {
		return message.Message{}, ErrNoPayloadResolver
	}

	payload, err := cfg.payloadResolver(kind)
	if err != nil {
		return message.Message{}, err
	}
	if payload == nil {
		return message.Message{}, ErrNilPayload
	}
	if err = cfg.codec.Unmarshal(record.Value, payload); err != nil {
		return message.Message{}, err
	}

	opts := []message.Option{
		message.WithKey(string(record.Key)),
	}
	if id := headerValue(record.Headers, cfg.headerNames.MessageID); id != "" {
		opts = append(opts, message.WithID(id))
	}

	occurredAtHeader := headerValue(record.Headers, cfg.headerNames.OccurredAt)
	if occurredAtHeader != "" {
		occurredAt, err := time.Parse(time.RFC3339Nano, occurredAtHeader)
		if err != nil {
			return message.Message{}, fmt.Errorf("parse message occurred at: %w", err)
		}
		opts = append(opts, message.WithOccurredAt(occurredAt))
	} else if !record.Timestamp.IsZero() {
		opts = append(opts, message.WithOccurredAt(record.Timestamp))
	}

	reserved := reservedHeaders(cfg)
	for _, header := range record.Headers {
		if _, ok := reserved[header.Key]; ok {
			continue
		}
		opts = append(opts, message.WithHeader(header.Key, string(header.Value)))
	}

	return message.New(kind, payload, opts...)
}

func cloneRecord(record *kgo.Record) *kgo.Record {
	if record == nil {
		return nil
	}

	clone := *record
	clone.Key = cloneBytes(record.Key)
	clone.Value = cloneBytes(record.Value)
	if record.Headers != nil {
		clone.Headers = make([]kgo.RecordHeader, len(record.Headers))
		for i, header := range record.Headers {
			clone.Headers[i] = kgo.RecordHeader{
				Key:   header.Key,
				Value: cloneBytes(header.Value),
			}
		}
	}
	return &clone
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	return append([]byte(nil), data...)
}
