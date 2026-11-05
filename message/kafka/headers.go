package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

func appendHeader(headers []kgo.RecordHeader, key, value string) []kgo.RecordHeader {
	return append(headers, kgo.RecordHeader{
		Key:   key,
		Value: []byte(value),
	})
}

func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func reservedHeaders(cfg config) map[string]struct{} {
	reserved := make(map[string]struct{}, 3)
	for _, header := range []string{
		cfg.headerNames.MessageID,
		cfg.headerNames.MessageKind,
		cfg.headerNames.OccurredAt,
	} {
		if header != "" {
			reserved[header] = struct{}{}
		}
	}
	return reserved
}
