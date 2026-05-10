package kafka

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	headerRetryAttempt      = "jimu-retry-attempt"
	headerOriginalTopic     = "jimu-original-topic"
	headerOriginalPartition = "jimu-original-partition"
	headerOriginalOffset    = "jimu-original-offset"
	headerFailedStage       = "jimu-failed-stage"
	headerFirstError        = "jimu-first-error"
	headerLastError         = "jimu-last-error"
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

func retryAttempt(headers []kgo.RecordHeader) int {
	attempt, err := strconv.Atoi(headerValue(headers, headerRetryAttempt))
	if err != nil || attempt < 0 {
		return 0
	}
	return attempt
}

func reservedHeaders(cfg config) map[string]struct{} {
	reserved := make(map[string]struct{}, 10)
	for _, header := range []string{
		cfg.headerNames.MessageID,
		cfg.headerNames.MessageKind,
		cfg.headerNames.OccurredAt,
		headerRetryAttempt,
		headerOriginalTopic,
		headerOriginalPartition,
		headerOriginalOffset,
		headerFailedStage,
		headerFirstError,
		headerLastError,
	} {
		if header != "" {
			reserved[header] = struct{}{}
		}
	}
	return reserved
}
