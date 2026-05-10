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

func addFailureHeaders(record *kgo.Record, _ config, failure Error, attempt int) {
	if record == nil || failure.Record == nil {
		return
	}

	firstError := headerValue(record.Headers, headerFirstError)
	record.Headers = filterHeaders(record.Headers, headerRetryAttempt, headerOriginalTopic, headerOriginalPartition, headerOriginalOffset, headerFailedStage, headerLastError)

	record.Headers = appendHeader(record.Headers, headerRetryAttempt, strconv.Itoa(attempt))
	record.Headers = appendHeader(record.Headers, headerOriginalTopic, failure.Record.Topic)
	record.Headers = appendHeader(record.Headers, headerOriginalPartition, strconv.Itoa(int(failure.Record.Partition)))
	record.Headers = appendHeader(record.Headers, headerOriginalOffset, strconv.FormatInt(failure.Record.Offset, 10))
	record.Headers = appendHeader(record.Headers, headerFailedStage, string(failure.Stage))
	if firstError == "" {
		record.Headers = appendHeader(record.Headers, headerFirstError, errorText(failure.Err))
	}
	record.Headers = appendHeader(record.Headers, headerLastError, errorText(failure.Err))
}

func filterHeaders(headers []kgo.RecordHeader, keys ...string) []kgo.RecordHeader {
	if len(headers) == 0 || len(keys) == 0 {
		return headers
	}

	drop := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		drop[key] = struct{}{}
	}

	filtered := headers[:0]
	for _, header := range headers {
		if _, ok := drop[header.Key]; ok {
			continue
		}
		filtered = append(filtered, header)
	}
	return filtered
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
