package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// TopicResolver returns the topic a message is published to. The default
// returns the Kind string. An error or "" fails Publish.
type TopicResolver func(message.Message) (string, error)

// KindResolver returns the message Kind of a consumed record. The default
// reads the MessageKind header. An error or "" fails the record at
// StageDecode.
type KindResolver func(*kgo.Record) (message.Kind, error)

// PayloadResolver is the upstream transport-neutral message payload resolver.
// A *message.PayloadRegistry is the usual value.
type PayloadResolver = message.PayloadResolver

// Deprecated: use ErrorObserver. ErrorHandler return values no longer control
// message-level failure flow.
type ErrorHandler func(context.Context, Error) error

// ErrorObserver sees every consumer failure, for logging, alerting, and
// metrics; the policies alone decide what happens to the record. It sees
// failures the policy then retries too: Error.Final marks the last one. See
// the concurrency contract in Handlers, and Shutdown, in the package
// documentation.
type ErrorObserver func(context.Context, Error)

// Recoverer takes each record the Consumer's FailurePolicy drops, before its
// offset is committed; the Error carries the source Record. Return nil to
// settle the record; an error keeps it and calls the Recoverer again after the
// backoff. See Recoverer in the package documentation.
type Recoverer func(context.Context, Error) error

// RetryBackoff returns the delay before attempt+1, after attempt failed
// attempts at a record (attempt starts at 1).
type RetryBackoff func(attempt int) time.Duration

// PublishBackoff returns the delay before the next publish retry attempt.
type PublishBackoff func(attempt int) time.Duration

// Option configures NewPublisher, NewConsumer, and NewShareConsumer. Each
// option names what it affects; the others ignore it.
type Option func(*config)

// HeaderNames are the reserved record headers that carry message metadata.
// Configure the publisher and the consumers with the same names.
type HeaderNames struct {
	MessageID   string
	MessageKind string
	OccurredAt  string
}

type config struct {
	codec               Codec
	topicResolver       TopicResolver
	kindResolver        KindResolver
	defaultKindResolver bool
	payloadResolver     PayloadResolver
	errorObserver       ErrorObserver
	logger              *slog.Logger
	recoverer           Recoverer
	retryBackoff        RetryBackoff
	handlerTimeout      time.Duration
	recordContext       func(context.Context, *kgo.Record) context.Context
	commitTimeout       time.Duration
	maxPollRecords      int
	concurrency         int
	maxPollRecordsSet   bool
	deliveryLimit       int
	afterFunc           func(time.Duration, func())
	publishRetry        publishRetryConfig
	closeClient         bool
	headerNames         HeaderNames
}

type publishRetryConfig struct {
	maxAttempts int
	backoff     PublishBackoff
	sleep       func(context.Context, time.Duration) error
}

func defaultConfig() config {
	return config{
		codec:               ProtoCodec{},
		topicResolver:       defaultTopicResolver,
		kindResolver:        defaultKindResolver(defaultHeaderNames()),
		defaultKindResolver: true,
		retryBackoff:        DefaultRetryBackoff,
		commitTimeout:       10 * time.Second,
		maxPollRecords:      DefaultMaxPollRecords,
		deliveryLimit:       DefaultDeliveryLimit,
		afterFunc:           func(d time.Duration, f func()) { time.AfterFunc(d, f) },
		publishRetry:        publishRetryConfig{maxAttempts: 1, sleep: sleepContext},
		headerNames:         defaultHeaderNames(),
	}
}

func defaultHeaderNames() HeaderNames {
	return HeaderNames{
		MessageID:   "jimu-message-id",
		MessageKind: "jimu-message-kind",
		OccurredAt:  "jimu-message-occurred-at",
	}
}

func defaultTopicResolver(msg message.Message) (string, error) {
	if msg.Kind() == "" {
		return "", ErrNoTopic
	}
	return string(msg.Kind()), nil
}

func defaultKindResolver(headers HeaderNames) KindResolver {
	return func(record *kgo.Record) (message.Kind, error) {
		if record == nil {
			return "", ErrNoKind
		}
		for _, header := range record.Headers {
			if header.Key == headers.MessageKind {
				if len(header.Value) == 0 {
					return "", ErrNoKind
				}
				return message.Kind(header.Value), nil
			}
		}
		return "", ErrNoKind
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// WithCodec replaces the protobuf codec for payloads, for the publisher and
// the consumers. A nil codec is ignored.
func WithCodec(codec Codec) Option {
	return func(cfg *config) {
		if codec != nil {
			cfg.codec = codec
		}
	}
}

// WithTopicResolver sets the publisher's message-to-topic mapping. A nil
// resolver is ignored.
func WithTopicResolver(resolver TopicResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.topicResolver = resolver
		}
	}
}

// WithKindResolver sets how the consumers derive a record's Kind, for records
// written by producers other than this package. A nil resolver is ignored.
func WithKindResolver(resolver KindResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.kindResolver = resolver
			cfg.defaultKindResolver = false
		}
	}
}

// WithPayloadResolver sets the consumers' payload resolver, usually a
// *message.PayloadRegistry. Both consumer constructors require it. A nil
// resolver is ignored.
func WithPayloadResolver(resolver PayloadResolver) Option {
	return func(cfg *config) {
		if resolver != nil {
			cfg.payloadResolver = resolver
		}
	}
}

// WithPayloadResolverFunc adapts a function into message.PayloadResolver.
func WithPayloadResolverFunc(resolver func(message.Kind) (proto.Message, error)) Option {
	if resolver == nil {
		return WithPayloadResolver(nil)
	}
	return WithPayloadResolver(message.PayloadResolverFunc(resolver))
}

// WithErrorObserver sets the consumers' ErrorObserver. A nil observer is
// ignored.
func WithErrorObserver(observer ErrorObserver) Option {
	return func(cfg *config) {
		if observer != nil {
			cfg.errorObserver = observer
		}
	}
}

// Deprecated: use WithErrorObserver. Returned errors are ignored because
// message-level failure flow is controlled by FailurePolicy.
func WithErrorHandler(handler ErrorHandler) Option {
	return func(cfg *config) {
		if handler != nil {
			cfg.errorObserver = func(ctx context.Context, failure Error) {
				_ = handler(ctx, failure)
			}
		}
	}
}

// WithPublishRetry makes Publish try a failed produce up to maxAttempts times
// in total, waiting backoff(attempt) between attempts or until ctx ends. A
// maxAttempts of 1 or less keeps one attempt; a nil backoff retries at once.
// It affects the publisher only.
func WithPublishRetry(maxAttempts int, backoff PublishBackoff) Option {
	return func(cfg *config) {
		if maxAttempts > 1 {
			cfg.publishRetry.maxAttempts = maxAttempts
		}
		cfg.publishRetry.backoff = backoff
	}
}

// WithCloseClient makes the consumers' Close close the franz-go client.
func WithCloseClient(closeClient bool) Option {
	return func(cfg *config) {
		cfg.closeClient = closeClient
	}
}

// WithHeaderNames replaces the reserved metadata header names for the
// publisher and the default kind resolver. A zero HeaderNames is ignored.
func WithHeaderNames(headers HeaderNames) Option {
	return func(cfg *config) {
		if headers == (HeaderNames{}) {
			return
		}
		cfg.headerNames = headers
		if cfg.defaultKindResolver {
			cfg.kindResolver = defaultKindResolver(headers)
		}
	}
}

// DefaultRetryBackoff waits 1s after the first failed attempt and doubles the
// delay for each further one, up to 1 minute.
func DefaultRetryBackoff(attempt int) time.Duration {
	const maxDelay = time.Minute
	if attempt <= 1 {
		return time.Second
	}
	if attempt > 7 {
		return maxDelay
	}
	return min(time.Second<<(attempt-1), maxDelay)
}

// WithRetryBackoff sets the Consumer's delay before handling a record again
// after FailureActionRetry, and before calling a failed Recoverer again. The
// default is DefaultRetryBackoff. A nil backoff is ignored.
func WithRetryBackoff(backoff RetryBackoff) Option {
	return func(cfg *config) {
		if backoff != nil {
			cfg.retryBackoff = backoff
		}
	}
}

// WithHandlerTimeout bounds how long the handlers of one record run, for both
// consumers. At the timeout the handlers' ctx is cancelled and the record
// fails at StageHandle with an error matching ErrHandlerTimeout. 0 or less,
// the default, sets no timeout. How to size it against kgo.RebalanceTimeout
// and the share group's record lock is in Consumer offsets and rebalances,
// and Share groups, in the package documentation.
func WithHandlerTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		cfg.handlerTimeout = timeout
	}
}

// WithRecordContext sets fn to derive the ctx of each record's work, for both
// consumers: the handlers' ctx, and the ctx of the FailurePolicy, the
// ShareFailurePolicy, the Recoverer, the ErrorObserver, and the logs about the
// record. fn receives Run's ctx and the record. Derive the result from ctx and
// only add values, such as the trace context a kgo hook put on
// record.Context, so Shutdown, a forced stop, and WithHandlerTimeout still
// apply. A nil fn, or a nil result, keeps ctx. See Handlers in the package
// documentation.
func WithRecordContext(fn func(ctx context.Context, record *kgo.Record) context.Context) Option {
	return func(cfg *config) {
		cfg.recordContext = fn
	}
}

// WithRecoverer sets the Consumer's Recoverer. A nil recoverer is ignored.
func WithRecoverer(recoverer Recoverer) Option {
	return func(cfg *config) {
		if recoverer != nil {
			cfg.recoverer = recoverer
		}
	}
}

// DefaultMaxPollRecords is the default WithMaxPollRecords limit.
const DefaultMaxPollRecords = 500

// WithMaxPollRecords limits the records of one batch, for both consumers. 0 or
// less means no limit. The default is DefaultMaxPollRecords for the Consumer
// and WithConcurrency for the ShareConsumer. See Consumer offsets and
// rebalances, and Share groups, in the package documentation.
func WithMaxPollRecords(n int) Option {
	return func(cfg *config) {
		cfg.maxPollRecords = max(n, 0)
		cfg.maxPollRecordsSet = true
	}
}

// WithConcurrency sets how many records the ShareConsumer handles at once. 0 or
// less, the default, means runtime.GOMAXPROCS(0). The Consumer runs one
// goroutine per partition instead; see Choosing a consumer in the package
// documentation.
func WithConcurrency(n int) Option {
	return func(cfg *config) {
		cfg.concurrency = n
	}
}

// DefaultDeliveryLimit is Kafka's default share.delivery.count.limit.
const DefaultDeliveryLimit = 5

// WithDeliveryLimit tells the ShareConsumer the share group's
// share.delivery.count.limit, so that a release on the last allowed delivery
// is reported with Error.Final set: the broker archives the record next. The
// default is DefaultDeliveryLimit; 0 or less never marks a release final.
func WithDeliveryLimit(n int) Option {
	return func(cfg *config) {
		cfg.deliveryLimit = max(n, 0)
	}
}

// WithLogger sets the logger of both consumers. The default is slog.Default()
// at construction; a nil logger keeps it. The levels and attributes the
// consumers log are in Logging in the package documentation.
func WithLogger(logger *slog.Logger) Option {
	return func(cfg *config) {
		if logger != nil {
			cfg.logger = logger
		}
	}
}
