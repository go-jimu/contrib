// Package kafka implements github.com/go-jimu/components/ddd/message on Kafka
// with franz-go: NewPublisher returns a message.Publisher, and Consumer is a
// message.Subscriber and message.Runner that commits offsets and applies a
// FailurePolicy to failed messages.
//
// # Where it belongs
//
// Import this package and github.com/twmb/franz-go/pkg/kgo only in runtime
// composition code, such as an internal/pkg/messagebus package. That code
// builds the client, the publisher, the consumer, and the payload registry,
// and starts and stops Run. Application, domain, and transport code depend
// only on ddd/message: message.Publisher, message.Subscriber,
// message.Handler, and *message.PayloadRegistry.
//
// # Mapping
//
// Publish writes Message.Payload to Record.Value with the Codec (protobuf by
// default) and Message.Key to Record.Key, so Kafka hash partitioning keeps one
// key on one partition, in order. OccurredAt becomes Record.Timestamp. ID,
// Kind, and OccurredAt also travel in the reserved headers jimu-message-id,
// jimu-message-kind, and jimu-message-occurred-at (see WithHeaderNames), and
// Message.Headers become record headers, except reserved names. The topic is
// the Kind string unless WithTopicResolver says otherwise; with
// message.KindOf, that is the protobuf full name, such as
// "user.integration.v1.UserRegisteredV1". This package creates no topics.
//
// The consumer reads the Kind from the jimu-message-kind header; use
// WithKindResolver for records written by other producers.
//
// # Composition
//
//	client, err := kafka.NewClient(
//		kgo.SeedBrokers(brokers...),
//		kgo.ConsumerGroup("orders-service"),
//		kgo.ConsumeTopics(topics...),
//		kgo.DisableAutoCommit(), // required: the consumer commits
//	)
//	registry := message.NewPayloadRegistry()
//	consumer, err := kafka.NewConsumer(client, kafka.DLQPolicy(),
//		kafka.WithPayloadResolver(registry), // required to decode
//		kafka.WithDefaultFailureTopics(),    // required by DLQ and retry
//		kafka.WithErrorObserver(observe),
//	)
//	publisher := kafka.NewPublisher(client)
//
// Hand publisher, registry, and consumer (as a message.Subscriber) to the
// bounded contexts. Before Run starts, every context registers a payload
// factory for each Kind it consumes and subscribes its handlers. A consumer
// without WithPayloadResolver fails every record at StageDecode with
// ErrNoPayloadResolver, and a Kind without a registered factory fails with
// message.ErrUnknownKind; both go to the FailurePolicy.
//
// # Choosing a FailurePolicy
//
// A handler error (StageHandle), a decode error (StageDecode), and a Kind with
// no handler (StageUnhandled) are message-level failures. NewConsumer requires
// a FailurePolicy that decides each one:
//
//   - DropAndCommitPolicy commits the offset and moves on; the message is
//     lost. Choose it when losing a failed message is acceptable, as for
//     best-effort notifications, or when the handler records its own
//     failures. It needs no failure topics.
//   - DLQPolicy publishes the record to its DLQ topic, then commits. Choose it
//     when failures need inspection or replay but an immediate retry would
//     not help. It needs WithDefaultFailureTopics or WithDLQTopicResolver.
//   - RetryThenDLQPolicy publishes a handler failure to its retry topic, then
//     commits; a later failure of that record repeats this until MaxAttempts
//     handler attempts (3 by default, counting the first), then publishes to
//     DLQ. Decode and unhandled failures go to DLQ at once. Choose it for
//     transient failures. It needs both topic resolvers, and the consumer
//     must also consume the retry topics, as in
//     kgo.ConsumeTopics("orders.paid", "orders.paid.retry"); retry records
//     are handled as soon as they are polled, with no delay.
//   - A FailurePolicyFunc decides from Error.Stage and Error.Err.
//
// WithDefaultFailureTopics names the topics <source-topic>.retry and
// <source-topic>.dlq, using the original topic for records consumed from a
// retry topic. Provision them in Kafka. Retry and DLQ records copy the source
// key, value, and headers, and add jimu-retry-attempt, jimu-original-topic,
// jimu-original-partition, jimu-original-offset, jimu-failed-stage,
// jimu-first-error, and jimu-last-error.
//
// The offset of a record is committed after exactly one of: the handlers
// return nil; the policy drops the failure; the retry or DLQ record is
// published. When the failure topic cannot be resolved or the publish fails,
// the default FailurePublishFallbackStop makes Run return an error matching
// ErrRetryPublishFailed or ErrDLQPublishFailed and leaves the offset
// uncommitted, so Kafka redelivers the record after a restart or rebalance.
// WithFailurePublishFallback(FailurePublishFallbackDrop) commits instead, and
// the message is lost.
//
// # Consumer.Run lifecycle
//
// Run polls and handles one record at a time on the calling goroutine, in
// partition order, passing its own ctx to the handlers. The handlers of one
// record run in subscription order and stop at the first error. Handler
// panics are not recovered: recover inside the handler to keep the process
// alive. Delivery is at least once, so make handlers idempotent.
//
// Run blocks until ctx ends and then returns nil. A record being handled when
// ctx ends is not committed and is delivered again later. Run returns a
// non-nil error when the consumer cannot continue: ErrNilClient, a poll error
// that returned no records, a commit failure matching ErrCommitFailed, or a
// failure publish error under FailurePublishFallbackStop. Treat that error as
// fatal for the consumer: log it and stop or restart the process.
//
// Shut down in this order: cancel Run's ctx, wait for Run to return, then close
// the client after every publisher sharing it has stopped. Consumer.Close
// closes the client only when WithCloseClient(true) was set; otherwise the
// caller closes it with client.Close.
//
// # Errors
//
// An Error value describes one failure: its Stage, the source Record, the
// decoded Message when there is one, and the cause in Err. The FailurePolicy
// and the ErrorObserver receive it; the observer is for logging and metrics
// and does not change the outcome. Error does not implement the error
// interface; compare Error.Err and the errors returned by Run and Publish
// with errors.Is against the Err* sentinels of this package and of
// ddd/message.
//
// The upstream ddd/message API is experimental. If this adapter reveals that
// the core contract does not fit real Kafka usage, fix the upstream contract
// instead of working around it here.
package kafka
