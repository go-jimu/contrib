// Package kafka implements github.com/go-jimu/components/ddd/message on Kafka
// with franz-go: NewPublisher returns a message.Publisher, and Consumer is a
// message.Subscriber and message.Runner that commits offsets and applies a
// FailurePolicy to each message-level failure.
//
// # Runtime composition
//
// Import this package and github.com/twmb/franz-go/pkg/kgo in runtime
// composition only, such as an internal/pkg/messagebus package. Runtime
// composition builds one client, the publisher, the consumer, and the payload
// registry, and starts and stops Run. Application, domain, and transport code
// depend on ddd/message alone: message.Publisher, message.Subscriber,
// message.Handler, and *message.PayloadRegistry.
//
//	client, err := kafka.NewClient(
//		kgo.SeedBrokers(brokers...),
//		kgo.ConsumerGroup("orders-service"),
//		kgo.ConsumeTopics(topics...),
//		kgo.DisableAutoCommit(), // the consumer commits
//	)
//	registry := message.NewPayloadRegistry()
//	consumer, err := kafka.NewConsumer(client, kafka.DLQPolicy(),
//		kafka.WithPayloadResolver(registry), // decodes payloads
//		kafka.WithDefaultFailureTopics(),    // names retry and DLQ topics
//		kafka.WithErrorObserver(observe),
//	)
//	publisher := kafka.NewPublisher(client)
//
// The publisher may share the consumer's client; it holds nothing of its own
// and has no Close. Hand publisher, registry, and consumer (as a
// message.Subscriber) to the bounded contexts. Before Run starts, each context
// registers a payload factory for every Kind it consumes and subscribes its
// handlers. NewConsumer requires WithPayloadResolver and returns
// ErrNoPayloadResolver without it. A record decodes only when its Kind is
// registered; an unregistered Kind fails at StageDecode with
// message.ErrUnknownKind.
//
// kgo.ConsumeTopics fixes the topics when the client is built, so list the
// topic of every Kind a handler subscribes to (the Kind itself, by default),
// plus the retry topics when the policy retries. A Kind whose topic is
// missing is never delivered.
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
// The consumer rebuilds the Message from the record: Kind from the
// jimu-message-kind header (WithKindResolver for records from other
// producers), Key from Record.Key, ID from its header or a new random ID,
// OccurredAt from its header or Record.Timestamp, and Headers from the
// remaining headers, without the reserved and jimu-* failure headers.
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
//     when failures need inspection or replay and an immediate retry would
//     not help. It needs WithDefaultFailureTopics or WithDLQTopicResolver.
//   - RetryThenDLQPolicy publishes a handler failure to its retry topic, then
//     commits; a later failure of that record repeats this until MaxAttempts
//     handler attempts (3 by default, counting the first), then publishes to
//     DLQ. With the default Retryable, decode and unhandled failures go to DLQ
//     at once. Choose it for transient failures. It needs both topic
//     resolvers and a client that also consumes the retry topics, as in
//     kgo.ConsumeTopics("orders.paid", "orders.paid.retry"). A retry record is
//     handled as soon as it is polled, with no delay, and on another topic, so
//     records of one key are no longer handled in order once one is retried.
//   - A FailurePolicyFunc decides from Error.Stage and Error.Err.
//
// A handler marks a permanent failure, one a retry cannot fix, by wrapping a
// sentinel error of its own; RetryThenDLQConfig.Retryable then excludes it:
//
//	Retryable: func(f kafka.Error) bool {
//		return f.Stage == kafka.StageHandle && !errors.Is(f.Err, ErrPermanent)
//	},
//
// The default Retryable retries a recovered panic like a handler error; add
// !errors.Is(f.Err, kafka.ErrHandlerPanic) to send panics straight to DLQ.
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
// the default FailurePublishFallbackStop stops Run and leaves the offset
// uncommitted, so Kafka redelivers the record after a restart or rebalance.
// WithFailurePublishFallback(FailurePublishFallbackDrop) commits instead, and
// the message is lost.
//
// # Consumer.Run lifecycle
//
// Run polls and handles one record at a time on the calling goroutine, in
// partition order, passing its own ctx to the handlers. The handlers of one
// record run in subscription order and stop at the first error or panic.
// Run recovers a handler panic as a StageHandle failure whose Err is a
// *PanicError, matching ErrHandlerPanic, and applies the FailurePolicy to it
// like a handler error; the ErrorObserver can log PanicError.Stack. Delivery
// is at least once, so make handlers idempotent, for example keyed by
// Message.ID.
//
// When ctx ends, Run returns nil. A handler that fails because ctx ended goes
// through the FailurePolicy like any other failure, and the publish and commit
// that follow run on the ended ctx: the record is either committed after that
// outcome or redelivered later. Under DropAndCommitPolicy it can be committed
// unhandled, so finish or return nil from handlers that are cut short. Run
// lists the errors that stop it early; treat them as fatal and stop or
// restart the process.
//
// Shut down in this order: cancel Run's ctx, wait for Run to return, then close
// the client after every publisher sharing it has stopped. Consumer.Close
// closes the client when WithCloseClient(true) was set; otherwise runtime
// composition closes it with client.Close.
//
// # Errors
//
// The FailurePolicy and the ErrorObserver receive an Error value. Compare
// Error.Err, and the errors returned by Run and Publish, with errors.Is
// against the Err* sentinels of this package and of ddd/message.
//
// The upstream ddd/message API is experimental. When this adapter shows that
// the core contract does not fit real Kafka usage, fix the upstream contract
// and keep this adapter a direct mapping.
package kafka
