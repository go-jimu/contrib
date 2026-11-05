// Package kafka implements github.com/go-jimu/components/ddd/message on Kafka
// with franz-go: NewPublisher returns a message.Publisher, and Consumer is a
// message.Subscriber and message.Runner that handles partitions concurrently,
// commits offsets per batch, and applies a FailurePolicy to each message-level
// failure. ShareConsumer consumes a Kafka share group instead, for messages
// that need no order, with retries and dead letters left to the broker.
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
//		kgo.DisableAutoCommit(),    // the consumer commits
//		kgo.BlockRebalanceOnPoll(), // no rebalance while a batch is handled
//	)
//	registry := message.NewPayloadRegistry()
//	consumer, err := kafka.NewConsumer(client, kafka.RetryPolicy(kafka.RetryConfig{}),
//		kafka.WithPayloadResolver(registry), // decodes payloads
//		kafka.WithHandlerTimeout(30*time.Second),
//		kafka.WithErrorObserver(observe),
//	)
//	publisher := kafka.NewPublisher(client)
//
// NewConsumer returns an error matching ErrInvalidClient unless the client
// has kgo.ConsumerGroup, kgo.DisableAutoCommit, and kgo.BlockRebalanceOnPoll.
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
// topic of every Kind a handler subscribes to (the Kind itself, by default).
// A Kind whose topic is missing is never delivered.
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
// remaining headers, without the reserved ones.
//
// # Choosing a FailurePolicy
//
// A handler error (StageHandle), a decode error (StageDecode), and a Kind with
// no handler (StageUnhandled) are message-level failures. NewConsumer requires
// a FailurePolicy that decides each one; Error.Attempt counts the failed
// attempts at the record so far:
//
//   - RetryPolicy retries a handler failure in place: the record stays
//     uncommitted, its partition is rewound to it and paused for the
//     WithRetryBackoff delay (1s, doubling, up to 1 minute by default), and
//     the record is handled again before any later record of its partition.
//     The partition keeps its order; other partitions keep going. After
//     MaxAttempts attempts (3 by default, counting the first) it applies
//     OnExhausted, FailureActionDrop or FailureActionStop. Choose it for
//     transient failures, such as a database that is briefly unavailable.
//   - DropAndCommitPolicy drops every failure at once.
//   - A FailurePolicyFunc decides from Error.Stage, Error.Err, and
//     Error.Attempt.
//
// RetryPolicy does not retry a failure a retry cannot fix: by default
// (DefaultRetryable) decode and unhandled failures, and any error matching
// ErrNonRetryable. A handler marks a permanent failure, such as invalid input,
// by returning NonRetryable(err). A handler timeout (ErrHandlerTimeout) and a
// recovered panic (ErrHandlerPanic) are retried like any handler error.
//
// A record retried in place holds up its partition until it succeeds or is
// given up, so a record that always fails delays the records behind it by the
// sum of its backoffs.
//
// # Dead letters
//
// This package publishes no retry or dead-letter records. A dead-letter queue
// is a broker feature of Kafka share groups (KIP-1191), which give up the
// per-partition order that this Consumer keeps.
//
// When the FailurePolicy drops a record, the Recoverer set with WithRecoverer
// gets it first, as an Error with Final set and the source Record, for example
// to save it in a failed-messages table. The offset is committed only after
// the Recoverer returns nil. When it returns an error, the record stays
// uncommitted, the failure is observed at StageRecover matching
// ErrRecoverFailed, and after the backoff the Recoverer is called again,
// without running the handlers again. The partition waits meanwhile, so alert
// on StageRecover. Without a Recoverer a dropped message is lost; the
// ErrorObserver still sees its Error with Final set.
//
// # Share groups
//
// ShareConsumer consumes with a kgo.ShareGroup client (KIP-932, generally
// available from Kafka 4.2). The broker hands each record to one member and
// tracks its delivery count; members are not tied to partitions, so
// WithConcurrency, not the partition count, sets how many records are
// handled at once, and nothing is kept in order. Each record is acknowledged
// before the next poll: accepted when its handlers succeed, otherwise as the
// ShareFailurePolicy decides. ReleaseOrRejectPolicy releases a retryable
// failure, which the broker delivers again until the group's
// share.delivery.count.limit (5 by default), and rejects the others. The
// broker archives a rejected or exhausted record, or writes it to the group's
// dead-letter topic when it supports KIP-1191 (share.version 2) and the group
// is configured for one. Error.Attempt is the delivery count.
//
// A new share group starts at the latest offset; set the group config
// share.auto.offset.reset to earliest to consume existing records. Keep
// handlers within the group's acquisition lock (share.record.lock.duration.ms,
// 30s by default) with WithHandlerTimeout, or the broker delivers the record
// again meanwhile.
//
// # Offsets and rebalances
//
// Each poll takes at most WithMaxPollRecords records (DefaultMaxPollRecords by
// default) and splits them by partition. Partitions are handled concurrently,
// each in offset order. A partition stops at its first record that is not
// settled, that is, handled, or dropped and recovered. When the whole batch
// is done, Run commits every partition's settled records in one commit,
// rewinds each partition with records left to the first of them, pauses the
// partitions that wait for a backoff, and only then lets the group rebalance
// (kgo.BlockRebalanceOnPoll). So no record is committed for a partition that
// was revoked meanwhile, and no partition is rebalanced away mid-batch.
//
// The price of blocked rebalances is that a batch must be done within the
// client's kgo.RebalanceTimeout, and the slowest partition holds up the
// commit and rebalance of the whole batch. Bound it with WithHandlerTimeout
// and WithMaxPollRecords.
//
// Attempts are counted in memory, per partition, for the record at its head.
// A restart or a partition that moves to another member starts counting
// again, so a record may get more than MaxAttempts attempts in total.
//
// # Consumer.Run lifecycle
//
// Run passes its own ctx to the handlers, or a ctx bounded by
// WithHandlerTimeout. The handlers of one record run in subscription order and
// stop at the first error or panic. Run recovers a handler panic as a
// StageHandle failure whose Err is a *PanicError, matching ErrHandlerPanic;
// the ErrorObserver can log PanicError.Stack. A handler that overruns
// WithHandlerTimeout fails with ErrHandlerTimeout; Go cannot stop it, so a
// handler that ignores ctx keeps running while the record is retried. Delivery
// is at least once, so make handlers idempotent, for example keyed by
// Message.ID.
//
// The handlers, the FailurePolicy, the Recoverer, and the ErrorObserver are
// called concurrently for different partitions and must be safe for
// concurrent use.
//
// When ctx ends, each partition stops before its next record, Run commits the
// records already settled on a ctx that is not cancelled, and returns nil. A
// failure that happens after ctx ended, such as a handler cut short by
// shutdown, skips the FailurePolicy and the ErrorObserver and leaves the
// record uncommitted, so Kafka redelivers it. Run lists the errors that stop
// it early; treat them as fatal and stop or restart the process.
//
// Shut down in this order: cancel Run's ctx, wait for Run to return, then close
// the client after every publisher sharing it has stopped. Consumer.Close
// closes the client when WithCloseClient(true) was set; otherwise runtime
// composition closes it with client.Close.
//
// # Errors
//
// The FailurePolicy, the Recoverer, and the ErrorObserver receive an Error
// value. Compare Error.Err, and the errors returned by Run and Publish, with
// errors.Is against the Err* sentinels of this package and of ddd/message.
//
// The upstream ddd/message API is experimental. When this adapter shows that
// the core contract does not fit real Kafka usage, fix the upstream contract
// and keep this adapter a direct mapping.
package kafka
