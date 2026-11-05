// Package kafka implements github.com/go-jimu/components/ddd/message on Kafka
// with franz-go: NewPublisher returns a message.Publisher, and Consumer and
// ShareConsumer are each a message.Subscriber and message.Runner.
//
// # Choosing a consumer
//
// Choose by whether the handlers of one key need their messages in order:
//
//   - In order: Consumer, on a consumer group. One goroutine per partition,
//     so the partition count bounds concurrency. A FailurePolicy retries a
//     failed record in place or drops it; a Recoverer keeps dropped records.
//   - Any order: ShareConsumer, on a share group (Kafka 4.2+). WithConcurrency
//     sets concurrency. Retries and dead letters are the broker's.
//
// # Runtime composition
//
// Import this package and github.com/twmb/franz-go/pkg/kgo in runtime
// composition only, such as an internal/pkg/messagebus package. Application,
// domain, and transport code depend on ddd/message alone: message.Publisher,
// message.Subscriber, message.Handler, and *message.PayloadRegistry.
//
// Runtime composition takes these steps:
//
//  1. Build one client with NewClient for each consumer. A Consumer client
//     needs kgo.ConsumerGroup, kgo.DisableAutoCommit, and
//     kgo.BlockRebalanceOnPoll; a ShareConsumer client needs kgo.ShareGroup.
//     Either needs kgo.ConsumeTopics listing the topic of every Kind a handler
//     subscribes to, the Kind itself by default; a Kind whose topic is
//     missing is never delivered.
//  2. Build a *message.PayloadRegistry, the publisher, and the consumer with
//     WithPayloadResolver(registry). The publisher may share the consumer's
//     client.
//  3. Hand the publisher, the registry, and the consumer as a
//     message.Subscriber to the bounded contexts. Before Run starts, each
//     registers a payload factory for every Kind it consumes and subscribes
//     its handlers.
//  4. Start Run on its own goroutine.
//  5. Shut down: cancel Run's ctx, wait for Run to return, then close the
//     client after every publisher sharing it has stopped. Close closes it
//     when WithCloseClient(true) was set. A client serves one Run: to consume
//     again, build a new client and consumer.
//
// Composition is done when the constructor returned no error, every
// subscribed Kind is registered and its topic consumed, and shutdown follows
// step 5.
//
//	client, err := kafka.NewClient(
//		kgo.SeedBrokers(brokers...),
//		kgo.ConsumerGroup("orders-service"),
//		kgo.ConsumeTopics(topics...),
//		kgo.DisableAutoCommit(),
//		kgo.BlockRebalanceOnPoll(),
//		kgo.WithLogger(kafka.KgoLogger(logger, kgo.LogLevelInfo)),
//	)
//	registry := message.NewPayloadRegistry()
//	consumer, err := kafka.NewConsumer(client, kafka.RetryPolicy(kafka.RetryConfig{}),
//		kafka.WithPayloadResolver(registry),
//		kafka.WithHandlerTimeout(5*time.Second),
//		kafka.WithMaxPollRecords(10), // 10 × 5s stays below kgo's 60s RebalanceTimeout
//		kafka.WithLogger(logger),
//		kafka.WithErrorObserver(observe),
//	)
//	publisher := kafka.NewPublisher(client)
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
// The consumers rebuild the Message from the record: Kind from the
// jimu-message-kind header (WithKindResolver for records from other
// producers), Key from Record.Key, ID from its header or a new random ID,
// OccurredAt from its header or Record.Timestamp, and Headers from the
// remaining headers. A record decodes only when its Kind is registered; an
// unregistered Kind fails at StageDecode with message.ErrUnknownKind.
//
// # Handlers
//
// This contract holds for both consumers. The handlers of one record run in
// subscription order and stop at the first error or panic. A panic is
// recovered as a StageHandle failure whose Err is a *PanicError.
//
// Delivery is at least once, so make handlers idempotent, for example keyed
// by Message.ID. Handlers, the FailurePolicy, the ShareFailurePolicy, the
// Recoverer, and the ErrorObserver are called concurrently: make each safe for
// concurrent use and quick to return.
//
// Handlers receive Run's ctx, bounded by WithHandlerTimeout when set. Return
// promptly when ctx ends: a handler past its timeout fails with
// ErrHandlerTimeout and the consumer moves on, while a goroutine that ignores
// ctx keeps running alongside the retry of its record. At shutdown Run waits
// for running handlers, up to the rest of their timeout.
//
// # Consumer failures
//
// A decode error (StageDecode), a Kind with no handler (StageUnhandled), and a
// handler error (StageHandle) are message-level failures. The FailurePolicy
// passed to NewConsumer decides each one, given Error.Attempt, the failed
// attempts at the record so far:
//
//   - RetryPolicy retries a failure in place: the record stays uncommitted,
//     its partition is rewound to it and paused for the WithRetryBackoff delay
//     (DefaultRetryBackoff: 1s, doubling, up to 1 minute), and the record is
//     handled again before any later record of its partition. After
//     MaxAttempts attempts it applies OnExhausted: FailureActionDrop or
//     FailureActionStop. Choose it for transient failures.
//   - DropAndCommitPolicy drops every failure at once.
//   - A FailurePolicyFunc decides from Error.Stage, Error.Err, and
//     Error.Attempt.
//
// RetryPolicy spends attempts only on failures DefaultRetryable accepts:
// StageHandle errors, including ErrHandlerTimeout and ErrHandlerPanic. A
// handler marks a permanent failure, such as invalid input, by returning
// NonRetryable(err); it goes to OnExhausted at once, as decode and unhandled
// failures do.
//
// A record retried in place holds up the rest of its partition until it is
// settled, so a record that always fails delays the records behind it by the
// sum of its backoffs. Attempts are counted in memory for the head of each
// partition, and kept while that record stays the head in this process, even
// across a rebalance that hands the partition back; a restart, or another
// member taking the partition, counts from 1 again.
//
// # Recoverer
//
// The Consumer keeps dropped records through the Recoverer set with
// WithRecoverer, for example in a failed-messages table; it publishes no retry
// or dead-letter records. The Recoverer gets a dropped record, as an Error
// with Final set, before its offset is committed. When it returns nil the
// record is settled. When it returns an error, the failure is observed at
// StageRecover, matching ErrRecoverFailed, and the record stays at the head of
// its partition: after the backoff the Recoverer is called again, without the
// handlers. Alert on StageRecover, since the partition waits meanwhile.
// Without a Recoverer a dropped message is lost, and the ErrorObserver sees
// its Error with Final set.
//
// # Consumer offsets and rebalances
//
// A batch is the records of one poll, at most WithMaxPollRecords
// (DefaultMaxPollRecords). The Consumer splits a batch by partition and
// handles the partitions concurrently, each in offset order, until the head
// of the partition is a record that is not settled. A record is settled when
// its handlers succeed, or when it is dropped and the Recoverer, if any,
// returned nil.
//
// After the whole batch, the Consumer commits each partition's settled
// records in one commit, pauses the partitions waiting for a backoff, rewinds
// each partition with records left to its head, and then allows the group to
// rebalance. kgo.BlockRebalanceOnPoll holds rebalances until then, so every
// commit is for a partition still assigned. The price: a batch must finish
// within the client's kgo.RebalanceTimeout, and its slowest partition holds up
// the commit and rebalance of all. A partition's records run one after
// another, so keep the number of records of one partition in a batch times
// WithHandlerTimeout below kgo.RebalanceTimeout; WithMaxPollRecords bounds
// that number. A member that holds a rebalance too long loses its group
// session: franz-go rejoins by itself, and the uncommitted records of the
// batch are delivered again.
//
// # Share groups
//
// The ShareConsumer handles each batch with at most WithConcurrency handlers
// at once and acknowledges every record before the next poll: accepted when
// its handlers succeed, otherwise as the ShareFailurePolicy decides. A batch
// holds one record per handler by default (WithMaxPollRecords overrides it).
// ReleaseOrRejectPolicy releases a failure DefaultRetryable accepts and
// rejects the others. The broker delivers a released record again, with
// Error.Attempt, the delivery count, incremented, up to the group's
// share.delivery.count.limit (5 by default). It archives a rejected or
// exhausted record, or writes it to the group's dead-letter topic when it
// supports KIP-1191 (share.version 2) and the group is configured for one.
// A release on the last allowed delivery is reported with Error.Final set;
// WithDeliveryLimit tells the consumer the group's limit.
//
// The broker locks each delivered record for share.record.lock.duration.ms
// (30s by default) from the fetch, and delivers it again once the lock
// expires. The ShareConsumer ends the handlers' ctx a margin before the lock
// does, and releases a record whose lock has nearly run out without handling
// it. A record that waits in the client is still locked, so keep the client's
// fetches small (kgo.ShareMaxRecords, about WithConcurrency) and handlers well
// within the lock. Set share.auto.offset.reset to earliest on the group to
// consume records that predate it.
//
// # Poll errors
//
// Each poll error is observed at StagePoll. Data loss (*kgo.ErrDataLoss) and a
// lost group session (*kgo.ErrGroupSession) are informational: franz-go
// recovers by itself, and Run goes on. Any other error stops Run when the
// poll brought no records.
//
// # Shutdown
//
// When Run's ctx ends, the Consumer stops each partition before its next
// record, commits the settled records, and rewinds the rest; the
// ShareConsumer releases the records it has not handled and flushes its
// acknowledgements. Both then return nil, and the ended ctx is not observed.
// A failure that happens after ctx ended, such as a handler cut short, is
// left for redelivery without the policy, the ErrorObserver, or a log.
// Commits and acknowledgement flushes run on a ctx detached from Run's,
// bounded by a timeout, so shutdown completes them. Run lists the errors that
// stop it early; treat them as fatal and stop or restart the process.
//
// # Logging
//
// Both consumers log through WithLogger, slog.Default() by default, with ctx,
// so a handler of the logger can add attributes from ctx such as a trace ID.
// Logs and the ErrorObserver are independent: logs explain what happened,
// while the ErrorObserver feeds metrics and alerts. For the franz-go client's
// own logs, such as group joins and partition assignments, pass KgoLogger to
// kgo.WithLogger.
//
// Every message is a constant string; filter on it. Attributes:
//
//   - kafka_topic, kafka_partition, kafka_offset locate a record.
//   - message_kind and message_id name the message once it decoded.
//   - stage, attempt, final, and error describe a failure, as in Error.
//   - consumer_group or share_group is on every log of a consumer built by
//     NewConsumer or NewShareConsumer.
//   - Durations are integer milliseconds: elapsed_ms, backoff_ms,
//     timeout_ms, rebalance_timeout_ms.
//
// Levels follow whether anything was lost:
//
//   - Error, a message lost or Run stopping: "kafka message dropped" (no
//     Recoverer), "kafka message stopping consumer", "kafka commit failed",
//     "kafka consumer stopped" with an error, "kafka message rejected",
//     "kafka message released on last delivery", "kafka message failure
//     accepted", "kafka acks flush failed", and "kafka share consumer
//     stopped" with an error.
//   - Warn, a failure that heals by itself or needs a look before it costs:
//     "kafka message retrying", "kafka message recovered" (dropped, kept by
//     the Recoverer), "kafka recoverer failed", "kafka message released",
//     "kafka record lock expiring, released unhandled", "kafka poll error",
//     "kafka batch slow" (a batch used half of kgo.RebalanceTimeout), "kafka
//     handler still running after timeout", and "kafka handler still running
//     at shutdown".
//   - Info, lifecycle: "kafka consumer started", "kafka consumer stopped",
//     and the share consumer's pair.
//   - Debug, the steady state: "kafka batch handled" (batch_records,
//     batch_partitions, committed_records, elapsed_ms), "kafka partition
//     paused", and "kafka partition resumed".
//
// A handled message is not logged here. To log each one at Debug, subscribe
// handlers wrapped as message.Chain(handler, message.Logging(logger)); its
// failure log is Debug too, since the logs above give a failure its level.
//
// # Errors
//
// The policies, the Recoverer, and the ErrorObserver receive an Error value.
// Compare Error.Err, and the errors returned by Run and Publish, with
// errors.Is against the Err* sentinels of this package and of ddd/message.
//
// The upstream ddd/message API is experimental. When this adapter shows that
// the core contract does not fit real Kafka usage, fix the upstream contract
// and keep this adapter a direct mapping.
package kafka
