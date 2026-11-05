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
//  1. Build one client with NewClient. A Consumer client needs
//     kgo.ConsumerGroup, kgo.DisableAutoCommit, and kgo.BlockRebalanceOnPoll;
//     a ShareConsumer client needs kgo.ShareGroup. Either needs
//     kgo.ConsumeTopics listing the topic of every Kind a handler subscribes
//     to, the Kind itself by default; a Kind whose topic is missing is never
//     delivered.
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
//     when WithCloseClient(true) was set.
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
//	)
//	registry := message.NewPayloadRegistry()
//	consumer, err := kafka.NewConsumer(client, kafka.RetryPolicy(kafka.RetryConfig{}),
//		kafka.WithPayloadResolver(registry),
//		kafka.WithHandlerTimeout(30*time.Second),
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
// ctx keeps running alongside the retry of its record.
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
// partition; a restart, or the partition moving to another member, counts
// from 1 again.
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
// the commit and rebalance of all. Bound it with WithHandlerTimeout and
// WithMaxPollRecords.
//
// # Share groups
//
// The ShareConsumer handles each batch with at most WithConcurrency handlers
// at once and acknowledges every record before the next poll: accepted when
// its handlers succeed, otherwise as the ShareFailurePolicy decides.
// ReleaseOrRejectPolicy releases a failure DefaultRetryable accepts and
// rejects the others. The broker delivers a released record again, with
// Error.Attempt, the delivery count, incremented, up to the group's
// share.delivery.count.limit (5 by default). It archives a rejected or
// exhausted record, or writes it to the group's dead-letter topic when it
// supports KIP-1191 (share.version 2) and the group is configured for one.
//
// Configure the share group on the broker: set share.auto.offset.reset to
// earliest to consume records that predate the group, and keep
// WithHandlerTimeout below share.record.lock.duration.ms (30s by default), the
// time after which the broker delivers a held record again.
//
// # Shutdown
//
// When Run's ctx ends, the Consumer stops each partition before its next
// record and commits the settled records; the ShareConsumer releases the
// records it has not handled and flushes its acknowledgements. Both then
// return nil. A failure that happens after ctx ended, such as a handler cut
// short, is left for redelivery without the policy or the ErrorObserver.
// Commits and acknowledgement flushes run on a ctx detached from Run's,
// bounded by a timeout, so shutdown completes them. Run lists the errors that
// stop it early; treat them as fatal and stop or restart the process.
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
