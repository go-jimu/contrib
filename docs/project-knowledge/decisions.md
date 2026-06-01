---
last_updated: 2026-06-01
updated_by: superpowers-memory:update
triggered_by_plan: 2026-06-01-kafka-explicit-failure-policy
---

# Decisions

## 2026-06-01 — Kafka Message Failures Use Explicit Policies

`message/kafka.NewConsumer` requires a `FailurePolicy` so callers must choose how message-level failures are handled. Built-in policies cover drop-and-commit, DLQ, and retry-then-DLQ. Retry/DLQ topic naming is opt-in through `WithDefaultFailureTopics` or custom topic resolvers, avoiding hidden runtime dependencies on undeclared Kafka topics. `WithErrorObserver` is observation only; message-level control flow belongs to `FailurePolicy`, while runtime-level failures are returned from `Consumer.Run`.
