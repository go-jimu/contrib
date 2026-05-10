---
last_updated: 2026-05-10
updated_by: superpowers-memory:update
triggered_by_plan: 2026-05-10-kafka-message-implementation.md
---

# Glossary

**Provider module** — Independently versioned Go module that implements one `go-jimu/components` interface using a concrete backend SDK. → `config/`, `logger/`

**Config Source** — `components/config` abstraction for loading and watching configuration values from a backend. → `config/*`

**Watcher** — `components/config` abstraction used by config providers to surface backend change notifications. → `config/*/watch.go`

**KeyValue** — Shared config value shape returned by provider modules. → `config/*`

**Logger Adapter** — Provider that maps `components/logger` calls onto a concrete logging implementation. → `logger/zap`

**Message Provider** — Provider module that maps `components/ddd/message` publish/subscribe contracts onto a broker. → `message/kafka`

**PayloadResolver** — Kafka provider hook that returns an empty protobuf payload for an incoming `message.Kind` before decode. → `message/kafka`

**DLQ** — Dead-letter topic used by the Kafka provider after non-retryable or exhausted processing failures. → `message/kafka`
