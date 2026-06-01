---
last_updated: 2026-06-01
updated_by: superpowers-memory:update
triggered_by_plan: 2026-06-01-kafka-explicit-failure-policy
---

# Conventions

## Repository Layout

- Add each provider as its own Go module under a capability/provider path, for example `config/etcd` or `logger/zap`.
- Add new provider modules to `go.work` so local workspace tests include them.
- Keep provider README files short and focused on package usage or source lineage.
- Keep repository-scoped Codex settings in `.codex/config.toml`; never store credentials or service tokens there.

## Component Adapter Style

- Provider packages adapt a concrete SDK to a `go-jimu/components` interface rather than defining a new application-level contract.
- Constructors should accept SDK clients or options in the package style already used by neighboring modules.
- Runtime behavior belongs in the provider submodule; the root module is only workspace coordination.

## Messaging Provider Rules

- Kafka consumers must use `kgo.DisableAutoCommit()` because the provider owns manual commit after handler success or durable retry/DLQ handoff.
- Kafka message providers reserve `jimu-message-id`, `jimu-message-kind`, `jimu-message-occurred-at`, and retry/DLQ `jimu-*` failure headers; caller headers must not override adapter metadata.
- `Message.Key()` maps to the Kafka record key; message IDs stay in reserved headers.
- Consuming protobuf payloads requires a `PayloadResolver`; publishing does not require one.
- Kafka `NewConsumer` requires an explicit `FailurePolicy`; message-level failures from `Handler.Handle`, decode, or unhandled-kind routing do not stop `Run` by themselves.
- `DropAndCommitPolicy` is the explicit ignore/default policy: observe the message-level failure, commit the source offset, and continue consumption.
- Retry/DLQ topics are opt-in. Use `WithDefaultFailureTopics` for `<source-topic>.retry` and `<source-topic>.dlq`, or provide custom topic resolvers.
- `WithErrorObserver` is observation only; it must not decide drop/retry/DLQ/stop control flow.
- Runtime-level failures such as poll failure without records, retry/DLQ publish failure with stop fallback, and offset commit failure may still return from `Run`.
- Publisher `WithPublishRetry` handles transient `ProduceSync` failures; deterministic message-to-record mapping errors are returned before producing and are not retried.

## Testing

- Use Go tests in the provider module being changed.
- `make test` is the repository-level verification command and runs race-enabled tests for root and nested modules.
- Avoid relying on live external services unless the test is explicitly integration-scoped and documented.
- Kafka integration tests use the `integration` build tag and require Docker.

## Release Workflow

- CI runs on `master` push, pull requests to `master`, and semantic version tags.
- CI uses a Go 1.25.x matrix.
- CI workflow actions should stay on supported major versions; deprecated artifact actions break before tests start.
- Tagged releases create package-scoped tags for listed subpackages in the GitHub Actions matrix.
- When adding a new provider module, update release automation if it should receive package-scoped tags.
