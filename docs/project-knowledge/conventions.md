---
last_updated: 2026-05-10
updated_by: superpowers-memory:update
triggered_by_plan: 2026-05-10-kafka-message-implementation.md
---

# Conventions

## Repository Layout

- Add each provider as its own Go module under a capability/provider path, for example `config/etcd` or `logger/zap`.
- Add new provider modules to `go.work` so local workspace tests include them.
- Keep provider README files short and focused on package usage or source lineage.

## Component Adapter Style

- Provider packages adapt a concrete SDK to a `go-jimu/components` interface rather than defining a new application-level contract.
- Constructors should accept SDK clients or options in the package style already used by neighboring modules.
- Runtime behavior belongs in the provider submodule; the root module is only workspace coordination.

## Messaging Provider Rules

- Kafka consumers must use `kgo.DisableAutoCommit()` because the provider owns manual commit after handler success or durable retry/DLQ handoff.
- Kafka message providers reserve `jimu-message-id`, `jimu-message-kind`, `jimu-message-occurred-at`, and retry/DLQ `jimu-*` failure headers; caller headers must not override adapter metadata.
- `Message.Key()` maps to the Kafka record key; message IDs stay in reserved headers.
- Consuming protobuf payloads requires a `PayloadResolver`; publishing does not require one.
- Default Kafka retry/DLQ policy uses `<source-topic>.retry`, `<source-topic>.dlq`, and max attempts `3`.
- With DLQ disabled, a custom `ErrorHandler` returning nil intentionally drops and commits the failed source record; the default handler returns the original error and leaves it uncommitted.

## Testing

- Use Go tests in the provider module being changed.
- `make test` is the repository-level verification command and runs race-enabled tests for root and nested modules.
- Avoid relying on live external services unless the test is explicitly integration-scoped and documented.
- Kafka integration tests use the `integration` build tag and require Docker.

## Release Workflow

- CI runs on `master` push, pull requests to `master`, and semantic version tags.
- CI uses a Go 1.25.x matrix.
- Tagged releases create package-scoped tags for listed subpackages in the GitHub Actions matrix.
- When adding a new provider module, update release automation if it should receive package-scoped tags.
