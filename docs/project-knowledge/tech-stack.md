---
last_updated: 2026-05-10
updated_by: superpowers-memory:update
triggered_by_plan: 2026-05-10-kafka-message-implementation.md
---

# Tech Stack

## Languages And Tooling

| Technology | Version / Source | Purpose | Why Chosen |
| --- | --- | --- | --- |
| Go | Root `go.mod` uses 1.21; `go.work` and Kafka provider use 1.25.0 | Provider implementation language | Matches the `go-jimu/components` ecosystem and current CI matrix. |
| Go workspace | `go.work` | Develop root and provider submodules together | Keeps independently versioned provider modules testable in one checkout. |
| Make | `Makefile` | Test and benchmark orchestration | Runs root and nested module commands consistently. |
| GitHub Actions | `.github/workflows/ci.yml` | CI, coverage upload, package tag release | Runs the Go 1.25.x matrix and package-scoped tags. |
| Docker | Local or CI host capability | Kafka integration test runtime | Required by Testcontainers-backed Kafka tests. |

## Runtime Dependencies

| Dependency | Version / Source | Purpose | Why Chosen |
| --- | --- | --- | --- |
| `github.com/go-jimu/components` | v0.8.0 in config modules and `message/kafka`; v0.4.0 in `logger/zap` | Shared component interfaces | Defines the contracts this repo implements. |
| `github.com/apolloconfig/agollo/v4` | v4.4.0 | Apollo SDK | Native Apollo client for Go. |
| `go.etcd.io/etcd/client/v3` | v3.5.14 | etcd SDK | Official etcd v3 client. |
| `k8s.io/client-go` | v0.30.2 | Kubernetes client | Standard Go client for ConfigMaps. |
| `github.com/nacos-group/nacos-sdk-go/v2` | v2.2.7 | Nacos SDK | Official Nacos Go client. |
| `go.uber.org/zap` | v1.24.0 | Logging backend | Existing logging adapter target. |
| `github.com/twmb/franz-go` | v1.21.1 | Kafka client | Pure-Go Kafka SDK with producer, consumer group, and manual commit support. |
| `github.com/testcontainers/testcontainers-go/modules/kafka` | v0.42.0 | Kafka integration tests | Provides Docker-backed Kafka brokers for build-tagged integration tests. |
