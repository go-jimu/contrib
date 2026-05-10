---
last_updated: 2026-05-10
updated_by: superpowers-memory:update
triggered_by_plan: 2026-05-10-kafka-message-implementation.md
---

# Architecture

## Pattern Overview

This repository is a Go workspace of external implementations for interfaces from `github.com/go-jimu/components`. Each implementation is an independently versioned Go module under a capability/provider path, such as `config/etcd`, `logger/zap`, or `message/kafka`, and adapts a third-party SDK to a stable component interface.

## System Context

- Consumers: Go services that depend on `go-jimu/components` interfaces and select a contrib provider module.
- Upstream contracts: `github.com/go-jimu/components` packages such as `config`, `logger`, and `ddd/message`.
- External systems: Apollo, etcd, Kubernetes ConfigMaps, Nacos, zap, and Kafka.
- Build and release environment: Go workspace, Makefile test runner, GitHub Actions, and Docker for Kafka integration tests.

## Layering

### Workspace Root

Defines the repository module and Go workspace membership. → `go.mod`, `go.work`

### Config Providers

Each config provider adapts one external configuration backend to `components/config.Source` and watcher contracts. → `config/apollo/`, `config/etcd/`, `config/kubernetes/`, `config/nacos/`

### Logger Providers

Logger providers adapt third-party logging libraries to `components/logger.Logger`. → `logger/zap/`

### Message Providers

Message providers adapt external brokers to `components/ddd/message` publisher and subscriber contracts. → `message/kafka/`

### Call-Direction Rules

Provider modules depend on `go-jimu/components` and their own backend SDK. The root workspace coordinates local development but does not own provider runtime behavior.

## Scenario Sequences

```mermaid
sequenceDiagram
    participant App as Consumer app
    participant Provider as Config provider module
    participant Contract as components/config
    participant Backend as External config backend
    App->>Provider: construct provider with options/client
    Provider-->>App: returns config.Source
    App->>Contract: use Source interface
    Contract->>Provider: Load or Watch
    Provider->>Backend: fetch or subscribe
    Backend-->>Provider: config data/change
    Provider-->>Contract: KeyValue or Watcher event
```

```mermaid
sequenceDiagram
    participant Dev as Maintainer
    participant Make as Makefile
    participant Root as Root Go module
    participant Sub as Provider submodule
    Dev->>Make: make test
    Make->>Root: go test ./...
    Make->>Sub: iterate every nested go.mod
    Sub-->>Make: race-enabled test and coverage output
    Make-->>Dev: combined result
```

```mermaid
sequenceDiagram
    participant App as Consumer app
    participant Pub as message/kafka Publisher
    participant Contract as components/ddd/message
    participant Broker as Kafka broker
    participant Sub as message/kafka Consumer
    participant Handler as message.Handler
    App->>Contract: create message.Message
    App->>Pub: Publish
    Pub->>Broker: produce Kafka record
    Sub->>Broker: poll records
    Sub->>Contract: reconstruct message.Message
    Contract->>Handler: Handle
    Sub->>Broker: commit offset or publish retry/DLQ
```

## Key Object FSMs

No aggregate or cross-bounded-context domain object state machines are present. Existing modules are infrastructure adapters for component interfaces, not domain aggregates.

## Key Design Decisions

- No ADRs recorded yet; current architecture is reflected by the workspace layout and provider modules.
