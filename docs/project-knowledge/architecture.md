---
last_updated: 2026-05-10
updated_by: superpowers-memory:rebuild
triggered_by_plan: null
---

# Architecture

## Pattern Overview

This repository is a Go workspace of external implementations for interfaces from `github.com/go-jimu/components`. Each implementation is an independently versioned Go module under a capability/provider path, such as `config/etcd` or `logger/zap`, and adapts a third-party SDK to a stable component interface.

## System Context

- Consumers: Go services that depend on `go-jimu/components` interfaces and select a contrib provider module.
- Upstream contracts: `github.com/go-jimu/components` packages such as `config` and `logger`.
- External systems: Apollo, etcd, Kubernetes ConfigMaps, Nacos, and zap.
- Build and release environment: Go workspace, Makefile test runner, GitHub Actions.

## Layering

### Workspace Root

Defines the repository module and Go workspace membership. → `go.mod`, `go.work`

### Config Providers

Each config provider adapts one external configuration backend to `components/config.Source` and watcher contracts. → `config/apollo/`, `config/etcd/`, `config/kubernetes/`, `config/nacos/`

### Logger Providers

Logger providers adapt third-party logging libraries to `components/logger.Logger`. → `logger/zap/`

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
    participant Tag as Release tag
    participant CI as GitHub Actions
    participant Repo as Repository
    participant SubPkg as Provider subpackage
    Tag->>CI: push version tag
    CI->>Repo: checkout
    CI->>SubPkg: create package-scoped tag
    SubPkg-->>Repo: push provider tag
```

## Key Object FSMs

No aggregate or cross-bounded-context domain object state machines are present. Existing modules are infrastructure adapters for component interfaces, not domain aggregates.

## Key Design Decisions

- No ADRs recorded yet; current architecture is reflected by the workspace layout and provider modules.
