---
last_updated: 2026-05-10
updated_by: superpowers-memory:rebuild
triggered_by_plan: null
---

# Features

## Implemented

### Configuration Providers

#### Apollo Config Source

**Enables** — Consumer services can load Apollo namespaces through the `components/config.Source` interface.

**Actors / Entry Points** — Go applications import `github.com/go-jimu/contrib/config/apollo` and construct a source with options.

**Capability Boundary** — Covers Apollo-backed loading and watching; provider wiring is described in architecture.md §Layering.

**References** — `config/apollo/README.md`, `config/apollo/apollo.go`

#### etcd Config Source

**Enables** — Consumer services can read etcd keys or key prefixes as `components/config.KeyValue` records.

**Actors / Entry Points** — Go applications pass an etcd client to `github.com/go-jimu/contrib/config/etcd`.

**Capability Boundary** — Covers etcd read and watch adaptation to the shared config contract.

**References** — `config/etcd/README.md`, `config/etcd/config.go`

#### Kubernetes Config Source

**Enables** — Consumer services can load Kubernetes ConfigMap data as config key values.

**Actors / Entry Points** — Go applications import `github.com/go-jimu/contrib/config/kubernetes` and provide namespace/selector options.

**Capability Boundary** — Covers in-cluster or kubeconfig-backed ConfigMap access through the shared config contract.

**References** — `config/kubernetes/config.go`

#### Nacos Config Source

**Enables** — Consumer services can load and watch Nacos configuration by data ID and group.

**Actors / Entry Points** — Go applications pass a Nacos config client to `github.com/go-jimu/contrib/config/nacos`.

**Capability Boundary** — Covers Nacos-backed loading and subscription forwarding to the shared config contract.

**References** — `config/nacos/README.md`, `config/nacos/config.go`

### Logging Providers

#### zap Logger Adapter

**Enables** — Consumer services can use a zap logger through `components/logger.Logger`.

**Actors / Entry Points** — Go applications import `github.com/go-jimu/contrib/logger/zap` and wrap an existing zap logger.

**Capability Boundary** — Covers level mapping and key-value field forwarding to zap.

**References** — `logger/zap/README.md`, `logger/zap/logger.go`

## In Progress

### Messaging Providers

#### Kafka DDD Message Adapter

**Intent** — Add a Kafka-backed implementation for the external ddd message component contract.

**Source** — Current brainstorming task.

## Planned

No other planned capabilities are recorded in the repository.
