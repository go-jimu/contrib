---
last_updated: 2026-05-10
updated_by: superpowers-memory:rebuild
covers_branch: release/message@df79752
triggered_by_plan: null
---

# Project Knowledge Index

## architecture.md

Repository structure and cross-module flows for the Go workspace of contrib providers.
Key points: independent provider modules adapt `go-jimu/components`; no domain aggregate FSMs exist yet.

## features.md

Current capability map for implemented provider modules and planned Kafka message adapter work.
Key points: config providers cover Apollo, etcd, Kubernetes, and Nacos; logging provider covers zap.

## tech-stack.md

Language, workspace, CI, and key SDK dependencies.
Key points: root uses Go 1.21 while `go.work` uses 1.22.1; config modules use `go-jimu/components` v0.5.6.

## conventions.md

Project-specific layout, adapter, testing, and release conventions.
Key points: new providers should be nested Go modules in `go.work`; `make test` is the repository-level verification command.

## decisions.md

ADR summary log.
Key points: no ADR-level decisions are recorded yet.

## glossary.md

Short definitions for repository-specific terms.
Key points: provider module, Config Source, Watcher, KeyValue, and Logger Adapter.
