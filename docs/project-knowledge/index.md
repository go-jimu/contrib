---
last_updated: 2026-05-10
updated_by: superpowers-memory:update
covers_branch: release/message@c5e30f0
triggered_by_plan: 2026-05-10-kafka-message-implementation.md
---

# Project Knowledge Index

## architecture.md

Repository structure and cross-module flows for the Go workspace of contrib providers.
Key points: provider modules now include config, logger, and Kafka message adapters; Kafka publish/consume flow crosses `ddd/message`, franz-go, and Kafka.

## features.md

Current capability map for implemented provider modules.
Key points: config providers cover Apollo, etcd, Kubernetes, and Nacos; logging covers zap; messaging covers Kafka Publisher/Consumer for `ddd/message`.

## tech-stack.md

Language, workspace, CI, and key SDK dependencies.
Key points: workspace/CI are on Go 1.25.x while root remains Go 1.21; config modules and Kafka use `components` v0.8.0.

## conventions.md

Project-specific layout, adapter, testing, messaging, and release conventions.
Key points: new providers belong in `go.work` and release matrix; Kafka consumers require manual commit semantics and Docker-backed integration tests use the `integration` tag.

## decisions.md

ADR summary log.
Key points: no ADR-level decisions are recorded yet.

## glossary.md

Short definitions for repository-specific terms.
Key points: provider module, Config Source, Logger Adapter, Message Provider, PayloadResolver, and DLQ.
