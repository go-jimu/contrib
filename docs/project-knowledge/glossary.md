---
last_updated: 2026-05-10
updated_by: superpowers-memory:rebuild
triggered_by_plan: null
---

# Glossary

**Provider module** — Independently versioned Go module that implements one `go-jimu/components` interface using a concrete backend SDK. → `config/`, `logger/`

**Config Source** — `components/config` abstraction for loading and watching configuration values from a backend. → `config/*`

**Watcher** — `components/config` abstraction used by config providers to surface backend change notifications. → `config/*/watch.go`

**KeyValue** — Shared config value shape returned by provider modules. → `config/*`

**Logger Adapter** — Provider that maps `components/logger` calls onto a concrete logging implementation. → `logger/zap`
