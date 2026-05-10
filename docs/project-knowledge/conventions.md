---
last_updated: 2026-05-10
updated_by: superpowers-memory:rebuild
triggered_by_plan: null
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

## Testing

- Use Go tests in the provider module being changed.
- `make test` is the repository-level verification command and runs race-enabled tests for root and nested modules.
- Avoid relying on live external services unless the test is explicitly integration-scoped and documented.

## Release Workflow

- CI runs on `master` push, pull requests to `master`, and semantic version tags.
- Tagged releases create package-scoped tags for listed subpackages in the GitHub Actions matrix.
- When adding a new provider module, update release automation if it should receive package-scoped tags.
