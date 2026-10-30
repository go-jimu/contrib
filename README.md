# Go Jimu Contrib

Provider adapters for [`github.com/go-jimu/components`](https://github.com/go-jimu/components).

This repository keeps optional integrations outside the core component
contracts. Each provider is an independent Go module under its capability path,
so applications can depend only on the adapters they actually use.

## Packages

| Package | Component contract | Provider |
| --- | --- | --- |
| `config/apollo` | `components/config` | Apollo |
| `config/etcd` | `components/config` | etcd |
| `config/kubernetes` | `components/config` | Kubernetes ConfigMap |
| `config/nacos` | `components/config` | Nacos |
| `message/kafka` | `components/ddd/message` | Kafka through franz-go |
| `taskqueue/asynq` | `components/taskqueue` | asynq |

## Install

Install the provider module you need:

```sh
go get github.com/go-jimu/contrib/taskqueue/asynq@latest
```

Use the package-specific import path for other providers:

```sh
go get github.com/go-jimu/contrib/config/etcd@latest
go get github.com/go-jimu/contrib/message/kafka@latest
```

Package directories have their own `go.mod` files and README documents with
provider-specific usage examples.

## Usage Boundary

Provider packages adapt concrete SDKs to stable `components` contracts. Keep
provider imports in infrastructure or runtime wiring. Business and application
code should depend on the provider-neutral component interfaces such as
`config.Source`, `message.Publisher`, `message.Subscriber`,
`taskqueue.Enqueuer`, `taskqueue.Processor`, and `taskqueue.Worker`.

## Development

The repository uses a Go workspace:

```sh
go work sync
```

Run all default tests from the repository root:

```sh
make test
```

Run one provider module directly:

```sh
cd taskqueue/asynq
go test ./...
```

Run benchmarks:

```sh
make benchmark
```

Kafka integration tests are build-tagged and require Docker:

```sh
cd message/kafka
go test -tags=integration ./...
```

## Release

CI runs on pull requests, `master` pushes, and semantic version tags. Root tags
such as `v0.9.5` trigger package-scoped tags for changed provider modules, for
example `taskqueue/asynq/v0.9.5`.
