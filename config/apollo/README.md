# Apollo Config

Apollo-backed implementation of `github.com/go-jimu/components/config`.

## Install

```bash
go get github.com/go-jimu/contrib/config/apollo@latest
```

## Usage

```go
source := apollo.NewSource(
	apollo.WithAppID("billing-service"),
	apollo.WithCluster("default"),
	apollo.WithEndpoint("http://127.0.0.1:8080"),
	apollo.WithNamespace("application.yaml"),
)

kvs, err := source.Load()
if err != nil {
	panic(err)
}
_ = kvs
```

Use `apollo.WithOriginalConfig()` when Apollo content should be exposed without parser normalization.

## Verification

```bash
go test ./...
```

Ported from <https://github.com/go-kratos/kratos/tree/main/contrib/config/apollo>.
