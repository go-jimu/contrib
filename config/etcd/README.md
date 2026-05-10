# Etcd Config

Etcd-backed implementation of `github.com/go-jimu/components/config`.

This module is compatible with `github.com/go-jimu/components v0.8.0`.

## Install

```bash
go get github.com/go-jimu/contrib/config/etcd@latest
```

## Usage

```go
client, err := clientv3.New(clientv3.Config{
	Endpoints: []string{"127.0.0.1:2379"},
})
if err != nil {
	panic(err)
}
defer client.Close()

source, err := etcd.New(
	client,
	etcd.WithPath("/config/app.yaml"),
)
if err != nil {
	panic(err)
}

kvs, err := source.Load()
if err != nil {
	panic(err)
}
_ = kvs
```

Use `etcd.WithPrefix(true)` when the configured path should load all keys under a prefix.

## Verification

```bash
go test ./...
```

Ported from <https://github.com/go-kratos/kratos/tree/main/contrib/config/etcd>.
