# Nacos Config

Nacos-backed implementation of `github.com/go-jimu/components/config`.

This module is compatible with `github.com/go-jimu/components v0.8.0`.

## Install

```bash
go get github.com/go-jimu/contrib/config/nacos@latest
```

## Usage

```go
client, err := clients.NewConfigClient(vo.NacosClientParam{
	ClientConfig:  constant.ClientConfig{},
	ServerConfigs: []constant.ServerConfig{},
})
if err != nil {
	panic(err)
}

source := nacos.NewConfigSource(
	client,
	nacos.WithGroup("DEFAULT_GROUP"),
	nacos.WithDataID("app.yaml"),
)

kvs, err := source.Load()
if err != nil {
	panic(err)
}
_ = kvs
```

The source also implements `Watch()` by registering a Nacos config listener for the same `group` and `dataID`.

## Verification

```bash
go test ./...
```

Ported from <https://github.com/go-kratos/kratos/tree/main/contrib/config/nacos>.
