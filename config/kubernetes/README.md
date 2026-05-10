# Kubernetes Config

Kubernetes ConfigMap-backed implementation of `github.com/go-jimu/components/config`.

This module is compatible with `github.com/go-jimu/components v0.8.0`.

## Install

```bash
go get github.com/go-jimu/contrib/config/kubernetes@latest
```

## Usage

```go
source := kubernetes.NewSource(
	kubernetes.Namespace("default"),
	kubernetes.LabelSelector("app=billing"),
	kubernetes.KubeConfig("/path/to/kubeconfig"),
)

kvs, err := source.Load()
if err != nil {
	panic(err)
}
_ = kvs
```

Omit `kubernetes.KubeConfig` when the process runs inside a Kubernetes cluster and should use in-cluster credentials.

## Verification

```bash
go test ./...
```
