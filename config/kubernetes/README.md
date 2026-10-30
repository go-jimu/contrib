# Kubernetes Config

Kubernetes ConfigMap-backed implementation of `github.com/go-jimu/components/config`.

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

## Watching

Each ConfigMap key becomes one config value, keyed `<namespace>/<configmap>/<key>`. A reload replaces the values of the keys a ConfigMap still has, so added and changed keys take effect while running. Keys removed from a ConfigMap, and deleted ConfigMaps, keep their last value until the process reloads from scratch; a deletion is logged as a watch error.

## Verification

```bash
go test ./...
```
