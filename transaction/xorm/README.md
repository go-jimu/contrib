# xorm Transaction

xorm adapter for `github.com/go-jimu/components/transaction`, built on
`xorm.io/xorm`.

The contracts live in the package documentation, which `go doc -all
github.com/go-jimu/contrib/transaction/xorm` prints: runtime composition, how
repositories resolve their session, and conflict classification. The
transaction rules, errors, and hooks are those of `go doc -all
github.com/go-jimu/components/transaction`. `example_test.go` compiles a use
case running two repository writes in one transaction.

## Install

```sh
go get github.com/go-jimu/contrib/transaction/xorm@latest
```

## Integration Tests

The integration tests run MySQL 8.4 through testcontainers and require
Docker:

```sh
cd transaction/xorm
go test -tags=integration ./...
```
