# xorm Database

xorm adapter for `github.com/go-jimu/components/transaction`, built on
`xorm.io/xorm`.

The contracts live in the package documentation, which `go doc -all
github.com/go-jimu/contrib/database/xorm` prints: composition, how
repositories resolve their session, and conflict classification. The
transaction rules, errors, and hooks are those of `go doc -all
github.com/go-jimu/components/transaction`. `example_test.go` compiles a use
case and a repository.

## Install

```sh
go get github.com/go-jimu/contrib/database/xorm@latest
```

## Composition

Build one `DB` per database. Application code receives `db.Manager()` as a
`transaction.Transactor`; repositories of that database receive `db`.

```go
engine, err := xorm.NewEngine("mysql", dsn)
if err != nil {
	return err
}
db, err := jimuxorm.New(engine)
if err != nil {
	return err
}

users := infrastructure.NewUserRepository(db)  // calls db.Session(ctx) per statement
app := application.New(db.Manager(), users)    // calls Within for each use case
```

## Integration tests

The integration tests run MySQL 8.4 through testcontainers and require
Docker:

```sh
cd database/xorm
go test -tags=integration ./...
```
