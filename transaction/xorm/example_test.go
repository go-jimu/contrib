package xorm_test

import (
	"context"

	"github.com/go-jimu/components/transaction"
	jimuxorm "github.com/go-jimu/contrib/transaction/xorm"
	_ "github.com/go-sql-driver/mysql" // registers the "mysql" database/sql driver
	xormio "xorm.io/xorm"
)

type accountDO struct {
	ID      string `xorm:"pk 'id'"`
	Balance int64  `xorm:"'balance'"`
}

// accountRepository resolves its session per statement, so it joins the
// transaction of the ctx it receives.
type accountRepository struct {
	db *jimuxorm.DB
}

func (r accountRepository) Save(ctx context.Context, account accountDO) error {
	session, err := r.db.Session(ctx)
	if err != nil {
		return err
	}
	_, err = session.ID(account.ID).Cols("balance").Update(&account)
	return err
}

// transfer is a use case: it sees only transaction.Transactor.
func transfer(ctx context.Context, transactor transaction.Transactor, accounts accountRepository, from, to accountDO) error {
	return transactor.Within(ctx, func(ctx context.Context) error {
		if err := accounts.Save(ctx, from); err != nil {
			return err
		}
		return accounts.Save(ctx, to)
	})
}

func Example() {
	engine, err := xormio.NewEngine("mysql", "user:password@tcp(127.0.0.1:3306)/app?parseTime=true")
	if err != nil {
		panic(err)
	}
	db, err := jimuxorm.New(engine)
	if err != nil {
		panic(err)
	}
	accounts := accountRepository{db: db}

	_ = transfer(context.Background(), db.Manager(), accounts,
		accountDO{ID: "alice", Balance: 90}, accountDO{ID: "bob", Balance: 110})
}
