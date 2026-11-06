package xorm

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jimu/components/transaction"
	"github.com/go-sql-driver/mysql"
	xormio "xorm.io/xorm"
)

// DB is one database for go-jimu: the xorm engine and the
// transaction.Manager that runs transactions on it. See "Runtime
// composition". Its methods are safe for concurrent use.
type DB struct {
	engine  *xormio.Engine
	manager *transaction.Manager
}

// New returns the DB of engine. It returns ErrNilEngine when engine is nil.
func New(engine *xormio.Engine) (*DB, error) {
	if engine == nil {
		return nil, ErrNilEngine
	}
	manager, err := transaction.NewManager(&driver{engine: engine})
	if err != nil {
		return nil, err
	}
	return &DB{engine: engine, manager: manager}, nil
}

// Manager returns the transaction.Manager of this database. See "Runtime
// composition".
func (db *DB) Manager() *transaction.Manager {
	return db.manager
}

// Session returns the session for one repository statement. See
// "Repositories". It returns:
//   - the transaction's session when ctx is in this database's transaction;
//   - a pool session when ctx is in no transaction;
//   - transaction.ErrForeignTransaction or transaction.ErrTransactionDone,
//     unwrapped, for a ctx in another database's transaction or in an ended
//     one;
//   - an error matching transaction.ErrTransactionDone and ErrSessionEnded
//     when a repository has ended the transaction's session itself.
func (db *DB) Session(ctx context.Context) (*xormio.Session, error) {
	current, err := db.manager.Current(ctx)
	switch {
	case err == nil:
		// The Manager was built with driver, so every Tx it holds is a *tx.
		session := current.(*tx).session
		if !session.IsInTx() {
			return nil, fmt.Errorf("%w: %w", transaction.ErrTransactionDone, ErrSessionEnded)
		}
		return session.Context(ctx), nil
	case errors.Is(err, transaction.ErrNoTransaction):
		return db.engine.Context(ctx), nil
	default:
		return nil, err
	}
}

// IsMySQLConflict reports whether err is a MySQL lock wait timeout (1205) or
// deadlock (1213). See "Conflicts".
func IsMySQLConflict(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
}

type driver struct {
	engine *xormio.Engine
}

func (d *driver) Begin(ctx context.Context) (transaction.Tx, error) {
	session := d.engine.NewSession().Context(ctx)
	if err := session.Begin(); err != nil {
		_ = session.Close()
		return nil, err
	}
	return &tx{session: session}, nil
}

func (*driver) IsConflict(err error) bool {
	return IsMySQLConflict(err)
}

// tx is one xorm transaction. Commit and Rollback close the session after
// ending the transaction; a Close error only concerns statement caches.
type tx struct {
	session *xormio.Session
}

func (t *tx) Commit() error {
	defer func() { _ = t.session.Close() }()
	if !t.session.IsInTx() {
		return ErrSessionEnded
	}
	return t.session.Commit()
}

func (t *tx) Rollback() error {
	defer func() { _ = t.session.Close() }()
	if !t.session.IsInTx() {
		return ErrSessionEnded
	}
	return t.session.Rollback()
}
