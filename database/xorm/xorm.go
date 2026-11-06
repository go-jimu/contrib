package xorm

import (
	"context"
	"errors"

	"github.com/go-jimu/components/transaction"
	"github.com/go-sql-driver/mysql"
	xormio "xorm.io/xorm"
)

// ErrNilEngine means New was given a nil engine.
var ErrNilEngine = errors.New("xorm: engine is nil")

// DB is one database for go-jimu: the xorm engine and the
// transaction.Manager that runs transactions on it. Build one per database in
// runtime composition; see "Composition". Its methods are safe for concurrent
// use.
type DB struct {
	engine  *xormio.Engine
	manager *transaction.Manager
}

// Option configures New.
type Option func(*config)

type config struct {
	isConflict func(error) bool
}

// WithConflict sets the function that classifies an error as a concurrency
// conflict; see "Conflicts". It defaults to IsMySQLConflict. A nil fn keeps
// the default.
func WithConflict(fn func(error) bool) Option {
	return func(c *config) {
		if fn != nil {
			c.isConflict = fn
		}
	}
}

// New returns the DB of engine. It returns ErrNilEngine when engine is nil.
func New(engine *xormio.Engine, opts ...Option) (*DB, error) {
	if engine == nil {
		return nil, ErrNilEngine
	}
	cfg := config{isConflict: IsMySQLConflict}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	manager, err := transaction.NewManager(&driver{engine: engine, isConflict: cfg.isConflict})
	if err != nil {
		return nil, err
	}
	return &DB{engine: engine, manager: manager}, nil
}

// Manager returns the transaction.Manager of this database. Hand it to
// application code as a transaction.Transactor; see "Composition".
func (db *DB) Manager() *transaction.Manager {
	return db.manager
}

// Session returns the session for one repository statement; see
// "Repositories". It returns the transaction's session when ctx is in this
// database's transaction, and a pool session when ctx is in no transaction.
// It returns transaction.ErrForeignTransaction or
// transaction.ErrTransactionDone, unwrapped, for a ctx in another database's
// transaction or in an ended one.
func (db *DB) Session(ctx context.Context) (*xormio.Session, error) {
	current, err := db.manager.Current(ctx)
	switch {
	case err == nil:
		// The Manager was built with driver, so every Tx it holds is a *tx.
		return current.(*tx).session.Context(ctx), nil
	case errors.Is(err, transaction.ErrNoTransaction):
		return db.engine.Context(ctx), nil
	default:
		return nil, err
	}
}

// IsMySQLConflict reports whether err is a MySQL lock wait timeout (1205) or
// deadlock (1213); see "Conflicts".
func IsMySQLConflict(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
}

type driver struct {
	engine     *xormio.Engine
	isConflict func(error) bool
}

func (d *driver) Begin(ctx context.Context) (transaction.Tx, error) {
	session := d.engine.NewSession().Context(ctx)
	if err := session.Begin(); err != nil {
		_ = session.Close()
		return nil, err
	}
	return &tx{session: session}, nil
}

func (d *driver) IsConflict(err error) bool {
	return d.isConflict(err)
}

// tx is one xorm transaction. The Manager calls exactly one of Commit and
// Rollback, once; each closes the session after ending the transaction.
type tx struct {
	session *xormio.Session
}

func (t *tx) Commit() error {
	err := t.session.Commit()
	_ = t.session.Close() // the commit outcome is what the Manager reports
	return err
}

func (t *tx) Rollback() error {
	err := t.session.Rollback()
	_ = t.session.Close()
	return err
}
