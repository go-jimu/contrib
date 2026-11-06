// Package xorm is the MySQL outbox.Store of go-jimu, on a
// transaction/xorm DB: records live in a table of the business database, and
// Insert writes in the use case's transaction. MySQL 8.0 or later is
// required, for FOR UPDATE SKIP LOCKED.
//
// # Runtime composition
//
//	db, err := jimuxorm.New(engine)             // contrib/transaction/xorm
//	store, err := outboxxorm.New(db)
//	err = store.CreateTable(ctx)                // or run Schema() in a migration
//	ob, err := outbox.New(store, registry, ...) // components/ddd/event/outbox
//
// Use cases run in db.Manager().Within, the transaction Insert joins; see
// "Runtime composition" in package outbox for the rest.
//
// # Table
//
// One table, outbox_records by default (WithTable), holds the records of
// every Outbox on the database; Schema returns its DDL. Times are BIGINT Unix
// microseconds, so the DSN's parseTime and loc settings do not affect them,
// and 0 means unset. Events are stored as JSON text, readable when an
// operator inspects a failed record:
//
//	SELECT id, handler, progress, last_error, events
//	FROM outbox_records WHERE status = 3 ORDER BY created_at;
//
// Status values are those of outbox.Status: 1 pending, 2 done, 3 failed.
package xorm
