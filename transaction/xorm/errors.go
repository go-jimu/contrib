package xorm

import "errors"

// Errors returned by this package. Compare with errors.Is.
var (
	// ErrNilEngine means New was given a nil *xorm.Engine.
	ErrNilEngine = errors.New("xorm engine is nil")

	// ErrSessionEnded means a repository ended the transaction's session
	// itself, through Close, Commit, or Rollback, before the Manager did.
	// Session returns it for later statements, and Within's error matches it.
	// Statements run on that session after it ended may have autocommitted,
	// so the outcome is unknown. See "Repositories".
	ErrSessionEnded = errors.New("xorm transaction session was ended by a repository")
)
