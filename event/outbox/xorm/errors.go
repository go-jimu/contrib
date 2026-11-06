package xorm

import "errors"

// Errors returned by this package. Compare with errors.Is.
var (
	// ErrNilDB means New was given a nil DB.
	ErrNilDB = errors.New("outbox xorm: db is nil")

	// ErrInvalidTable means WithTable was given a name other than letters,
	// digits, and underscores.
	ErrInvalidTable = errors.New("outbox xorm: table name is invalid")
)
