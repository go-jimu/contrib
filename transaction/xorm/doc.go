// Package xorm adapts xorm.io/xorm to github.com/go-jimu/components/transaction:
// it runs the transactions of one database and gives repositories the session
// of the transaction their ctx is in. The transaction rules, errors, and hooks
// are those of package transaction; read its documentation first.
//
// # Runtime composition
//
// Runtime composition builds one DB per database and hands it out twice:
//
//  1. db, err := xorm.New(engine) wraps the xorm engine of one database and
//     builds its transaction.Manager.
//  2. db.Manager() goes to application code as a transaction.Transactor.
//  3. db goes to the repository adapters of that database.
//
// Another database gets its own engine and DB; rule 5 of the transaction
// package's "Running a use case" applies between them.
//
// # Repositories
//
// A repository calls Session for each statement and runs that one statement
// on the returned session:
//
//	func (r *userRepository) Save(ctx context.Context, user *domain.User) error {
//		session, err := r.db.Session(ctx)
//		if err != nil {
//			return err
//		}
//		_, err = session.Insert(toDO(user))
//		return err
//	}
//
// Inside Within, Session returns the transaction's session; outside, a session
// on the connection pool that closes itself after one statement. The DB ends
// and closes the transaction's session when the Manager commits or rolls back,
// so repositories leave Close, Commit, and Rollback to it. A repository that
// ends the transaction's session itself ends the transaction early: Session
// returns ErrSessionEnded for later statements, and Within returns an error
// matching ErrSessionEnded, and transaction.ErrOutcomeUnknown when fn
// returned nil. Treat it as an unknown outcome: statements run on the ended
// session may have autocommitted.
//
// The transaction package's "Repositories" section covers the rest: the
// errors Session passes through, and WithinOrJoin for a write that must be
// atomic on its own.
//
// # Conflicts
//
// The DB classifies MySQL errors 1205 (lock wait timeout) and 1213 (deadlock)
// as conflicts; see the transaction package's "Conflict and unknown outcome".
// It recognizes no other database's conflict errors.
//
// The transaction is bound to the ctx passed to Within: when that ctx ends
// before Commit, database/sql rolls the transaction back and Commit fails, so
// Within reports transaction.ErrOutcomeUnknown.
package xorm
