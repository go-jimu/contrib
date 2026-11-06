// Package xorm adapts xorm.io/xorm to github.com/go-jimu/components/transaction:
// it runs the transactions of one database and gives repositories the session
// of the transaction their ctx is in. The transaction rules, errors, and hooks
// are those of package transaction; read its documentation first.
//
// # Composition
//
// Runtime composition builds one DB per database and hands it out twice:
//
//  1. db, err := xorm.New(engine) wraps the xorm engine of one database.
//  2. db.Manager() goes to application code as a transaction.Transactor, which
//     use cases call Within on.
//  3. db goes to the repository adapters of that database.
//
// A second database gets its own engine and DB. A ctx in one DB's transaction
// makes the other DB's Session return transaction.ErrForeignTransaction, so a
// write to the second database never looks atomic with the first.
//
// # Repositories
//
// A repository calls Session for each statement and runs the statement on the
// returned session:
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
// on the connection pool that closes itself after one statement. The DB owns
// both: the transaction's session closes when the transaction ends. A write of
// several statements that must be atomic on its own, such as an aggregate
// spread over several tables, runs in db.Manager().WithinOrJoin.
//
// # Conflicts
//
// The DB classifies MySQL errors 1205 (lock wait timeout) and 1213 (deadlock)
// as conflicts by default, so Within reports them as transaction.ErrConflict
// and the use case can run again. A database other than MySQL passes its own
// classifier with WithConflict.
//
// The transaction is bound to the ctx passed to Within: when that ctx ends
// before Commit, database/sql rolls the transaction back and Commit fails, and
// Within reports transaction.ErrOutcomeUnknown.
package xorm
