package xorm

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-jimu/components/ddd/event"
	"github.com/go-jimu/components/ddd/event/outbox"
	jimuxorm "github.com/go-jimu/contrib/transaction/xorm"
	xormio "xorm.io/xorm"
)

// DefaultTable is the table a Store uses without WithTable.
const DefaultTable = "outbox_records"

// purgeBatch bounds the rows one Purge statement deletes, so a large purge
// holds locks briefly.
const purgeBatch = 1000

var tableName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Store is an outbox.Store in a MySQL table of db; see the package
// documentation. Its methods are safe for concurrent use.
type Store struct {
	db    *jimuxorm.DB
	table string
}

var _ outbox.Store = (*Store)(nil)

// Option configures New.
type Option func(*Store)

// WithTable sets the table name, DefaultTable by default.
func WithTable(name string) Option {
	return func(s *Store) { s.table = name }
}

// New returns the Store of db. It returns ErrNilDB or ErrInvalidTable.
func New(db *jimuxorm.DB, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	s := &Store{db: db, table: DefaultTable}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if !tableName.MatchString(s.table) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTable, s.table)
	}
	return s, nil
}

// Schema returns the CREATE TABLE statement of the Store's table, for a
// migration tool.
func (s *Store) Schema() string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` ("+`
  id              VARCHAR(64)  NOT NULL,
  handler         VARCHAR(191) NOT NULL,
  events          LONGTEXT     NOT NULL,
  event_count     INT          NOT NULL,
  progress        INT          NOT NULL,
  status          TINYINT      NOT NULL,
  attempts        INT          NOT NULL,
  lease_owner     VARCHAR(191) NOT NULL,
  lease_until     BIGINT       NOT NULL,
  next_attempt_at BIGINT       NOT NULL,
  last_error      TEXT         NOT NULL,
  trace           TEXT         NOT NULL,
  created_at      BIGINT       NOT NULL,
  updated_at      BIGINT       NOT NULL,
  PRIMARY KEY (id),
  KEY idx_due (status, handler, next_attempt_at),
  KEY idx_created (status, created_at),
  KEY idx_updated (status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, s.table)
}

// CreateTable creates the Store's table when it does not exist.
func (s *Store) CreateTable(ctx context.Context) error {
	return s.exec(ctx, s.Schema())
}

// row is one record as stored.
type row struct {
	ID            string `xorm:"'id'"`
	Handler       string `xorm:"'handler'"`
	Events        string `xorm:"'events'"`
	EventCount    int    `xorm:"'event_count'"`
	Progress      int    `xorm:"'progress'"`
	Status        int    `xorm:"'status'"`
	Attempts      int    `xorm:"'attempts'"`
	LeaseOwner    string `xorm:"'lease_owner'"`
	LeaseUntil    int64  `xorm:"'lease_until'"`
	NextAttemptAt int64  `xorm:"'next_attempt_at'"`
	LastError     string `xorm:"'last_error'"`
	Trace         string `xorm:"'trace'"`
	CreatedAt     int64  `xorm:"'created_at'"`
	UpdatedAt     int64  `xorm:"'updated_at'"`
}

const columns = "id, handler, events, event_count, progress, status, attempts, lease_owner, " +
	"lease_until, next_attempt_at, last_error, trace, created_at, updated_at"

// storedEvent is the JSON form of one event in the events column.
type storedEvent struct {
	Kind    event.Kind      `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// Insert implements outbox.Store: it writes through db.TxSession, so it
// returns transaction.ErrNoTransaction outside a transaction.
func (s *Store) Insert(ctx context.Context, records []outbox.Record) error {
	if len(records) == 0 {
		return nil
	}
	session, err := s.db.TxSession(ctx)
	if err != nil {
		return err
	}
	placeholders := make([]string, len(records))
	args := []any{}
	for i, r := range records {
		stored, err := toRow(r)
		if err != nil {
			return err
		}
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args, stored.ID, stored.Handler, stored.Events, stored.EventCount, stored.Progress,
			stored.Status, stored.Attempts, stored.LeaseOwner, stored.LeaseUntil, stored.NextAttemptAt,
			stored.LastError, stored.Trace, stored.CreatedAt, stored.UpdatedAt)
	}
	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", s.table, columns, strings.Join(placeholders, ", "))
	_, err = session.Exec(append([]any{query}, args...)...)
	return err
}

// Claim implements outbox.Store, in a transaction of its own.
func (s *Store) Claim(ctx context.Context, req outbox.ClaimRequest) ([]outbox.Record, error) {
	if len(req.Handlers) == 0 || req.Limit <= 0 {
		return nil, nil
	}
	var claimed []outbox.Record
	err := s.db.Manager().Within(ctx, func(ctx context.Context) error {
		session, err := s.db.TxSession(ctx)
		if err != nil {
			return err
		}
		handlers, handlerArgs := in(req.Handlers)
		var ids []string
		err = session.SQL(fmt.Sprintf(
			"SELECT id FROM `%s` WHERE status = ? AND handler IN (%s) AND next_attempt_at <= ? AND lease_until <= ? "+
				"ORDER BY created_at, id LIMIT ? FOR UPDATE SKIP LOCKED", s.table, handlers),
			append(append([]any{int(outbox.StatusPending)}, handlerArgs...),
				micros(req.Now), micros(req.Now), req.Limit)...).Find(&ids)
		if err != nil || len(ids) == 0 {
			return err
		}
		idList, idArgs := in(ids)
		update := fmt.Sprintf("UPDATE `%s` SET lease_owner = ?, lease_until = ?, attempts = attempts + 1, updated_at = ? "+
			"WHERE id IN (%s)", s.table, idList)
		if _, err := session.Exec(append([]any{update, req.Owner, micros(req.LeaseUntil), now()}, idArgs...)...); err != nil {
			return err
		}
		claimed, err = s.find(session, fmt.Sprintf("id IN (%s) ORDER BY created_at, id", idList), idArgs...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// Advance implements outbox.Store: on db.Session, it joins the transaction
// ctx is in, where its row lock holds the record until that transaction
// ends, and returns transaction.ErrForeignTransaction in another DB's.
func (s *Store) Advance(ctx context.Context, lease outbox.Lease, progress int) error {
	return s.fenced(ctx, lease, "progress = GREATEST(progress, ?)", progress)
}

// Complete implements outbox.Store.
func (s *Store) Complete(ctx context.Context, lease outbox.Lease) error {
	return s.fenced(ctx, lease, "status = ?, lease_owner = '', lease_until = 0", int(outbox.StatusDone))
}

// Retry implements outbox.Store.
func (s *Store) Retry(ctx context.Context, lease outbox.Lease, nextAttemptAt time.Time, cause string) error {
	return s.fenced(ctx, lease, "next_attempt_at = ?, last_error = ?, lease_owner = '', lease_until = 0",
		micros(nextAttemptAt), cause)
}

// Fail implements outbox.Store.
func (s *Store) Fail(ctx context.Context, lease outbox.Lease, cause string) error {
	return s.fenced(ctx, lease, "status = ?, last_error = ?, lease_owner = '', lease_until = 0",
		int(outbox.StatusFailed), cause)
}

// RetryFailed implements outbox.Store.
func (s *Store) RetryFailed(ctx context.Context, at time.Time) (int, error) {
	return s.affected(ctx, fmt.Sprintf("UPDATE `%s` SET status = ?, next_attempt_at = ?, updated_at = ? WHERE status = ?", s.table),
		int(outbox.StatusPending), micros(at), now(), int(outbox.StatusFailed))
}

// Skip implements outbox.Store.
func (s *Store) Skip(ctx context.Context, id string, index int) error {
	n, err := s.affected(ctx, fmt.Sprintf(
		"UPDATE `%s` SET progress = ?, status = IF(? >= event_count, ?, ?), lease_owner = '', lease_until = 0, updated_at = ? "+
			"WHERE id = ? AND progress = ? AND status IN (?, ?)", s.table),
		index+1, index+1, int(outbox.StatusDone), int(outbox.StatusPending), now(),
		id, index, int(outbox.StatusPending), int(outbox.StatusFailed))
	if err != nil {
		return err
	}
	if n == 0 {
		return outbox.ErrRecordNotFound
	}
	return nil
}

// Purge implements outbox.Store, deleting in batches.
func (s *Store) Purge(ctx context.Context, before time.Time) (int, error) {
	total := 0
	for {
		n, err := s.affected(ctx, fmt.Sprintf("DELETE FROM `%s` WHERE status = ? AND updated_at < ? LIMIT %d", s.table, purgeBatch),
			int(outbox.StatusDone), micros(before))
		total += n
		if err != nil || n < purgeBatch {
			return total, err
		}
	}
}

// Counts implements outbox.Store.
func (s *Store) Counts(ctx context.Context) ([]outbox.Count, error) {
	session, err := s.db.Session(ctx)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Handler string `xorm:"'handler'"`
		Status  int    `xorm:"'status'"`
		Records int    `xorm:"'records'"`
		Oldest  int64  `xorm:"'oldest'"`
	}
	err = session.SQL(fmt.Sprintf("SELECT handler, status, COUNT(*) AS records, MIN(created_at) AS oldest FROM `%s` "+
		"WHERE status IN (?, ?) GROUP BY handler, status ORDER BY handler, status", s.table),
		int(outbox.StatusPending), int(outbox.StatusFailed)).Find(&rows)
	if err != nil {
		return nil, err
	}
	counts := make([]outbox.Count, len(rows))
	for i, r := range rows {
		counts[i] = outbox.Count{Handler: r.Handler, Status: outbox.Status(r.Status), Records: r.Records, Oldest: fromMicros(r.Oldest)}
	}
	return counts, nil
}

// Failed implements outbox.Store.
func (s *Store) Failed(ctx context.Context, limit int) ([]outbox.Record, error) {
	session, err := s.db.Session(ctx)
	if err != nil {
		return nil, err
	}
	return s.find(session, "status = ? ORDER BY created_at, id LIMIT ?", int(outbox.StatusFailed), limit)
}

// fenced applies set to the record while lease holds, and returns
// outbox.ErrLeaseLost otherwise. updated_at changes on every update, so
// MySQL reports the row as affected whenever the lease matched.
func (s *Store) fenced(ctx context.Context, lease outbox.Lease, set string, args ...any) error {
	query := fmt.Sprintf("UPDATE `%s` SET %s, updated_at = ? WHERE id = ? AND status = ? AND lease_owner = ? AND attempts = ?",
		s.table, set)
	args = append(args, now(), lease.RecordID, int(outbox.StatusPending), lease.Owner, lease.Attempt)
	n, err := s.affected(ctx, query, args...)
	if err != nil {
		return err
	}
	if n == 0 {
		return outbox.ErrLeaseLost
	}
	return nil
}

// affected runs one statement on db.Session, joining ctx's transaction when
// there is one, and returns the rows it affected.
func (s *Store) affected(ctx context.Context, query string, args ...any) (int, error) {
	session, err := s.db.Session(ctx)
	if err != nil {
		return 0, err
	}
	result, err := session.Exec(append([]any{query}, args...)...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	return int(n), err
}

func (s *Store) exec(ctx context.Context, query string) error {
	_, err := s.affected(ctx, query)
	return err
}

func (s *Store) find(session *xormio.Session, where string, args ...any) ([]outbox.Record, error) {
	var rows []row
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s", columns, s.table, where)
	if err := session.SQL(query, args...).Find(&rows); err != nil {
		return nil, err
	}
	records := make([]outbox.Record, len(rows))
	for i, r := range rows {
		record, err := fromRow(r)
		if err != nil {
			return nil, err
		}
		records[i] = record
	}
	return records, nil
}

func toRow(r outbox.Record) (row, error) {
	events := make([]storedEvent, len(r.Events))
	for i, e := range r.Events {
		events[i] = storedEvent{Kind: e.Kind, Payload: e.Payload}
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		return row{}, fmt.Errorf("outbox xorm: record %s: events must be JSON: %w", r.ID, err)
	}
	trace := ""
	if r.Trace != nil {
		b, err := json.Marshal(r.Trace)
		if err != nil {
			return row{}, err
		}
		trace = string(b)
	}
	return row{
		ID: r.ID, Handler: r.Handler, Events: string(encoded), EventCount: len(r.Events),
		Progress: r.Progress, Status: int(r.Status), Attempts: r.Attempts, LeaseOwner: r.LeaseOwner,
		LeaseUntil: micros(r.LeaseUntil), NextAttemptAt: micros(r.NextAttemptAt), LastError: r.LastError,
		Trace: trace, CreatedAt: micros(r.CreatedAt), UpdatedAt: micros(r.UpdatedAt),
	}, nil
}

func fromRow(r row) (outbox.Record, error) {
	var events []storedEvent
	if err := json.Unmarshal([]byte(r.Events), &events); err != nil {
		return outbox.Record{}, fmt.Errorf("outbox xorm: record %s: events: %w", r.ID, err)
	}
	encoded := make([]outbox.EncodedEvent, len(events))
	for i, e := range events {
		encoded[i] = outbox.EncodedEvent{Kind: e.Kind, Payload: []byte(e.Payload)}
	}
	var trace map[string]string
	if r.Trace != "" {
		if err := json.Unmarshal([]byte(r.Trace), &trace); err != nil {
			return outbox.Record{}, fmt.Errorf("outbox xorm: record %s: trace: %w", r.ID, err)
		}
	}
	return outbox.Record{
		ID: r.ID, Handler: r.Handler, Events: encoded, Progress: r.Progress, Status: outbox.Status(r.Status),
		Attempts: r.Attempts, LeaseOwner: r.LeaseOwner, LeaseUntil: fromMicros(r.LeaseUntil),
		NextAttemptAt: fromMicros(r.NextAttemptAt), LastError: r.LastError, Trace: trace,
		CreatedAt: fromMicros(r.CreatedAt), UpdatedAt: fromMicros(r.UpdatedAt),
	}, nil
}

// in returns "?, ?, ..." and the args for an IN list.
func in[T any](values []T) (string, []any) {
	args := make([]any, len(values))
	for i, v := range values {
		args[i] = v
	}
	return strings.TrimSuffix(strings.Repeat("?, ", len(values)), ", "), args
}

func now() int64 { return micros(time.Now()) }

// micros stores t as Unix microseconds, the zero time as 0.
func micros(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMicro()
}

func fromMicros(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us)
}
