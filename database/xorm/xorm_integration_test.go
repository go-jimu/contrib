//go:build integration

package xorm_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-jimu/components/transaction"
	jimuxorm "github.com/go-jimu/contrib/database/xorm"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	xormio "xorm.io/xorm"
)

const integrationMySQLImage = "mysql:8.4"

type itemDO struct {
	ID    string `xorm:"pk varchar(36) 'id'"`
	Value int    `xorm:"int 'value'"`
}

func (itemDO) TableName() string { return "item" }

// startMySQL starts one MySQL server for the whole test binary run and
// returns its DSN.
var startMySQL = sync.OnceValues(func() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	_, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        integrationMySQLImage,
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "secret",
				"MYSQL_DATABASE":      "jimu",
			},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{
					network.MustParsePort("3306/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}},
				}
			},
			// The entrypoint starts a temporary server for initialization
			// first, so the real server is the second "ready for
			// connections".
			WaitingFor: wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return "", fmt.Errorf("start mysql container: %w", err)
	}
	// testcontainers' reaper removes the container when the test binary
	// exits.
	return fmt.Sprintf("root:secret@tcp(127.0.0.1:%d)/jimu?parseTime=true", port), nil
})

// newDB returns a DB on a fresh, empty item table.
func newDB(t *testing.T) (*jimuxorm.DB, *xormio.Engine) {
	t.Helper()
	dsn, err := startMySQL()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := xormio.NewEngine("mysql", dsn)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if err := pingUntilReady(engine); err != nil {
		t.Fatalf("ping mysql: %v", err)
	}
	if err := engine.DropTables(new(itemDO)); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if err := engine.Sync(new(itemDO)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	db, err := jimuxorm.New(engine)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return db, engine
}

func pingUntilReady(engine *xormio.Engine) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := engine.Ping()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func insert(ctx context.Context, db *jimuxorm.DB, id string, value int) error {
	session, err := db.Session(ctx)
	if err != nil {
		return err
	}
	_, err = session.Insert(&itemDO{ID: id, Value: value})
	return err
}

func update(ctx context.Context, db *jimuxorm.DB, id string, value int) error {
	session, err := db.Session(ctx)
	if err != nil {
		return err
	}
	_, err = session.ID(id).Cols("value").Update(&itemDO{Value: value})
	return err
}

// count reads committed rows on the pool, outside any transaction.
func count(t *testing.T, engine *xormio.Engine, id string) int64 {
	t.Helper()
	n, err := engine.Where("id = ?", id).Count(new(itemDO))
	if err != nil {
		t.Fatalf("count %s: %v", id, err)
	}
	return n
}

func TestIntegrationCommitAndRollback(t *testing.T) {
	db, engine := newDB(t)
	ctx := context.Background()

	if err := db.Manager().Within(ctx, func(ctx context.Context) error {
		if err := insert(ctx, db, "a", 1); err != nil {
			return err
		}
		if n := count(t, engine, "a"); n != 0 {
			t.Errorf("uncommitted row visible on the pool: count = %d", n)
		}
		return insert(ctx, db, "b", 2)
	}); err != nil {
		t.Fatalf("Within() error = %v", err)
	}
	if count(t, engine, "a") != 1 || count(t, engine, "b") != 1 {
		t.Fatal("committed rows are not visible")
	}

	want := errors.New("use case failed")
	err := db.Manager().Within(ctx, func(ctx context.Context) error {
		if err := insert(ctx, db, "c", 3); err != nil {
			return err
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("Within() error = %v, want %v", err, want)
	}
	if n := count(t, engine, "c"); n != 0 {
		t.Fatalf("rolled-back row visible: count = %d", n)
	}
}

func TestIntegrationSessionOutsideTransactionAutocommits(t *testing.T) {
	db, engine := newDB(t)
	if err := insert(context.Background(), db, "a", 1); err != nil {
		t.Fatalf("insert outside a transaction: %v", err)
	}
	if n := count(t, engine, "a"); n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}
}

func TestIntegrationSessionFailsClosed(t *testing.T) {
	db, engine := newDB(t)
	other, err := jimuxorm.New(engine)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var kept context.Context
	if err := db.Manager().Within(context.Background(), func(ctx context.Context) error {
		kept = ctx
		if err := insert(ctx, other, "foreign", 1); !errors.Is(err, transaction.ErrForeignTransaction) {
			t.Errorf("Session() of another DB error = %v, want %v", err, transaction.ErrForeignTransaction)
		}
		return nil
	}); err != nil {
		t.Fatalf("Within() error = %v", err)
	}
	if err := insert(kept, db, "kept", 1); !errors.Is(err, transaction.ErrTransactionDone) {
		t.Fatalf("Session() on a kept ctx error = %v, want %v", err, transaction.ErrTransactionDone)
	}
	if count(t, engine, "foreign")+count(t, engine, "kept") != 0 {
		t.Fatal("a rejected write reached the database")
	}
}

func TestIntegrationWithinOrJoin(t *testing.T) {
	db, engine := newDB(t)
	saveTwo := func(ctx context.Context, prefix string) error {
		return db.Manager().WithinOrJoin(ctx, func(ctx context.Context) error {
			if err := insert(ctx, db, prefix+"-1", 1); err != nil {
				return err
			}
			return insert(ctx, db, prefix+"-1", 2) // duplicate key
		})
	}

	if err := saveTwo(context.Background(), "own"); err == nil {
		t.Fatal("WithinOrJoin() error = nil, want a duplicate key error")
	}
	if n := count(t, engine, "own-1"); n != 0 {
		t.Fatalf("own transaction left a partial write: count = %d", n)
	}

	_ = db.Manager().Within(context.Background(), func(ctx context.Context) error {
		if err := insert(ctx, db, "outer", 1); err != nil {
			return err
		}
		return saveTwo(ctx, "joined")
	})
	if count(t, engine, "outer")+count(t, engine, "joined-1") != 0 {
		t.Fatal("joined failure did not roll back the caller's transaction")
	}
}

func TestIntegrationDeadlockIsConflict(t *testing.T) {
	db, _ := newDB(t)
	for _, id := range []string{"x", "y"} {
		if err := insert(context.Background(), db, id, 0); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	// Each transaction locks its first row, waits until both hold one, then
	// locks the other's row: InnoDB picks one as the deadlock victim.
	var holding sync.WaitGroup
	holding.Add(2)
	run := func(first, second string) error {
		return db.Manager().Within(context.Background(), func(ctx context.Context) error {
			if err := update(ctx, db, first, 1); err != nil {
				holding.Done()
				return err
			}
			holding.Done()
			holding.Wait()
			return update(ctx, db, second, 1)
		})
	}
	errs := make(chan error, 2)
	go func() { errs <- run("x", "y") }()
	go func() { errs <- run("y", "x") }()

	var conflicts, successes int
	for range 2 {
		err := <-errs
		switch {
		case err == nil:
			successes++
		case errors.Is(err, transaction.ErrConflict):
			conflicts++
		default:
			t.Errorf("Within() error = %v, want nil or ErrConflict", err)
		}
	}
	if conflicts != 1 || successes != 1 {
		t.Fatalf("conflicts = %d, successes = %d; want one of each", conflicts, successes)
	}
}

func TestIntegrationLockWaitTimeoutIsConflictAndRollsBack(t *testing.T) {
	db, engine := newDB(t)
	if err := insert(context.Background(), db, "locked", 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	locked := make(chan struct{})
	release := make(chan struct{})
	holder := make(chan error, 1)
	go func() {
		holder <- db.Manager().Within(context.Background(), func(ctx context.Context) error {
			if err := update(ctx, db, "locked", 1); err != nil {
				close(locked)
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked

	err := db.Manager().Within(context.Background(), func(ctx context.Context) error {
		session, err := db.Session(ctx)
		if err != nil {
			return err
		}
		if _, err := session.Exec("SET SESSION innodb_lock_wait_timeout = 1"); err != nil {
			return err
		}
		if err := insert(ctx, db, "waiter", 1); err != nil {
			return err
		}
		return update(ctx, db, "locked", 2) // waits for the holder, then 1205
	})
	close(release)
	if holdErr := <-holder; holdErr != nil {
		t.Fatalf("holder Within() error = %v", holdErr)
	}
	if !errors.Is(err, transaction.ErrConflict) {
		t.Fatalf("Within() error = %v, want %v", err, transaction.ErrConflict)
	}
	// MySQL rolls back only the statement on 1205; the Manager rolls back
	// the rest.
	if n := count(t, engine, "waiter"); n != 0 {
		t.Fatalf("write before the timeout survived: count = %d", n)
	}
}

func TestIntegrationKilledConnectionIsOutcomeUnknown(t *testing.T) {
	db, engine := newDB(t)
	afterCommit := false
	err := db.Manager().Within(context.Background(), func(ctx context.Context) error {
		if err := insert(ctx, db, "killed", 1); err != nil {
			return err
		}
		session, err := db.Session(ctx)
		if err != nil {
			return err
		}
		var connectionID int64
		if _, err := session.SQL("SELECT CONNECTION_ID()").Get(&connectionID); err != nil {
			return err
		}
		if _, err := engine.Exec(fmt.Sprintf("KILL %d", connectionID)); err != nil {
			return err
		}
		return transaction.AfterCommit(ctx, func(context.Context) { afterCommit = true })
	})
	if !errors.Is(err, transaction.ErrOutcomeUnknown) {
		t.Fatalf("Within() error = %v, want %v", err, transaction.ErrOutcomeUnknown)
	}
	if afterCommit {
		t.Fatal("AfterCommit hook ran after an unknown outcome")
	}
	// The server rolled back when it killed the connection; a caller could
	// only learn that by reading back.
	if n := count(t, engine, "killed"); n != 0 {
		t.Fatalf("count = %d, want 0", n)
	}
}
