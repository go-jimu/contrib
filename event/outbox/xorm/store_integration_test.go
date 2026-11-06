//go:build integration

package xorm_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-jimu/components/ddd/event"
	"github.com/go-jimu/components/ddd/event/outbox"
	"github.com/go-jimu/components/ddd/event/outbox/outboxtest"
	"github.com/go-jimu/components/transaction"
	outboxxorm "github.com/go-jimu/contrib/event/outbox/xorm"
	jimuxorm "github.com/go-jimu/contrib/transaction/xorm"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	xormio "xorm.io/xorm"
)

const integrationMySQLImage = "mysql:8.4"

var integrationDSN string

// TestMain starts one MySQL server for every test and terminates it after
// the run.
func TestMain(m *testing.M) {
	container, dsn, err := startMySQL()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	integrationDSN = dsn
	code := m.Run()
	terminate(container)
	os.Exit(code)
}

func terminate(container testcontainers.Container) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := container.Terminate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "terminate mysql container:", err)
	}
}

func startMySQL() (testcontainers.Container, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        integrationMySQLImage,
			ExposedPorts: []string{"3306/tcp"},
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "secret", "MYSQL_DATABASE": "jimu"},
			WaitingFor:   wait.ForListeningPort("3306/tcp").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("start mysql container: %w", err)
	}
	endpoint, err := container.PortEndpoint(ctx, "3306/tcp", "")
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, "", fmt.Errorf("mysql endpoint: %w", err)
	}
	// loc=Asia/Shanghai checks that stored times do not depend on the DSN.
	return container, fmt.Sprintf("root:secret@tcp(%s)/jimu?parseTime=true&loc=Asia%%2FShanghai", endpoint), nil
}

// newStore returns a Store on a fresh, empty table, and its DB.
func newStore(t *testing.T) (*outboxxorm.Store, *jimuxorm.DB, *xormio.Engine) {
	t.Helper()
	engine, err := xormio.NewEngine("mysql", integrationDSN)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if err := pingUntilReady(engine); err != nil {
		t.Fatalf("ping mysql: %v", err)
	}
	db, err := jimuxorm.New(engine)
	if err != nil {
		t.Fatal(err)
	}
	store, err := outboxxorm.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Exec("DROP TABLE IF EXISTS " + outboxxorm.DefaultTable); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTable(context.Background()); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	return store, db, engine
}

// pingUntilReady waits out the entrypoint's temporary server, which accepts
// connections and then restarts.
func pingUntilReady(engine *xormio.Engine) error {
	deadline := time.Now().Add(time.Minute)
	for {
		err := engine.Ping()
		if err == nil {
			if _, err = engine.Exec("SELECT 1"); err == nil {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Second)
	}
}

func TestIntegrationStoreConformance(t *testing.T) {
	outboxtest.TestStore(t, func(t *testing.T) outboxtest.Harness {
		store, db, engine := newStore(t)
		// A second DB on the same engine has its own Manager, so its
		// transactions are foreign to store.
		other, err := jimuxorm.New(engine)
		if err != nil {
			t.Fatal(err)
		}
		return outboxtest.Harness{Store: store, Transactor: db.Manager(), Foreign: other.Manager()}
	})
}

type OrderPaid struct {
	OrderID string `json:"order_id"`
}

func (OrderPaid) Kind() event.Kind { return "order.paid" }

type projector struct {
	mu   sync.Mutex
	runs map[string]int
}

func (*projector) Name() string            { return "projector" }
func (*projector) Listening() []event.Kind { return []event.Kind{OrderPaid{}.Kind()} }

func (p *projector) Handle(_ context.Context, e event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runs[e.(OrderPaid).OrderID]++
	return nil
}

func (p *projector) count() (orders, runs int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, n := range p.runs {
		orders++
		runs += n
	}
	return orders, runs
}

// TestIntegrationRelaysShareRecords runs two Outboxes, as two processes,
// over one table: every committed order runs once, and a rolled back one
// never runs.
func TestIntegrationRelaysShareRecords(t *testing.T) {
	store, db, engine := newStore(t)
	if _, err := engine.Exec("CREATE TABLE IF NOT EXISTS orders (id VARCHAR(36) PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Exec("DELETE FROM orders"); err != nil {
		t.Fatal(err)
	}
	handler := &projector{runs: map[string]int{}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var outboxes []*outbox.Outbox
	for i := range 2 {
		registry := outbox.NewRegistry()
		if err := outbox.Register[OrderPaid](registry); err != nil {
			t.Fatal(err)
		}
		ob, err := outbox.New(store, registry, outbox.WithLogger(logger),
			outbox.WithOwner(fmt.Sprintf("relay-%d", i)), outbox.WithPollInterval(10*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if err := ob.Subscribe(handler); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- ob.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("Run() error = %v", err)
			}
		})
		outboxes = append(outboxes, ob)
	}

	const orders = 30
	placeOrder := func(i int, fail bool) error {
		return db.Manager().Within(context.Background(), func(ctx context.Context) error {
			session, err := db.Session(ctx)
			if err != nil {
				return err
			}
			id := fmt.Sprintf("o-%d", i)
			if _, err := session.Exec("INSERT INTO orders (id) VALUES (?)", id); err != nil {
				return err
			}
			if err := outboxes[i%2].Dispatch(ctx, OrderPaid{OrderID: id}); err != nil {
				return err
			}
			if fail {
				return errors.New("payment declined")
			}
			return nil
		})
	}
	var wg sync.WaitGroup
	for i := range orders {
		wg.Go(func() {
			if err := placeOrder(i, false); err != nil {
				t.Errorf("order %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	if err := placeOrder(orders, true); err == nil {
		t.Fatal("declined order committed")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		counts, err := store.Counts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(counts) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("records still open: %+v", counts)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n, runs := handler.count(); n != orders || runs != orders {
		t.Fatalf("handled %d orders in %d runs, want %d orders run once each", n, runs, orders)
	}
}

func TestIntegrationInsertNeedsTransaction(t *testing.T) {
	store, _, _ := newStore(t)
	err := store.Insert(context.Background(), []outbox.Record{{ID: "r-1", Handler: "a"}})
	if err == nil {
		t.Fatal("Insert outside a transaction succeeded")
	}
}

func TestNewRejectsInvalidTable(t *testing.T) {
	_, db, _ := newStore(t)
	if _, err := outboxxorm.New(db, outboxxorm.WithTable("outbox; DROP TABLE x")); !errors.Is(err, outboxxorm.ErrInvalidTable) {
		t.Fatalf("New() error = %v, want ErrInvalidTable", err)
	}
	if _, err := outboxxorm.New(nil); !errors.Is(err, outboxxorm.ErrNilDB) {
		t.Fatalf("New(nil) error = %v, want ErrNilDB", err)
	}
}

// projectOnce writes each order into the projection table and calls
// MarkDone in one transaction of db, on the ctx Handle received; the first
// transaction of every order rolls back at commit time.
type projectOnce struct {
	db       *jimuxorm.DB
	mu       sync.Mutex
	attempts map[string]int
}

func (*projectOnce) Name() string            { return "projection" }
func (*projectOnce) Listening() []event.Kind { return []event.Kind{OrderPaid{}.Kind()} }

func (p *projectOnce) Handle(ctx context.Context, e event.Event) error {
	id := e.(OrderPaid).OrderID
	p.mu.Lock()
	p.attempts[id]++
	first := p.attempts[id] == 1
	p.mu.Unlock()
	return p.db.Manager().Within(ctx, func(ctx context.Context) error {
		session, err := p.db.Session(ctx)
		if err != nil {
			return err
		}
		if _, err := session.Exec("INSERT INTO projection (order_id) VALUES (?)", id); err != nil {
			return err
		}
		if err := outbox.MarkDone(ctx); err != nil {
			return err
		}
		if !first {
			return nil
		}
		// Fail at commit time, after MarkDone advanced the progress in this
		// transaction: the rollback must undo both.
		return transaction.BeforeCommit(ctx, func(context.Context) error {
			return errors.New("deadlock at commit, retry")
		})
	})
}

// TestIntegrationHandlerWritesOnce runs a handler whose writes go to the
// Store's database: a rolled back attempt leaves neither a row nor progress,
// and every order ends with exactly one row.
func TestIntegrationHandlerWritesOnce(t *testing.T) {
	store, db, engine := newStore(t)
	for _, q := range []string{
		"DROP TABLE IF EXISTS projection",
		"CREATE TABLE projection (order_id VARCHAR(36) NOT NULL)", // no key: a duplicate write would show
	} {
		if _, err := engine.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	registry := outbox.NewRegistry()
	if err := outbox.Register[OrderPaid](registry); err != nil {
		t.Fatal(err)
	}
	ob, err := outbox.New(store, registry, outbox.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		outbox.WithPollInterval(10*time.Millisecond), outbox.WithBackoff(time.Millisecond, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	handler := &projectOnce{db: db, attempts: map[string]int{}}
	if err := ob.Subscribe(handler); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ob.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() error = %v", err)
		}
	})

	// Two events in one record: the second's progress commits with its row.
	err = db.Manager().Within(context.Background(), func(ctx context.Context) error {
		return ob.DispatchAll(ctx, []event.Event{OrderPaid{OrderID: "o-1"}, OrderPaid{OrderID: "o-2"}})
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		counts, err := store.Counts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(counts) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("records still open: %+v", counts)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var rows []struct {
		OrderID string `xorm:"'order_id'"`
		N       int    `xorm:"'n'"`
	}
	if err := engine.SQL("SELECT order_id, COUNT(*) AS n FROM projection GROUP BY order_id ORDER BY order_id").Find(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].N != 1 || rows[1].N != 1 {
		t.Fatalf("projection rows %+v, want o-1 and o-2 once each", rows)
	}
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.attempts["o-1"] != 2 || handler.attempts["o-2"] != 2 {
		t.Fatalf("attempts %v, want two per order: one rolled back, one committed", handler.attempts)
	}
}
