package xorm_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	jimuxorm "github.com/go-jimu/contrib/transaction/xorm"
	"github.com/go-sql-driver/mysql"
	xormio "xorm.io/xorm"
)

// newOfflineEngine returns an engine that never connects: database/sql opens
// connections lazily, and these tests run no statement.
func newOfflineEngine(t *testing.T) *xormio.Engine {
	t.Helper()
	engine, err := xormio.NewEngine("mysql", "root:secret@tcp(127.0.0.1:1)/jimu")
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func TestNewRejectsNilEngine(t *testing.T) {
	if _, err := jimuxorm.New(nil); !errors.Is(err, jimuxorm.ErrNilEngine) {
		t.Fatalf("New(nil) error = %v, want %v", err, jimuxorm.ErrNilEngine)
	}
}

func TestNewBuildsManager(t *testing.T) {
	db, err := jimuxorm.New(newOfflineEngine(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if db.Manager() == nil {
		t.Fatal("Manager() = nil")
	}
}

func TestSessionOutsideTransactionUsesPool(t *testing.T) {
	db, err := jimuxorm.New(newOfflineEngine(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	session, err := db.Session(context.Background())
	if err != nil || session == nil {
		t.Fatalf("Session() = %v, %v; want a pool session", session, err)
	}
}

func TestIsMySQLConflict(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"lock wait timeout", &mysql.MySQLError{Number: 1205}, true},
		{"deadlock", &mysql.MySQLError{Number: 1213}, true},
		{"wrapped deadlock", fmt.Errorf("save: %w", &mysql.MySQLError{Number: 1213}), true},
		{"duplicate key", &mysql.MySQLError{Number: 1062}, false},
		{"not a MySQL error", errors.New("connection reset"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := jimuxorm.IsMySQLConflict(tt.err); got != tt.want {
				t.Fatalf("IsMySQLConflict(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
