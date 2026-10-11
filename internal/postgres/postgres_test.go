package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"io/fs"
	"net/url"
	"os"
	"sync"
	"testing"
)

// testDB creates a throwaway database for one test and drops it afterwards.
// It skips the test when DATABASE_URL is unset, so `go test ./...` still
// works without Postgres.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres test")
	}
	ctx := context.Background()

	admin, err := Open(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })

	b := make([]byte, 6)
	rand.Read(b)
	name := "test_" + hex.EncodeToString(b)
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}

	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	db, err := Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return db
}

func migrationCount(t *testing.T) int {
	t.Helper()
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	return len(names)
}

func TestMigrateAppliesOnceAndIsIdempotent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	applied, err := Migrate(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != migrationCount(t) {
		t.Fatalf("first run applied %v, want all %d migrations", applied, migrationCount(t))
	}

	again, err := Migrate(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("second run applied %v, want nothing", again)
	}

	for _, table := range []string{"applications", "endpoints", "messages", "deliveries", "attempts"} {
		var exists bool
		err := db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists)
		if err != nil || !exists {
			t.Errorf("table %s missing (err %v)", table, err)
		}
	}
}

func TestMigrateConcurrentCallersApplyEachMigrationOnce(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	const callers = 4
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		total int
	)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			applied, err := Migrate(ctx, db)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			total += len(applied)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if total != migrationCount(t) {
		t.Fatalf("callers applied %d migrations in total, want %d", total, migrationCount(t))
	}
}
