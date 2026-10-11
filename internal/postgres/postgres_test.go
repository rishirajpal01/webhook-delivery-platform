package postgres

import (
	"context"
	"database/sql"
	"io/fs"
	"sync"
	"testing"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/testdb"
)

// testDB returns a fresh, unmigrated database.
func testDB(t *testing.T) *sql.DB {
	db, _ := testdb.New(t)
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
