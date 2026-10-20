// Package postgres opens the database and applies the embedded schema
// migrations.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrateLockID is the pg_advisory_lock key that serialises migrations when
// several processes start at once. Any constant works; it only has to be the
// same in every process.
const migrateLockID = 7_202_610_11

// Open connects to Postgres and checks the connection works. maxConns caps
// the process's connections; keep it above the dispatcher concurrency so API
// requests still get connections while every dispatcher slot is busy.
func Open(ctx context.Context, url string, maxConns int) (*sql.DB, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	// database/sql keeps only 2 idle connections by default, so a busy
	// dispatcher would open and close a Postgres connection (a new backend
	// process) for most queries. Keep a steady pool instead.
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return db, nil
}

// Migrate applies every embedded migration that has not been applied yet, in
// file-name order, each in its own transaction. It returns the versions it
// applied. Safe to call from several processes at once.
func Migrate(ctx context.Context, db *sql.DB) ([]string, error) {
	// An advisory lock is held by a session, so pin one connection for the
	// whole run.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrateLockID); err != nil {
		return nil, fmt.Errorf("take migration lock: %w", err)
	}
	// Unlock with a fresh context so a cancelled ctx still releases the lock.
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrateLockID)

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}

	done, err := appliedVersions(ctx, conn)
	if err != nil {
		return nil, err
	}

	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	var applied []string
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		if done[version] {
			continue
		}
		body, err := migrationFiles.ReadFile(name)
		if err != nil {
			return applied, err
		}
		if err := applyOne(ctx, conn, version, string(body)); err != nil {
			return applied, fmt.Errorf("migration %s: %w", version, err)
		}
		applied = append(applied, version)
	}
	return applied, nil
}

func appliedVersions(ctx context.Context, conn *sql.Conn) (map[string]bool, error) {
	rows, err := conn.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	done := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		done[v] = true
	}
	return done, rows.Err()
}

func applyOne(ctx context.Context, conn *sql.Conn, version, body string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op after Commit

	if _, err := tx.ExecContext(ctx, body); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
		return err
	}
	return tx.Commit()
}
