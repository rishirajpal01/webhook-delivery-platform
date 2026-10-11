// Package testdb gives tests a throwaway Postgres database.
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// New creates an empty database for one test and drops it afterwards. It
// skips the test when DATABASE_URL is unset, so `go test ./...` still works
// without Postgres. It returns the database and its connection string.
func New(t *testing.T) (*sql.DB, string) {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres test")
	}
	ctx := context.Background()

	admin, err := sql.Open("pgx", base)
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
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return db, u.String()
}
