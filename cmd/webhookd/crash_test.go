package main

import (
	"context"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/signing"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/testdb"
)

// TestCrashLosesNothing is the phase 1 exit gate: queue many messages, kill
// the dispatcher with SIGKILL at random moments several times, then let a
// final dispatcher finish. Every delivery must succeed and the receiver must
// have seen every message id at least once. Duplicates are allowed: that is
// what at-least-once means.
//
// CRASH_TEST_MESSAGES (default 2000) and CRASH_TEST_KILLS (default 5) scale
// it; the design doc's full run is 100000 messages.
func TestCrashLosesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("crash test skipped in -short mode")
	}
	db, dbURL := testdb.New(t)
	ctx := context.Background()
	if _, err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	messages := envInt(t, "CRASH_TEST_MESSAGES", 2000)
	kills := envInt(t, "CRASH_TEST_KILLS", 5)

	// The receiver counts each webhook-id and takes a few ms per request, so
	// kills land while sends are in flight.
	var (
		mu   sync.Mutex
		seen = map[string]int{}
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Duration(rand.IntN(5)) * time.Millisecond)
		mu.Lock()
		seen[r.Header.Get(signing.HeaderID)]++
		mu.Unlock()
	}))
	defer receiver.Close()

	store := postgres.NewStore(db)
	app, err := store.CreateApp(ctx, "crash")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateEndpoint(ctx, postgres.Endpoint{
		AppID: app.ID, URL: receiver.URL, Secret: signing.NewSecret(),
		MaxInFlight: 32, // as many as the dispatcher sends at once
	}); err != nil {
		t.Fatal(err)
	}
	// Bulk insert: this test is about the dispatcher, not the send path.
	if _, err := db.ExecContext(ctx, `
		WITH m AS (
			INSERT INTO messages (id, app_id, event_type, payload)
			SELECT 'msg_' || replace(gen_random_uuid()::text, '-', ''), $1, 'crash.test', jsonb_build_object('n', n)
			FROM generate_series(1, $2) AS n
			RETURNING id)
		INSERT INTO deliveries (id, message_id, endpoint_id)
		SELECT 'del_' || replace(gen_random_uuid()::text, '-', ''), m.id, e.id
		FROM m CROSS JOIN endpoints e`, app.ID, messages); err != nil {
		t.Fatal(err)
	}

	bin := filepath.Join(t.TempDir(), "webhookd")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	start := func() *exec.Cmd {
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(),
			"DATABASE_URL="+dbURL,
			"ROLE=dispatcher",
			"LISTEN_ADDR=127.0.0.1:0",
			"ALLOW_PRIVATE_NETWORKS=true",
			"DISPATCHER_CONCURRENCY=32",
			"DISPATCHER_LEASE=2s",
		)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd
	}

	remaining := func() int {
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM deliveries WHERE state <> 'succeeded'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	for i := range kills {
		cmd := start()
		time.Sleep(time.Duration(200+rand.IntN(600)) * time.Millisecond)
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		t.Logf("kill %d: %d deliveries not yet succeeded", i+1, remaining())
	}

	final := start()
	defer func() {
		final.Process.Signal(syscall.SIGTERM)
		final.Wait()
	}()
	deadline := time.Now().Add(time.Minute + time.Duration(messages)*5*time.Millisecond)
	for remaining() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d deliveries never succeeded", remaining())
		}
		time.Sleep(200 * time.Millisecond)
	}

	var ids []string
	rows, err := db.QueryContext(ctx, `SELECT id FROM messages`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()

	mu.Lock()
	defer mu.Unlock()
	missing, dupes := 0, 0
	for _, id := range ids {
		switch n := seen[id]; {
		case n == 0:
			missing++
		case n > 1:
			dupes += n - 1
		}
	}
	if len(ids) != messages || missing > 0 {
		t.Fatalf("%d of %d messages never reached the receiver", missing, len(ids))
	}
	t.Logf("%d messages, %d kills: none lost, %d duplicate deliveries", messages, kills, dupes)
}

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return n
}
