package postgres

import (
	"context"
	"testing"
	"time"
)

func partitionCount(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM pg_inherits WHERE inhparent = 'attempts'::regclass`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAttemptPartitionsAndRetention(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if n := partitionCount(t, s); n != 8 {
		t.Fatalf("after migrating: %d partitions, want today plus 7 days", n)
	}
	if created, dropped, err := s.MaintainAttemptPartitions(ctx, 30); err != nil || created != 0 || dropped != 0 {
		t.Fatalf("maintenance with nothing to do = %d created, %d dropped, %v", created, dropped, err)
	}

	// An attempt from 40 days ago, in its own partition.
	app := mustApp(t, s)
	mustEndpoint(t, s, app.ID)
	mustSend(t, s, app.ID, "a")
	jobs, _ := s.Claim(ctx, 1, time.Minute)
	exec(t, s, `SELECT create_attempt_partitions(current_date - 40, current_date - 40)`)
	if _, err := s.Complete(ctx, jobs[0], Outcome{
		Attempt:   Attempt{AttemptedAt: time.Now().AddDate(0, 0, -40), LatencyMS: 1},
		NextState: DeliverySucceeded,
	}, testPolicy); err != nil {
		t.Fatal(err)
	}
	as, _ := s.ListAttempts(ctx, app.ID, jobs[0].DeliveryID)
	if len(as) != 1 {
		t.Fatalf("attempts = %d, want 1", len(as))
	}

	if _, dropped, err := s.MaintainAttemptPartitions(ctx, 0); err != nil || dropped != 0 {
		t.Fatalf("retention 0 dropped %d, %v; want to keep everything", dropped, err)
	}
	if _, dropped, err := s.MaintainAttemptPartitions(ctx, 30); err != nil || dropped != 1 {
		t.Fatalf("dropped %d, %v; want the 40-day-old partition", dropped, err)
	}
	if as, _ := s.ListAttempts(ctx, app.ID, jobs[0].DeliveryID); len(as) != 0 {
		t.Fatalf("attempts after retention = %d, want 0", len(as))
	}
}

func TestMigrationKeepsExistingAttempts(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	// Apply only the migrations before partitioning, as an existing install would have.
	for _, v := range []string{"0001_core_schema", "0002_endpoint_health", "0003_endpoint_limits"} {
		body, err := migrationFiles.ReadFile("migrations/" + v + ".sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(body)); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		`CREATE TABLE schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`,
		`INSERT INTO schema_migrations (version) VALUES ('0001_core_schema'), ('0002_endpoint_health'), ('0003_endpoint_limits')`,
		`INSERT INTO applications (id, name) VALUES ('app_1', 'a')`,
		`INSERT INTO endpoints (id, app_id, url, secret) VALUES ('ep_1', 'app_1', 'https://x.io', 's')`,
		`INSERT INTO messages (id, app_id, event_type, payload) VALUES ('msg_1', 'app_1', 'a', '{}')`,
		`INSERT INTO deliveries (id, message_id, endpoint_id) VALUES ('del_1', 'msg_1', 'ep_1')`,
		`INSERT INTO attempts (delivery_id, attempted_at, latency_ms) VALUES
			('del_1', now() - interval '3 days', 1), ('del_1', now(), 2)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	if _, err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	s := NewStore(db)
	as, err := s.ListAttempts(ctx, "app_1", "del_1")
	if err != nil || len(as) != 2 || as[0].LatencyMS != 1 || as[1].LatencyMS != 2 {
		t.Fatalf("attempts after migrating = %+v, %v", as, err)
	}
	// New ids continue after the copied ones.
	exec(t, s, `INSERT INTO attempts (delivery_id, latency_ms) VALUES ('del_1', 3)`)
	var maxID int64
	db.QueryRow(`SELECT max(id) FROM attempts`).Scan(&maxID)
	if maxID != 3 {
		t.Fatalf("next attempt id = %d, want 3", maxID)
	}
}
