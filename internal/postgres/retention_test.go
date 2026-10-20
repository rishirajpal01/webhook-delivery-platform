package postgres

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestDeleteOldMessages(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	mustEndpoint(t, s, app.ID)

	finished := mustSend(t, s, app.ID, "a")   // old, delivered: deleted
	unfinished := mustSend(t, s, app.ID, "a") // old, still pending: kept
	recent := mustSend(t, s, app.ID, "a")     // finished but new: kept
	if _, err := s.db.ExecContext(ctx, `
		UPDATE deliveries SET state = 'succeeded' WHERE message_id IN ($1, $2)`,
		finished.Message.ID, recent.Message.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE messages SET created_at = now() - interval '31 days' WHERE id IN ($1, $2)`,
		finished.Message.ID, unfinished.Message.ID); err != nil {
		t.Fatal(err)
	}

	n, err := s.DeleteOldMessages(ctx, 30, 1)
	if err != nil || n != 1 {
		t.Fatalf("deleted %d, err %v; want 1", n, err)
	}
	ms, err := s.ListMessages(ctx, app.ID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range ms {
		ids = append(ids, m.ID)
	}
	if len(ids) != 2 || slices.Contains(ids, finished.Message.ID) {
		t.Fatalf("messages left = %v; want all but %s", ids, finished.Message.ID)
	}
	var deliveries int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries WHERE message_id = $1`,
		finished.Message.ID).Scan(&deliveries); err != nil || deliveries != 0 {
		t.Fatalf("%d deliveries of the deleted message left, err %v", deliveries, err)
	}
}

func TestListMessagesNewestFirst(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	var want []string
	for range 5 {
		want = append(want, mustSend(t, s, app.ID, "a").Message.ID)
	}
	slices.Reverse(want)

	var got []string
	cursor := ""
	for {
		page, err := s.ListMessages(ctx, app.ID, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range page {
			got = append(got, m.ID)
		}
		if len(page) < 2 {
			break
		}
		cursor = page[len(page)-1].ID
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paged messages = %v; want %v", got, want)
	}
	if _, err := s.ListMessages(ctx, "app_missing", "", 2); err != ErrNotFound {
		t.Fatalf("missing app: err %v, want ErrNotFound", err)
	}
}

func TestListenDueWakesOnSend(t *testing.T) {
	s := testStore(t)
	app := mustApp(t, s)
	mustEndpoint(t, s, app.ID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	woken := make(chan struct{}, 10)
	done := make(chan error, 1)
	go func() { done <- s.ListenDue(ctx, func() { woken <- struct{}{} }) }()

	// LISTEN may not be in place yet, so send until a wake arrives.
	deadline := time.After(5 * time.Second)
	for {
		mustSend(t, s, app.ID, "a")
		select {
		case <-woken:
			cancel()
			<-done
			return
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("no notification after sending")
		}
	}
}
