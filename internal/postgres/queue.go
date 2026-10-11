package postgres

import (
	"context"
	"time"
)

// Job is a claimed delivery with everything needed to send it.
type Job struct {
	DeliveryID   string
	MessageID    string
	EndpointID   string
	AttemptCount int       // attempts made before this one
	DueAt        time.Time // when the delivery became due; lag = claim time - DueAt
	EventType    string
	Payload      []byte
	URL          string
	Secret       string
	// LockedUntil is the lease this claim holds. It doubles as a fencing
	// token: Complete only applies if the row still carries this exact lease.
	LockedUntil time.Time
}

// Claim leases up to limit due deliveries for lease. Rows locked by another
// dispatcher are skipped rather than waited on, so any number of dispatchers
// can claim side by side. Deliveries to paused or disabled endpoints stay
// pending.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH claimed AS (
			UPDATE deliveries
			SET state = 'in_flight', locked_until = now() + $2::bigint * interval '1 millisecond', updated_at = now()
			WHERE id = ANY (ARRAY(
				SELECT d.id FROM deliveries d
				WHERE d.state = 'pending' AND d.next_attempt_at <= now()
				  AND d.endpoint_id IN (SELECT id FROM endpoints WHERE status = 'active')
				ORDER BY d.next_attempt_at
				LIMIT $1
				FOR UPDATE SKIP LOCKED))
			RETURNING id, message_id, endpoint_id, attempt_count, next_attempt_at, locked_until
		)
		SELECT c.id, c.message_id, c.endpoint_id, c.attempt_count, c.next_attempt_at, c.locked_until,
		       m.event_type, m.payload::text, e.url, e.secret
		FROM claimed c
		JOIN messages m ON m.id = c.message_id
		JOIN endpoints e ON e.id = c.endpoint_id`,
		limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var (
			j       Job
			payload string
		)
		if err := rows.Scan(&j.DeliveryID, &j.MessageID, &j.EndpointID, &j.AttemptCount, &j.DueAt,
			&j.LockedUntil, &j.EventType, &payload, &j.URL, &j.Secret); err != nil {
			return nil, err
		}
		j.Payload = []byte(payload)
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Outcome is the result of one attempt and what to do next.
type Outcome struct {
	Attempt Attempt
	// NextState is DeliverySucceeded, DeliveryPending (retry) or DeliveryDead.
	NextState string
	// RetryAt is when to try again; used only when NextState is pending.
	RetryAt time.Time
}

// Complete records an attempt and moves the delivery to its next state, in
// one transaction. The attempt is always recorded, because the HTTP call
// happened. The state change applies only if the row still holds j's lease:
// if the lease expired and another dispatcher took the row over, that
// dispatcher now owns its state. It reports whether the state changed.
func (s *Store) Complete(ctx context.Context, j Job, o Outcome) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	a := o.Attempt
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO attempts (delivery_id, attempted_at, status_code, latency_ms, error, response_body)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''))`,
		j.DeliveryID, a.AttemptedAt, a.StatusCode, a.LatencyMS, a.Error, a.ResponseBody); err != nil {
		return false, err
	}

	res, err := tx.ExecContext(ctx, `
		UPDATE deliveries
		SET state = $3,
		    attempt_count = attempt_count + 1,
		    next_attempt_at = CASE WHEN $3 = 'pending' THEN $4 ELSE next_attempt_at END,
		    locked_until = NULL,
		    updated_at = now()
		WHERE id = $1 AND state = 'in_flight' AND locked_until = $2`,
		j.DeliveryID, j.LockedUntil, o.NextState, o.RetryAt)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, tx.Commit()
}

// ReleaseExpired returns in_flight deliveries whose lease has passed to
// pending, so a dispatcher that died mid-send does not strand them. It
// returns how many rows it released.
func (s *Store) ReleaseExpired(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE deliveries SET state = 'pending', locked_until = NULL, updated_at = now()
		WHERE state = 'in_flight' AND locked_until < now()`)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}
