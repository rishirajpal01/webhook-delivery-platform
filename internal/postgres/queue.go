package postgres

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/id"
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
	CreatedAt    time.Time // when the message was created
	URL          string
	Secret       string
	// PreviousSecret is set while a rotated-out secret is still valid; the
	// request is then signed with both.
	PreviousSecret string
	// Probe is true when this delivery tests an endpoint whose circuit
	// breaker is open.
	Probe bool
	// LockedUntil is the lease this claim holds. It doubles as a fencing
	// token: Complete only applies if the row still carries this exact lease.
	LockedUntil time.Time
}

// Claim leases up to limit due deliveries for lease, sharing them fairly
// between endpoints. Endpoints are served round-robin, least recently
// served first, and each gets no more than its free concurrency slots and
// rate-limit tokens, so a backlog on one endpoint never holds up another.
//
// Each picked endpoint's row is locked for the claim and claimers skip
// locked rows, so any number of dispatchers can claim side by side and the
// limits still hold across all of them. The locks are FOR NO KEY UPDATE, so
// they do not block foreign-key checks from sends inserting deliveries. Deliveries to paused or disabled
// endpoints, and to endpoints whose circuit breaker is open, stay pending.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration) ([]Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// A rate-limited endpoint has rate_limit tokens per second and holds at
	// most one second's worth; rate_tat tracks how many are spent.
	rows, err := tx.QueryContext(ctx, `
		SELECT e.id, least(e.max_in_flight - f.n, due.n, CASE WHEN e.rate_limit IS NULL THEN $1 ELSE
		           floor(extract(epoch FROM now() + interval '1 second' - greatest(e.rate_tat, now())) * e.rate_limit) END)::int
		FROM endpoints e
		CROSS JOIN LATERAL (SELECT count(*) AS n FROM deliveries d
		                    WHERE d.endpoint_id = e.id AND d.state = 'in_flight') f
		-- Due work, counted only up to limit so a large backlog stays cheap.
		CROSS JOIN LATERAL (SELECT count(*) AS n FROM (
		                        SELECT 1 FROM deliveries d
		                        WHERE d.endpoint_id = e.id AND d.state = 'pending' AND d.next_attempt_at <= now()
		                        LIMIT $1) x) due
		WHERE e.status = 'active' AND e.circuit_open_until IS NULL
		  AND e.max_in_flight > f.n AND due.n > 0
		  AND (e.rate_limit IS NULL OR e.rate_tat <= now() + interval '1 second' - interval '1 second' / e.rate_limit)
		ORDER BY e.last_claimed_at NULLS FIRST, e.id
		LIMIT $1
		FOR NO KEY UPDATE OF e SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	var (
		endpointIDs []string
		room        []int
	)
	for rows.Next() {
		var (
			id string
			n  int
		)
		if err := rows.Scan(&id, &n); err != nil {
			rows.Close()
			return nil, err
		}
		endpointIDs = append(endpointIDs, id)
		room = append(room, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(endpointIDs) == 0 {
		return nil, err
	}

	jobs, err := claim(ctx, tx, lease, `
		SELECT p.id FROM unnest($2::text[], $3::int[]) AS q(endpoint_id, n)
		CROSS JOIN LATERAL (
			SELECT d.id FROM deliveries d
			WHERE d.endpoint_id = q.endpoint_id AND d.state = 'pending' AND d.next_attempt_at <= now()
			ORDER BY d.next_attempt_at
			LIMIT q.n
			FOR NO KEY UPDATE SKIP LOCKED) p`, endpointIDs, shareOut(limit, room))
	if err != nil {
		return nil, err
	}

	taken := make([]int, len(endpointIDs))
	for _, j := range jobs {
		taken[slices.Index(endpointIDs, j.EndpointID)]++
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE endpoints e SET
			last_claimed_at = now(),
			rate_tat = CASE WHEN e.rate_limit IS NULL THEN e.rate_tat
			                ELSE greatest(e.rate_tat, now()) + c.n * interval '1 second' / e.rate_limit END
		FROM unnest($1::text[], $2::int[]) AS c(id, n)
		WHERE e.id = c.id`, endpointIDs, taken); err != nil {
		return nil, err
	}
	return jobs, tx.Commit()
}

// shareOut splits limit slots between endpoints one at a time, round-robin,
// giving each no more than its room (free slots, tokens and due work).
func shareOut(limit int, room []int) []int {
	share := make([]int, len(room))
	for limit > 0 {
		given := false
		for i := range room {
			if limit > 0 && share[i] < room[i] {
				share[i]++
				limit--
				given = true
			}
		}
		if !given {
			break
		}
	}
	return share
}

// ClaimProbes picks up to limit endpoints whose circuit breaker cool-down has
// passed and leases one due delivery for each, as a probe. Each picked
// endpoint's circuit stays open for another lease, so exactly one probe is in
// flight per endpoint; completing the probe closes the circuit or re-opens
// it for a longer cool-down.
func (s *Store) ClaimProbes(ctx context.Context, limit int, lease time.Duration) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		UPDATE endpoints SET circuit_open_until = now() + $2::bigint * interval '1 millisecond'
		WHERE id IN (
			SELECT e.id FROM endpoints e
			WHERE e.status = 'active' AND e.circuit_open_until <= now()
			  AND EXISTS (SELECT 1 FROM deliveries d
			              WHERE d.endpoint_id = e.id AND d.state = 'pending' AND d.next_attempt_at <= now())
			LIMIT $1
			FOR UPDATE SKIP LOCKED)
		RETURNING id`, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	var endpointIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		endpointIDs = append(endpointIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var jobs []Job
	for _, id := range endpointIDs {
		js, err := claim(ctx, s.db, lease, `
			SELECT d.id FROM deliveries d
			WHERE d.endpoint_id = $2 AND d.state = 'pending' AND d.next_attempt_at <= now()
			ORDER BY d.next_attempt_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED`, id)
		if err != nil {
			return jobs, err
		}
		for i := range js {
			js[i].Probe = true
		}
		jobs = append(jobs, js...)
	}
	return jobs, nil
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// claim leases the deliveries whose ids pick selects. pick must lock the
// rows it returns with FOR UPDATE SKIP LOCKED; its own parameters start at $2.
func claim(ctx context.Context, q querier, lease time.Duration, pick string, args ...any) ([]Job, error) {
	rows, err := q.QueryContext(ctx, `
		WITH claimed AS (
			UPDATE deliveries
			SET state = 'in_flight', locked_until = now() + $1::bigint * interval '1 millisecond', updated_at = now()
			WHERE id = ANY (ARRAY(`+pick+`))
			RETURNING id, message_id, endpoint_id, attempt_count, next_attempt_at, locked_until
		)
		SELECT c.id, c.message_id, c.endpoint_id, c.attempt_count, c.next_attempt_at, c.locked_until,
		       m.event_type, m.payload::text, m.created_at, e.url, e.secret,
		       CASE WHEN e.previous_secret_expires_at > now() THEN e.previous_secret ELSE '' END
		FROM claimed c
		JOIN messages m ON m.id = c.message_id
		JOIN endpoints e ON e.id = c.endpoint_id`,
		append([]any{lease.Milliseconds()}, args...)...)
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
			&j.LockedUntil, &j.EventType, &payload, &j.CreatedAt, &j.URL, &j.Secret, &j.PreviousSecret); err != nil {
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

// HealthPolicy decides when an endpoint's circuit breaker trips and when a
// failing endpoint is disabled.
type HealthPolicy struct {
	// FailureThreshold is how many failures in a row trip the breaker.
	FailureThreshold int
	// CooldownBase is the first cool-down after tripping; each failed probe
	// doubles it, up to CooldownMax.
	CooldownBase, CooldownMax time.Duration
	// DisableAfter is how long an endpoint may fail continuously before it
	// is disabled.
	DisableAfter time.Duration
}

// CompleteResult reports what Complete changed.
type CompleteResult struct {
	// Applied is false when the lease was lost: the attempt was logged but
	// another dispatcher owns the delivery's state.
	Applied bool
	// Tripped is true when this failure opened the endpoint's circuit.
	Tripped bool
	// Disabled is true when this failure disabled the endpoint.
	Disabled bool
	// DisabledEvent is the id of the endpoint.disabled message sent to the
	// app's endpoints, set when Disabled is.
	DisabledEvent string
}

// Complete records an attempt, moves the delivery to its next state and
// updates the endpoint's health, in one transaction. The attempt and the
// health update always apply, because the HTTP call happened. The delivery's
// state changes only if the row still holds j's lease: if the lease expired
// and another dispatcher took the row over, that dispatcher owns its state.
func (s *Store) Complete(ctx context.Context, j Job, o Outcome, h HealthPolicy) (CompleteResult, error) {
	var r CompleteResult
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()

	a := o.Attempt
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO attempts (delivery_id, attempted_at, status_code, latency_ms, error, response_body)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''))`,
		j.DeliveryID, a.AttemptedAt, a.StatusCode, a.LatencyMS, a.Error, a.ResponseBody); err != nil {
		return r, err
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
		return r, err
	}
	n, _ := res.RowsAffected()
	r.Applied = n == 1

	if o.NextState == DeliverySucceeded {
		// Only write when there is something to reset, so successes to a
		// healthy endpoint do not all queue on its row lock.
		if _, err := tx.ExecContext(ctx, `
			UPDATE endpoints SET consecutive_failures = 0, failing_since = NULL,
			                     circuit_open_until = NULL, updated_at = now()
			WHERE id = $1 AND (consecutive_failures > 0 OR circuit_open_until IS NOT NULL)`,
			j.EndpointID); err != nil {
			return r, err
		}
	} else {
		// Columns on the right-hand side are the values before this update.
		err := tx.QueryRowContext(ctx, `
			UPDATE endpoints SET
				consecutive_failures = consecutive_failures + 1,
				failing_since = coalesce(failing_since, now()),
				circuit_open_until = CASE
					WHEN consecutive_failures + 1 >= $2 THEN now() + least(
						$3::bigint * power(2, least(consecutive_failures + 1 - $2, 20)),
						$4::bigint) * interval '1 millisecond'
					ELSE circuit_open_until END,
				status = CASE
					WHEN status = 'active' AND failing_since <= now() - $5::bigint * interval '1 millisecond'
					THEN 'disabled' ELSE status END,
				disabled_at = CASE
					WHEN status = 'active' AND failing_since <= now() - $5::bigint * interval '1 millisecond'
					THEN now() ELSE disabled_at END,
				updated_at = now()
			WHERE id = $1
			RETURNING consecutive_failures = $2, disabled_at IS NOT NULL AND disabled_at = now()`,
			j.EndpointID, h.FailureThreshold, h.CooldownBase.Milliseconds(),
			h.CooldownMax.Milliseconds(), h.DisableAfter.Milliseconds()).Scan(&r.Tripped, &r.Disabled)
		if err != nil && !errors.Is(err, sql.ErrNoRows) { // no rows: endpoint deleted meanwhile
			return r, err
		}
		if r.Disabled {
			// In the same transaction, so the event is sent exactly once.
			if r.DisabledEvent, err = sendDisabledEvent(ctx, tx, j.EndpointID); err != nil {
				return r, err
			}
		}
	}
	return r, tx.Commit()
}

// sendDisabledEvent creates an endpoint.disabled message in the endpoint's
// app and fans it out, returning the message id.
func sendDisabledEvent(ctx context.Context, tx *sql.Tx, endpointID string) (string, error) {
	msgID := id.New(id.Message)
	var appID string
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO messages (id, app_id, event_type, payload)
		SELECT $1, app_id, $3, jsonb_build_object(
			'endpoint_id', id, 'url', url,
			'failing_since', failing_since, 'disabled_at', disabled_at)
		FROM endpoints WHERE id = $2
		RETURNING app_id`, msgID, endpointID, EventEndpointDisabled).Scan(&appID); err != nil {
		return "", err
	}
	_, err := fanOut(ctx, tx, msgID, appID, EventEndpointDisabled, false)
	return msgID, err
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

// MaintainAttemptPartitions creates the attempt log's daily partitions for
// today and the next 7 days (UTC), and drops those older than retentionDays
// (0 keeps everything). Inserting an attempt for a day with no partition
// fails, so dispatchers call this at start and then regularly.
func (s *Store) MaintainAttemptPartitions(ctx context.Context, retentionDays int) (created, dropped int, err error) {
	if err := s.db.QueryRowContext(ctx, `
		SELECT create_attempt_partitions((now() AT TIME ZONE 'UTC')::date, (now() AT TIME ZONE 'UTC')::date + 7)`).
		Scan(&created); err != nil {
		return 0, 0, err
	}
	if retentionDays > 0 {
		err = s.db.QueryRowContext(ctx, `
			SELECT drop_attempt_partitions((now() AT TIME ZONE 'UTC')::date - $1::integer)`, retentionDays).
			Scan(&dropped)
	}
	return created, dropped, err
}
