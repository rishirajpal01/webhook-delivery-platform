package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/id"
)

// ErrNotFound is returned when a looked-up row does not exist (or belongs to
// another application).
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when a row is not in a state the operation allows,
// such as replaying a delivery that is still pending.
var ErrConflict = errors.New("conflict")

// ErrIdempotencyMismatch is returned when an Idempotency-Key is reused with a
// different request than the one it was first used for.
var ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")

// Endpoint statuses.
const (
	EndpointActive   = "active"
	EndpointPaused   = "paused"
	EndpointDisabled = "disabled"
)

// EventEndpointDisabled is the type of the message the service sends when it
// disables an endpoint that kept failing. Only endpoints that list it in
// their event types receive it; an empty filter means every type the
// operator sends, not the service's own events.
const EventEndpointDisabled = "endpoint.disabled"

// Delivery states.
const (
	DeliveryPending   = "pending"
	DeliveryInFlight  = "in_flight"
	DeliverySucceeded = "succeeded"
	DeliveryDead      = "dead"
)

// App is one of the operator's customers.
type App struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// Endpoint is a URL an app receives webhooks at.
type Endpoint struct {
	ID    string `json:"id"`
	AppID string `json:"app_id"`
	URL   string `json:"url"`
	// Secret is only filled in where the caller needs it (on create, and for
	// the dispatcher); the API leaves it empty otherwise.
	Secret     string   `json:"secret,omitempty"`
	EventTypes []string `json:"event_types"`
	Status     string   `json:"status"`
	// MaxInFlight caps how many deliveries to this endpoint are sent at
	// once, across all dispatchers. 0 on create means the default, 4.
	MaxInFlight int `json:"max_in_flight"`
	// RateLimit caps requests per second; 0 means unlimited.
	RateLimit int `json:"rate_limit"`
	// Health, maintained by the dispatcher.
	ConsecutiveFailures int        `json:"consecutive_failures"`
	FailingSince        *time.Time `json:"failing_since"`
	CircuitOpenUntil    *time.Time `json:"circuit_open_until"`
	DisabledAt          *time.Time `json:"disabled_at"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

// Message is one event sent by the operator.
type Message struct {
	ID             string          `json:"id"`
	AppID          string          `json:"app_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// Delivery is one message going to one endpoint.
type Delivery struct {
	ID            string    `json:"id"`
	MessageID     string    `json:"message_id"`
	EndpointID    string    `json:"endpoint_id"`
	State         string    `json:"state"`
	AttemptCount  int       `json:"attempt_count"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Attempt is one HTTP call made for a delivery.
type Attempt struct {
	ID           int64     `json:"id"`
	DeliveryID   string    `json:"delivery_id"`
	AttemptedAt  time.Time `json:"attempted_at"`
	StatusCode   *int      `json:"status_code"` // nil when no response arrived
	LatencyMS    int       `json:"latency_ms"`
	Error        string    `json:"error,omitempty"`
	ResponseBody string    `json:"response_body,omitempty"`
}

// Store runs the service's queries.
type Store struct {
	db *sql.DB
}

// NewStore wraps an open database.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// --- applications ---

// CreateApp creates an application.
func (s *Store) CreateApp(ctx context.Context, name string) (App, error) {
	a, _, err := s.CreateAppIdempotent(ctx, name, "")
	return a, err
}

// CreateAppIdempotent creates an application. With a non-empty key, a second
// call with the same key returns the first application and reports
// duplicate, creating nothing.
func (s *Store) CreateAppIdempotent(ctx context.Context, name, key string) (a App, duplicate bool, err error) {
	a = App{ID: id.New(id.App), Name: name}
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO applications (id, name, idempotency_key) VALUES ($1, $2, nullif($3, ''))
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING created_at`,
		a.ID, a.Name, key).Scan(&a.CreatedAt)
	if !errors.Is(err, sql.ErrNoRows) {
		return a, false, err
	}
	// The key was used before. ON CONFLICT waited for that insert to commit,
	// so the original row is visible to this next statement.
	err = s.db.QueryRowContext(ctx,
		`SELECT id, name, created_at FROM applications WHERE idempotency_key = $1`, key).
		Scan(&a.ID, &a.Name, &a.CreatedAt)
	if err == nil && a.Name != name {
		return App{}, false, ErrIdempotencyMismatch
	}
	return a, true, err
}

// GetApp returns one application.
func (s *Store) GetApp(ctx context.Context, appID string) (App, error) {
	var a App
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, created_at FROM applications WHERE id = $1`, appID).
		Scan(&a.ID, &a.Name, &a.CreatedAt)
	return a, notFound(err)
}

// --- endpoints ---

const endpointColumns = `id, app_id, url, secret, event_types, status,
	max_in_flight, coalesce(rate_limit, 0), consecutive_failures, failing_since, circuit_open_until, disabled_at, created_at, updated_at`

// scanEndpoint reads endpointColumns, then any extra columns into extra.
func scanEndpoint(row interface{ Scan(...any) error }, extra ...any) (Endpoint, error) {
	var e Endpoint
	err := row.Scan(append([]any{&e.ID, &e.AppID, &e.URL, &e.Secret,
		pgtype.NewMap().SQLScanner(&e.EventTypes), &e.Status, &e.MaxInFlight, &e.RateLimit,
		&e.ConsecutiveFailures, &e.FailingSince, &e.CircuitOpenUntil, &e.DisabledAt,
		&e.CreatedAt, &e.UpdatedAt}, extra...)...)
	return e, err
}

// CreateEndpoint creates an endpoint. e.ID and timestamps are filled in.
func (s *Store) CreateEndpoint(ctx context.Context, e Endpoint) (Endpoint, error) {
	created, _, err := s.CreateEndpointIdempotent(ctx, e, "")
	return created, err
}

// CreateEndpointIdempotent creates an endpoint. With a non-empty key, a
// second call with the same key for the same app returns the first endpoint
// (secret included) and reports duplicate, creating nothing.
func (s *Store) CreateEndpointIdempotent(ctx context.Context, e Endpoint, key string) (created Endpoint, duplicate bool, err error) {
	if e.EventTypes == nil {
		e.EventTypes = []string{}
	}
	fingerprint := endpointFingerprint(e)
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO endpoints (id, app_id, url, secret, event_types, max_in_flight, rate_limit,
		                       idempotency_key, idempotency_fingerprint)
		SELECT $1, $2, $3, $4, $5, coalesce(nullif($6, 0), 4), nullif($7, 0), nullif($8, ''), nullif($9, '')
		WHERE EXISTS (SELECT 1 FROM applications WHERE id = $2)
		ON CONFLICT (app_id, idempotency_key) DO NOTHING
		RETURNING `+endpointColumns,
		id.New(id.Endpoint), e.AppID, e.URL, e.Secret, e.EventTypes, e.MaxInFlight, e.RateLimit,
		key, fingerprint)
	created, err = scanEndpoint(row)
	if !errors.Is(err, sql.ErrNoRows) || key == "" {
		return created, false, notFound(err)
	}
	// No row: either the app does not exist or the key was used before.
	var same bool
	row = s.db.QueryRowContext(ctx,
		`SELECT `+endpointColumns+`, coalesce(idempotency_fingerprint = $3, true)
		 FROM endpoints WHERE app_id = $1 AND idempotency_key = $2`, e.AppID, key, fingerprint)
	created, err = scanEndpoint(row, &same)
	if err == nil && !same {
		return Endpoint{}, false, ErrIdempotencyMismatch
	}
	return created, err == nil, notFound(err)
}

// endpointFingerprint identifies what a create asked for. A secret the
// caller left out is generated anew on every try, so it is not part of it.
func endpointFingerprint(e Endpoint) string {
	b, _ := json.Marshal([]any{e.URL, e.EventTypes, e.MaxInFlight, e.RateLimit})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// GetEndpoint returns one endpoint of an app.
func (s *Store) GetEndpoint(ctx context.Context, appID, endpointID string) (Endpoint, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+endpointColumns+` FROM endpoints WHERE app_id = $1 AND id = $2`, appID, endpointID)
	e, err := scanEndpoint(row)
	return e, notFound(err)
}

// ListEndpoints returns up to limit endpoints of an app with ids after the
// cursor (empty for the first page), ordered by id.
func (s *Store) ListEndpoints(ctx context.Context, appID, after string, limit int) ([]Endpoint, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+endpointColumns+` FROM endpoints
		 WHERE app_id = $1 AND id > $2 ORDER BY id LIMIT $3`, appID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Endpoint{}
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EndpointPatch holds the fields an update may change; nil means unchanged.
type EndpointPatch struct {
	URL         *string
	EventTypes  *[]string
	Status      *string
	MaxInFlight *int
	RateLimit   *int // 0 removes the limit
}

// UpdateEndpoint applies a patch and returns the updated endpoint. Setting
// the status to active also clears the endpoint's failure history, so a
// disabled or tripped endpoint starts again with a closed circuit.
func (s *Store) UpdateEndpoint(ctx context.Context, appID, endpointID string, p EndpointPatch) (Endpoint, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE endpoints SET
			url         = COALESCE($3, url),
			event_types = COALESCE($4, event_types),
			status      = COALESCE($5, status),
			max_in_flight = COALESCE($6, max_in_flight),
			rate_limit    = CASE WHEN $7::integer IS NULL THEN rate_limit ELSE nullif($7, 0) END,
			consecutive_failures = CASE WHEN $5 = 'active' THEN 0 ELSE consecutive_failures END,
			failing_since        = CASE WHEN $5 = 'active' THEN NULL ELSE failing_since END,
			circuit_open_until   = CASE WHEN $5 = 'active' THEN NULL ELSE circuit_open_until END,
			disabled_at          = CASE WHEN $5 = 'active' THEN NULL ELSE disabled_at END,
			updated_at  = now()
		WHERE app_id = $1 AND id = $2
		RETURNING `+endpointColumns,
		appID, endpointID, p.URL, p.EventTypes, p.Status, p.MaxInFlight, p.RateLimit)
	e, err := scanEndpoint(row)
	if err == nil && e.Status == EndpointActive && p.Status != nil {
		// Resuming makes the deliveries held while paused claimable.
		err = notifyDue(ctx, s.db)
	}
	return e, notFound(err)
}

// RotateSecret makes newSecret the endpoint's signing secret. The old secret
// stays valid for overlap: requests are signed with both until then, so
// receivers can switch over without dropping any.
func (s *Store) RotateSecret(ctx context.Context, appID, endpointID, newSecret string, overlap time.Duration) (Endpoint, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE endpoints SET
			previous_secret            = secret,
			previous_secret_expires_at = now() + $4::bigint * interval '1 millisecond',
			secret                     = $3,
			updated_at                 = now()
		WHERE app_id = $1 AND id = $2
		RETURNING `+endpointColumns,
		appID, endpointID, newSecret, overlap.Milliseconds())
	e, err := scanEndpoint(row)
	return e, notFound(err)
}

// DeleteEndpoint removes an endpoint and, by cascade, its deliveries.
func (s *Store) DeleteEndpoint(ctx context.Context, appID, endpointID string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM endpoints WHERE app_id = $1 AND id = $2`, appID, endpointID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// --- messages ---

// SendResult is what CreateMessage reports back.
type SendResult struct {
	Message    Message
	Deliveries int
	// Duplicate is true when the idempotency key matched an earlier message,
	// which is returned instead of creating a new one.
	Duplicate bool
}

// CreateMessage stores a message and, in the same transaction, one pending
// delivery per active endpoint subscribed to its event type. A crash can
// never leave a message without its deliveries.
//
// With a non-empty idempotency key, a second call with the same key for the
// same app returns the first message and creates nothing.
func (s *Store) CreateMessage(ctx context.Context, m Message) (SendResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SendResult{}, err
	}
	defer tx.Rollback()

	var exists bool
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM applications WHERE id = $1)`, m.AppID).Scan(&exists); err != nil {
		return SendResult{}, err
	}
	if !exists {
		return SendResult{}, ErrNotFound
	}

	m.ID = id.New(id.Message)
	key := sql.NullString{String: m.IdempotencyKey, Valid: m.IdempotencyKey != ""}
	err = tx.QueryRowContext(ctx, `
		INSERT INTO messages (id, app_id, event_type, payload, idempotency_key)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (app_id, idempotency_key) DO NOTHING
		RETURNING created_at`,
		m.ID, m.AppID, m.EventType, string(m.Payload), key).Scan(&m.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		// The key was used before. ON CONFLICT waited for that insert to
		// commit, so the original row is visible now.
		return s.existingMessage(ctx, tx, m)
	}
	if err != nil {
		return SendResult{}, err
	}

	n, err := fanOut(ctx, tx, m.ID, m.AppID, m.EventType, true)
	if err != nil {
		return SendResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SendResult{}, err
	}
	return SendResult{Message: m, Deliveries: n}, nil
}

// fanOut creates one pending delivery of a message per active or paused
// endpoint of the app subscribed to its event type, and returns how many.
// A paused endpoint's deliveries wait until it is resumed; a disabled
// endpoint gets none. Endpoints with an empty filter receive every type only
// when wildcard is true.
func fanOut(ctx context.Context, tx *sql.Tx, messageID, appID, eventType string, wildcard bool) (int, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO deliveries (id, message_id, endpoint_id)
		SELECT 'del_' || replace(gen_random_uuid()::text, '-', ''), $1, e.id
		FROM endpoints e
		WHERE e.app_id = $2 AND e.status IN ('active', 'paused')
		  AND (($4::boolean AND cardinality(e.event_types) = 0) OR $3 = ANY (e.event_types))`,
		messageID, appID, eventType, wildcard)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return int(n), err
	}
	return int(n), notifyDue(ctx, tx)
}

// DueChannel is the channel dispatchers LISTEN on. A notification means new
// deliveries may be due; it carries no payload.
const DueChannel = "webhookd_due"

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// notifyDue wakes listening dispatchers. Inside a transaction the
// notification is sent on commit, and repeats within it are folded into one.
func notifyDue(ctx context.Context, q execer) error {
	_, err := q.ExecContext(ctx, `SELECT pg_notify($1, '')`, DueChannel)
	return err
}

// existingMessage returns the message an earlier send with m's idempotency
// key created, or ErrIdempotencyMismatch if that send had another event type
// or payload.
func (s *Store) existingMessage(ctx context.Context, tx *sql.Tx, m Message) (SendResult, error) {
	var (
		r       = SendResult{Duplicate: true}
		payload string
		same    bool
	)
	err := tx.QueryRowContext(ctx, `
		SELECT m.id, m.app_id, m.event_type, m.payload::text, m.idempotency_key, m.created_at,
		       (SELECT count(*) FROM deliveries d WHERE d.message_id = m.id),
		       m.event_type = $3 AND m.payload = $4::jsonb
		FROM messages m WHERE m.app_id = $1 AND m.idempotency_key = $2`,
		m.AppID, m.IdempotencyKey, m.EventType, string(m.Payload)).
		Scan(&r.Message.ID, &r.Message.AppID, &r.Message.EventType, &payload,
			&r.Message.IdempotencyKey, &r.Message.CreatedAt, &r.Deliveries, &same)
	if err == nil && !same {
		return SendResult{}, ErrIdempotencyMismatch
	}
	r.Message.Payload = json.RawMessage(payload)
	return r, err
}

// ListMessages returns up to limit of an app's messages, newest first,
// starting after the message the cursor names (empty for the first page).
func (s *Store) ListMessages(ctx context.Context, appID, after string, limit int) ([]Message, error) {
	if _, err := s.GetApp(ctx, appID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT m.id, m.app_id, m.event_type, m.payload::text, coalesce(m.idempotency_key, ''), m.created_at
		FROM messages m
		WHERE m.app_id = $1
		  AND ($2 = '' OR (m.created_at, m.id) < (SELECT c.created_at, c.id FROM messages c
		                                          WHERE c.app_id = $1 AND c.id = $2))
		ORDER BY m.created_at DESC, m.id DESC
		LIMIT $3`, appID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		var (
			m       Message
			payload string
		)
		if err := rows.Scan(&m.ID, &m.AppID, &m.EventType, &payload, &m.IdempotencyKey, &m.CreatedAt); err != nil {
			return nil, err
		}
		m.Payload = json.RawMessage(payload)
		out = append(out, m)
	}
	return out, rows.Err()
}

// --- replay ---

const deliveryColumns = `d.id, d.message_id, d.endpoint_id, d.state, d.attempt_count, d.next_attempt_at, d.created_at, d.updated_at`

func scanDelivery(row interface{ Scan(...any) error }) (Delivery, error) {
	var d Delivery
	err := row.Scan(&d.ID, &d.MessageID, &d.EndpointID, &d.State, &d.AttemptCount,
		&d.NextAttemptAt, &d.CreatedAt, &d.UpdatedAt)
	return d, err
}

// ReplayDelivery sends a dead or succeeded delivery again, with a fresh
// retry schedule. The message id, and so the webhook-id header, is unchanged.
// It returns ErrConflict if the delivery is still pending or in flight.
func (s *Store) ReplayDelivery(ctx context.Context, appID, deliveryID string) (Delivery, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE deliveries d SET
			state = 'pending', attempt_count = 0, next_attempt_at = now(), updated_at = now()
		FROM messages m
		WHERE d.id = $2 AND m.id = d.message_id AND m.app_id = $1
		  AND d.state IN ('dead', 'succeeded')
		RETURNING `+deliveryColumns, appID, deliveryID)
	d, err := scanDelivery(row)
	if err == nil {
		return d, notifyDue(ctx, s.db)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return d, err
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM deliveries d JOIN messages m ON m.id = d.message_id
		               WHERE m.app_id = $1 AND d.id = $2)`, appID, deliveryID).Scan(&exists); err != nil {
		return Delivery{}, err
	}
	if exists {
		return Delivery{}, ErrConflict
	}
	return Delivery{}, ErrNotFound
}

// ReplayEndpoint sends again every dead delivery to an endpoint whose
// message was created at or after since. It returns how many it queued.
func (s *Store) ReplayEndpoint(ctx context.Context, appID, endpointID string, since time.Time) (int, error) {
	if _, err := s.GetEndpoint(ctx, appID, endpointID); err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE deliveries SET
			state = 'pending', attempt_count = 0, next_attempt_at = now(), updated_at = now()
		WHERE endpoint_id = $1 AND state = 'dead' AND created_at >= $2`, endpointID, since)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return int(n), err
	}
	return int(n), notifyDue(ctx, s.db)
}

// --- delivery log ---

// ListDeliveries returns up to limit deliveries of one message with ids
// after the cursor (empty for the first page), ordered by id.
func (s *Store) ListDeliveries(ctx context.Context, appID, messageID, after string, limit int) ([]Delivery, error) {
	var found bool
	if err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM messages WHERE app_id = $1 AND id = $2)`,
		appID, messageID).Scan(&found); err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT `+deliveryColumns+` FROM deliveries d
		WHERE d.message_id = $1 AND d.id > $2 ORDER BY d.id LIMIT $3`, messageID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Delivery{}
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ListAttempts returns up to limit attempts of one delivery with ids after
// the cursor (0 for the first page), oldest first.
func (s *Store) ListAttempts(ctx context.Context, appID, deliveryID string, after int64, limit int) ([]Attempt, error) {
	var found bool
	if err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM deliveries d JOIN messages m ON m.id = d.message_id
		               WHERE m.app_id = $1 AND d.id = $2)`, appID, deliveryID).Scan(&found); err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, delivery_id, attempted_at, status_code, latency_ms,
		       coalesce(error, ''), coalesce(response_body, '')
		FROM attempts WHERE delivery_id = $1 AND id > $2 ORDER BY id LIMIT $3`, deliveryID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Attempt{}
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.DeliveryID, &a.AttemptedAt, &a.StatusCode, &a.LatencyMS,
			&a.Error, &a.ResponseBody); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func notFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
