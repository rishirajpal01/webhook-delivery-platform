package webhooks

import (
	"encoding/json"
	"time"
)

// App is one of the operator's customers. It owns endpoints and messages.
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
	// Secret is the whsec_ signing secret. It is only returned by
	// CreateEndpoint and RotateSecret.
	Secret string `json:"secret,omitempty"`
	// EventTypes lists the types delivered here; empty means every type the
	// operator sends.
	EventTypes []string `json:"event_types"`
	// Status is "active", "paused" or "disabled" (set by the service after
	// 5 days of continuous failure).
	Status string `json:"status"`
	// MaxInFlight caps concurrent sends, across all dispatchers.
	MaxInFlight int `json:"max_in_flight"`
	// RateLimit caps requests per second; 0 means unlimited.
	RateLimit           int        `json:"rate_limit"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	FailingSince        *time.Time `json:"failing_since"`
	CircuitOpenUntil    *time.Time `json:"circuit_open_until"`
	DisabledAt          *time.Time `json:"disabled_at"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

// Delivery is one message going to one endpoint.
type Delivery struct {
	ID         string `json:"id"`
	MessageID  string `json:"message_id"`
	EndpointID string `json:"endpoint_id"`
	// State is "pending", "in_flight", "succeeded" or "dead".
	State         string    `json:"state"`
	AttemptCount  int       `json:"attempt_count"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Attempt is one HTTP call made for a delivery.
type Attempt struct {
	ID          int64     `json:"id"`
	DeliveryID  string    `json:"delivery_id"`
	AttemptedAt time.Time `json:"attempted_at"`
	// StatusCode is nil when no response arrived.
	StatusCode   *int   `json:"status_code"`
	LatencyMS    int    `json:"latency_ms"`
	Error        string `json:"error,omitempty"`
	ResponseBody string `json:"response_body,omitempty"` // first 4 KB
}

// Page is one page of a list. Pass NextCursor as ListOptions.Cursor for the
// next page; it is empty on the last page.
type Page[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// ListOptions selects a page. Zero values mean the first page of 50.
type ListOptions struct {
	Limit  int // 1 to 100
	Cursor string
}

// SendResult is what SendMessage returns.
type SendResult struct {
	// ID is the message id, sent to receivers as the webhook-id header.
	ID string `json:"id"`
	// Deliveries is how many endpoints the message fanned out to.
	Deliveries int `json:"deliveries"`
	// Replayed is true when the Idempotency-Key matched an earlier send and
	// nothing new was queued.
	Replayed bool `json:"-"`
}

// CreateAppRequest creates an application.
type CreateAppRequest struct {
	Name string `json:"name"`
	// IdempotencyKey, if set, makes a retried create return the first app.
	IdempotencyKey string `json:"-"`
}

// CreateEndpointRequest creates an endpoint. Only URL is required.
type CreateEndpointRequest struct {
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types,omitempty"`
	// Secret is a whsec_ secret; one is generated when empty.
	Secret      string `json:"secret,omitempty"`
	MaxInFlight int    `json:"max_in_flight,omitempty"` // default 4
	RateLimit   int    `json:"rate_limit,omitempty"`    // requests per second; 0 is unlimited
	// IdempotencyKey, if set, makes a retried create return the first
	// endpoint, secret included.
	IdempotencyKey string `json:"-"`
}

// UpdateEndpointRequest changes the fields that are not nil. Ptr helps fill
// it in.
type UpdateEndpointRequest struct {
	URL        *string   `json:"url,omitempty"`
	EventTypes *[]string `json:"event_types,omitempty"`
	// Status is "active" or "paused". Setting "active" also re-enables a
	// disabled endpoint.
	Status      *string `json:"status,omitempty"`
	MaxInFlight *int    `json:"max_in_flight,omitempty"`
	RateLimit   *int    `json:"rate_limit,omitempty"` // 0 removes the limit
}

// RotateSecretRequest sets a new signing secret.
type RotateSecretRequest struct {
	// Secret is the new whsec_ secret; one is generated when empty.
	Secret string `json:"secret,omitempty"`
}

// SendMessageRequest sends an event.
type SendMessageRequest struct {
	EventType string `json:"event_type"`
	// Payload is any JSON-encodable value except nil. A json.RawMessage is
	// sent as is.
	Payload any `json:"payload"`
	// IdempotencyKey, if set, makes a retried send return the first message
	// instead of sending the event twice.
	IdempotencyKey string `json:"-"`
}

// Event is the body of a webhook request, the payload structure Standard
// Webhooks recommends.
type Event struct {
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// Ptr returns a pointer to v, for the optional fields of
// UpdateEndpointRequest.
func Ptr[T any](v T) *T { return &v }
