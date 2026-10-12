// Package webhooks is the Go SDK for webhook-delivery-platform: a Client for
// the HTTP API, and Verify for receivers checking that a request is genuine.
//
// Sending:
//
//	c := webhooks.NewClient("http://localhost:8080", apiKey)
//	app, err := c.CreateApp(ctx, webhooks.CreateAppRequest{Name: "acme"})
//	ep, err := c.CreateEndpoint(ctx, app.ID, webhooks.CreateEndpointRequest{URL: "https://acme.example/hooks"})
//	msg, err := c.SendMessage(ctx, app.ID, webhooks.SendMessageRequest{
//		EventType: "order.paid", Payload: order, IdempotencyKey: "order-981-paid",
//	})
//
// Receiving:
//
//	event, err := webhooks.VerifyRequest(secret, r)
//
// It uses only the standard library.
package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client calls the API. It is safe for concurrent use.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client requests go through. The default has
// a 30 second timeout.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.http = h }
}

// NewClient returns a client for the API at baseURL (such as
// http://localhost:8080), authenticating with apiKey.
func NewClient(baseURL, apiKey string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Error is an error response from the API, an RFC 9457 problem.
type Error struct {
	StatusCode int    `json:"status"`
	Type       string `json:"type"`
	Title      string `json:"title"`
	Detail     string `json:"detail"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("webhooks: %d %s: %s", e.StatusCode, e.Title, e.Detail)
}

// --- applications ---

// CreateApp creates an application.
func (c *Client) CreateApp(ctx context.Context, req CreateAppRequest) (*App, error) {
	var out App
	_, err := c.do(ctx, http.MethodPost, "/v1/apps", nil, req.IdempotencyKey, req, &out)
	return &out, err
}

// GetApp reads an application.
func (c *Client) GetApp(ctx context.Context, appID string) (*App, error) {
	var out App
	_, err := c.do(ctx, http.MethodGet, path("v1/apps", appID), nil, "", nil, &out)
	return &out, err
}

// --- endpoints ---

// CreateEndpoint creates an endpoint. The result carries the signing secret,
// which no later read returns: hand it to the receiver now.
func (c *Client) CreateEndpoint(ctx context.Context, appID string, req CreateEndpointRequest) (*Endpoint, error) {
	var out Endpoint
	_, err := c.do(ctx, http.MethodPost, path("v1/apps", appID, "endpoints"), nil, req.IdempotencyKey, req, &out)
	return &out, err
}

// ListEndpoints returns a page of an app's endpoints, ordered by id.
func (c *Client) ListEndpoints(ctx context.Context, appID string, opts ListOptions) (*Page[Endpoint], error) {
	var out Page[Endpoint]
	_, err := c.do(ctx, http.MethodGet, path("v1/apps", appID, "endpoints"), opts.query(), "", nil, &out)
	return &out, err
}

// GetEndpoint reads an endpoint (without its secret).
func (c *Client) GetEndpoint(ctx context.Context, appID, endpointID string) (*Endpoint, error) {
	var out Endpoint
	_, err := c.do(ctx, http.MethodGet, path("v1/apps", appID, "endpoints", endpointID), nil, "", nil, &out)
	return &out, err
}

// UpdateEndpoint changes the fields set in req.
func (c *Client) UpdateEndpoint(ctx context.Context, appID, endpointID string, req UpdateEndpointRequest) (*Endpoint, error) {
	var out Endpoint
	_, err := c.do(ctx, http.MethodPatch, path("v1/apps", appID, "endpoints", endpointID), nil, "", req, &out)
	return &out, err
}

// DeleteEndpoint deletes an endpoint and its deliveries.
func (c *Client) DeleteEndpoint(ctx context.Context, appID, endpointID string) error {
	_, err := c.do(ctx, http.MethodDelete, path("v1/apps", appID, "endpoints", endpointID), nil, "", nil, nil)
	return err
}

// RotateSecret gives an endpoint a new signing secret, returned in the
// result. The old secret keeps signing too for 24 hours.
func (c *Client) RotateSecret(ctx context.Context, appID, endpointID string, req RotateSecretRequest) (*Endpoint, error) {
	var out Endpoint
	_, err := c.do(ctx, http.MethodPost, path("v1/apps", appID, "endpoints", endpointID, "secret", "rotate"), nil, "", req, &out)
	return &out, err
}

// EnableEndpoint re-enables an endpoint, typically one the service disabled,
// and clears its failure history.
func (c *Client) EnableEndpoint(ctx context.Context, appID, endpointID string) (*Endpoint, error) {
	var out Endpoint
	_, err := c.do(ctx, http.MethodPost, path("v1/apps", appID, "endpoints", endpointID, "enable"), nil, "", nil, &out)
	return &out, err
}

// ReplayEndpoint queues again every dead delivery to the endpoint created at
// or after since, and returns how many.
func (c *Client) ReplayEndpoint(ctx context.Context, appID, endpointID string, since time.Time) (int, error) {
	var out struct {
		Replayed int `json:"replayed"`
	}
	q := url.Values{"since": {since.UTC().Format(time.RFC3339)}}
	_, err := c.do(ctx, http.MethodPost, path("v1/apps", appID, "endpoints", endpointID, "replay"), q, "", nil, &out)
	return out.Replayed, err
}

// --- messages and deliveries ---

// SendMessage sends an event to every active endpoint of the app subscribed
// to its type.
func (c *Client) SendMessage(ctx context.Context, appID string, req SendMessageRequest) (*SendResult, error) {
	var out SendResult
	resp, err := c.do(ctx, http.MethodPost, path("v1/apps", appID, "messages"), nil, req.IdempotencyKey, req, &out)
	if err == nil {
		out.Replayed = resp.Header.Get("Idempotent-Replayed") == "true"
	}
	return &out, err
}

// ListDeliveries returns a page of a message's deliveries, ordered by id.
func (c *Client) ListDeliveries(ctx context.Context, appID, messageID string, opts ListOptions) (*Page[Delivery], error) {
	var out Page[Delivery]
	_, err := c.do(ctx, http.MethodGet, path("v1/apps", appID, "messages", messageID, "deliveries"), opts.query(), "", nil, &out)
	return &out, err
}

// ListAttempts returns a page of a delivery's attempts, oldest first.
func (c *Client) ListAttempts(ctx context.Context, appID, deliveryID string, opts ListOptions) (*Page[Attempt], error) {
	var out Page[Attempt]
	_, err := c.do(ctx, http.MethodGet, path("v1/apps", appID, "deliveries", deliveryID, "attempts"), opts.query(), "", nil, &out)
	return &out, err
}

// ReplayDelivery sends a dead or succeeded delivery again, with the same
// webhook-id and a fresh retry schedule.
func (c *Client) ReplayDelivery(ctx context.Context, appID, deliveryID string) (*Delivery, error) {
	var out Delivery
	_, err := c.do(ctx, http.MethodPost, path("v1/apps", appID, "deliveries", deliveryID, "replay"), nil, "", nil, &out)
	return &out, err
}

// --- plumbing ---

// path joins the segments, escaping each id.
func path(prefix string, ids ...string) string {
	var b strings.Builder
	b.WriteString("/" + prefix)
	for _, id := range ids {
		b.WriteString("/" + url.PathEscape(id))
	}
	return b.String()
}

func (o ListOptions) query() url.Values {
	q := url.Values{}
	if o.Limit > 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	if o.Cursor != "" {
		q.Set("cursor", o.Cursor)
	}
	return q
}

// do sends one request with in as the JSON body (nil for none) and decodes
// a 2xx response into out (nil to ignore it). Other statuses become *Error.
func (c *Client) do(ctx context.Context, method, p string, query url.Values, idempotencyKey string, in, out any) (*http.Response, error) {
	u := c.baseURL + p
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("webhooks: encode request: %w", err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		apiErr := &Error{StatusCode: resp.StatusCode, Title: http.StatusText(resp.StatusCode)}
		json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(apiErr) // best effort: keep the status if it is not a problem
		apiErr.StatusCode = resp.StatusCode
		return resp, apiErr
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp, fmt.Errorf("webhooks: decode %s %s response: %w", method, p, err)
		}
	}
	return resp, nil
}
