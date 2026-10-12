package api

import (
	"maps"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/apispec"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
)

// TestServerMatchesOpenAPI checks docs/openapi.json against the server: the
// same routes, and the same JSON fields in every request and response body.
func TestServerMatchesOpenAPI(t *testing.T) {
	spec, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}

	var served []string
	for _, rt := range (&server{}).routes() {
		served = append(served, rt.pattern)
	}
	served = append(served, "GET /healthz", "GET /readyz", "GET /metrics", "GET /openapi.json")
	inSpec := spec.Operations()
	for _, p := range served {
		if _, ok := inSpec[p]; !ok {
			t.Errorf("%s is served but not in docs/openapi.json", p)
		}
	}
	for _, p := range slices.Sorted(maps.Keys(inSpec)) {
		if !slices.Contains(served, p) {
			t.Errorf("%s is in docs/openapi.json but not served", p)
		}
	}

	responses := map[string]any{
		"App":                    postgres.App{},
		"Endpoint":               postgres.Endpoint{},
		"Delivery":               postgres.Delivery{},
		"Attempt":                postgres.Attempt{},
		"EndpointList":           list[postgres.Endpoint]{},
		"DeliveryList":           list[postgres.Delivery]{},
		"AttemptList":            list[postgres.Attempt]{},
		"SendMessageResponse":    sendMessageResponse{},
		"ReplayEndpointResponse": replayEndpointResponse{},
	}
	requests := map[string]any{
		"CreateAppRequest":      createAppRequest{},
		"CreateEndpointRequest": createEndpointRequest{},
		"UpdateEndpointRequest": updateEndpointRequest{},
		"RotateSecretRequest":   rotateSecretRequest{},
		"ReplayEndpointRequest": replayEndpointRequest{},
		"SendMessageRequest":    sendMessageRequest{},
	}
	for name, v := range responses {
		if err := spec.CheckFields(name, v, true); err != nil {
			t.Error(err)
		}
	}
	for name, v := range requests {
		if err := spec.CheckFields(name, v, false); err != nil {
			t.Error(err)
		}
	}
	// Problem and Status are written from maps; every other schema must be
	// checked above.
	for name := range spec.Components.Schemas {
		if _, ok := responses[name]; !ok && requests[name] == nil && name != "Problem" && name != "Status" {
			t.Errorf("schema %s is not checked against a Go type", name)
		}
	}

	rec := httptest.NewRecorder()
	NewOpsHandler(fakePinger{}, discard).ServeHTTP(rec, httptest.NewRequest("GET", "/openapi.json", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"openapi": "3.1.0"`) {
		t.Errorf("GET /openapi.json = %d", rec.Code)
	}
}
