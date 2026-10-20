package webhooks

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/apispec"
)

// TestSDKMatchesOpenAPI checks the SDK against docs/openapi.json: a Client
// method for every API operation, and the same JSON fields as the spec.
func TestSDKMatchesOpenAPI(t *testing.T) {
	spec, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}
	client := reflect.TypeOf(&Client{})
	for route, op := range spec.Operations() {
		if slices.Contains(op.Tags, "Operations") { // health, metrics: not for SDK users
			continue
		}
		method := strings.ToUpper(op.OperationID[:1]) + op.OperationID[1:]
		if _, ok := client.MethodByName(method); !ok {
			t.Errorf("%s (%s) has no Client.%s", route, op.OperationID, method)
		}
	}

	responses := map[string]any{
		"App": App{}, "Endpoint": Endpoint{}, "Delivery": Delivery{}, "Attempt": Attempt{},
		"EndpointList": Page[Endpoint]{}, "DeliveryList": Page[Delivery]{}, "AttemptList": Page[Attempt]{},
		"SendMessageResponse": SendResult{}, "Message": Message{}, "MessageList": Page[Message]{},
	}
	requests := map[string]any{
		"CreateAppRequest": CreateAppRequest{}, "CreateEndpointRequest": CreateEndpointRequest{},
		"UpdateEndpointRequest": UpdateEndpointRequest{}, "RotateSecretRequest": RotateSecretRequest{},
		"SendMessageRequest": SendMessageRequest{},
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
}
