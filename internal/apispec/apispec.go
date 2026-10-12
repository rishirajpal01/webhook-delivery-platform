// Package apispec reads docs/openapi.json for contract tests, which check
// that the server and the Go SDK match the spec.
package apispec

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/rishirajpal01/webhook-delivery-platform/docs"
)

// Spec is the part of an OpenAPI document the contract tests need.
type Spec struct {
	Paths      map[string]map[string]Operation `json:"paths"`
	Components struct {
		Schemas map[string]Schema `json:"schemas"`
	} `json:"components"`
}

// Operation is one method on one path.
type Operation struct {
	OperationID string   `json:"operationId"`
	Tags        []string `json:"tags"`
}

// Schema is an object schema.
type Schema struct {
	Properties map[string]json.RawMessage `json:"properties"`
	Required   []string                   `json:"required"`
}

// Load parses the embedded spec.
func Load() (*Spec, error) {
	var s Spec
	if err := json.Unmarshal(docs.OpenAPI, &s); err != nil {
		return nil, fmt.Errorf("parse docs/openapi.json: %w", err)
	}
	return &s, nil
}

// Operations returns every operation keyed "METHOD /path", the form
// http.ServeMux patterns take.
func (s *Spec) Operations() map[string]Operation {
	ops := map[string]Operation{}
	for path, methods := range s.Paths {
		for method, op := range methods {
			ops[strings.ToUpper(method)+" "+path] = op
		}
	}
	return ops
}

// CheckFields reports how a schema's properties differ from the JSON fields
// of v's struct type. With checkRequired, the schema's required properties
// must also be exactly the fields without omitempty, as for a response.
func (s *Spec) CheckFields(schema string, v any, checkRequired bool) error {
	sc, ok := s.Components.Schemas[schema]
	if !ok {
		return fmt.Errorf("schema %s is not in the spec", schema)
	}
	fields, required := jsonFields(reflect.TypeOf(v))
	if got, want := slices.Sorted(maps.Keys(fields)), slices.Sorted(maps.Keys(sc.Properties)); !slices.Equal(got, want) {
		return fmt.Errorf("schema %s: Go fields %v, spec properties %v", schema, got, want)
	}
	if checkRequired {
		if got, want := slices.Sorted(slices.Values(required)), slices.Sorted(slices.Values(sc.Required)); !slices.Equal(got, want) {
			return fmt.Errorf("schema %s: Go fields without omitempty %v, spec required %v", schema, got, want)
		}
	}
	return nil
}

// jsonFields returns the JSON names of a struct type's fields, and the ones
// always present (no omitempty).
func jsonFields(t reflect.Type) (fields map[string]bool, required []string) {
	fields = map[string]bool{}
	for i := range t.NumField() {
		f := t.Field(i)
		name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" || !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		fields[name] = true
		if !strings.Contains(opts, "omitempty") {
			required = append(required, name)
		}
	}
	return fields, required
}
