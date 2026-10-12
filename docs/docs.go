// Package docs embeds the API description, openapi.json, so the service can
// serve it and tests can check the code against it.
package docs

import _ "embed"

// OpenAPI is the OpenAPI 3.1 description of the HTTP API. It is the source
// of truth: contract tests fail when the server or the Go SDK disagree with
// it.
//
//go:embed openapi.json
var OpenAPI []byte
