// Package id generates the service's prefixed random identifiers, such as
// app_3f2a… or msg_9c1e….
package id

import (
	"crypto/rand"
	"encoding/hex"
)

// Prefixes for each kind of object.
const (
	App      = "app"
	Endpoint = "ep"
	Message  = "msg"
	Delivery = "del"
)

// New returns prefix_ followed by 32 random hex characters (128 bits).
func New(prefix string) string {
	b := make([]byte, 16)
	rand.Read(b) // never returns an error; panics if the OS source fails
	return prefix + "_" + hex.EncodeToString(b)
}
