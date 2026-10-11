package id

import (
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	a, b := New(Message), New(Message)
	if !strings.HasPrefix(a, "msg_") || len(a) != len("msg_")+32 {
		t.Errorf("unexpected id %q", a)
	}
	if a == b {
		t.Error("two ids were equal")
	}
}
