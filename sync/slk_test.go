package sync

import (
	"testing"
	"time"
)

func TestSLK(t *testing.T) {
	s := NewSLK()
	tk := "test"
	timeout := time.Millisecond * 500
	s.Add(tk, timeout)
	if !s.Get(tk) {
		t.Fatal("did not have key before timeout")
	}
	time.Sleep(timeout)
	if s.Get(tk) {
		t.Fatal("had after timeout")
	}
}
