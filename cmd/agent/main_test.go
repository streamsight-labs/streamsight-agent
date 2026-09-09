package main

import (
	"strings"
	"testing"
)

// Everything the agent writes goes to stderr in one shape. The failure paths
// that reach main are the ones an evaluator hits first -- a typo in the config,
// a broker that will not dial -- and a second line format in the same stream is
// what a reader notices before the error itself.
func TestFatalMatchesTheAgentsLogFormat(t *testing.T) {
	var b strings.Builder
	fatal(&b, "agent failed", errStub{})

	got := b.String()
	for _, want := range []string{"level=ERROR", "msg=\"agent failed\"", "error=", "time="} {
		if !strings.Contains(got, want) {
			t.Errorf("fatal() = %q, missing %q", got, want)
		}
	}
	if strings.Count(got, "\n") != 1 {
		t.Errorf("fatal() wrote %d lines, want 1: %q", strings.Count(got, "\n"), got)
	}
}

type errStub struct{}

func (errStub) Error() string { return "boom" }
