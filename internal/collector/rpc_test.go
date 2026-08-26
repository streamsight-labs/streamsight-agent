package collector

import (
	"testing"

	"kafka-metrics-agent/internal/metrics"
)

// Off means skipped, not absent and not empty: an operator who turned the
// section off must not look like an agent with no traffic. The nil client is
// safe precisely because a disabled phase touches no client.
func TestCollectRPCSkippedWhenDisabled(t *testing.T) {
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stats, sec := c.collectRPC(false)
	if stats != nil {
		t.Errorf("stats = %+v, want none", stats)
	}
	if sec.name != sectionBrokerRPC || sec.status != metrics.SectionSkipped {
		t.Errorf("section = %q/%q, want %q/skipped", sec.name, sec.status, sectionBrokerRPC)
	}
}

// The watcher is handed in as an object rather than a switch, so the collector
// must hold on to exactly the one the agent started.
func TestGroupStateWatchHandleReachesTheCollector(t *testing.T) {
	w, err := NewGroupStateWatcher(nil, GroupStateWatchOptions{})
	if err != nil {
		t.Fatalf("NewGroupStateWatcher: %v", err)
	}
	c, err := New(nil, Options{GroupStateWatch: w})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.stateWatch != w {
		t.Fatal("the collector did not keep the watcher it was configured with")
	}

	// And with no watcher the phase is skipped rather than absent, like every
	// other phase that did not run.
	off, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	watch, sec := off.collectGroupStates(off.stateWatch)
	if watch != nil {
		t.Errorf("watch = %+v, want none", watch)
	}
	if sec.name != sectionGroupStates || sec.status != metrics.SectionSkipped {
		t.Errorf("section = %q/%q, want %q/skipped", sec.name, sec.status, sectionGroupStates)
	}
}
