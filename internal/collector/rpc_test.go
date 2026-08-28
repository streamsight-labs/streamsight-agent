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
