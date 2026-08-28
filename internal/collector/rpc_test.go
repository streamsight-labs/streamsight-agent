package collector

import (
	"testing"

	"kafka-metrics-agent/internal/kafka"
	"kafka-metrics-agent/internal/metrics"
)

// There is no switch for this phase, so the only way it reports nothing is a
// client whose hooks were never installed. That must read as skipped rather
// than as an agent that sent no requests: "not measured" and "measured, no
// traffic" are different facts about the cycle.
func TestCollectRPCSkippedWhenTheHooksAreAbsent(t *testing.T) {
	c, err := New(&kafka.Client{}, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stats, sec := c.collectRPC()
	if stats != nil {
		t.Errorf("stats = %+v, want none", stats)
	}
	if sec.name != sectionBrokerRPC || sec.status != metrics.SectionSkipped {
		t.Errorf("section = %q/%q, want %q/skipped", sec.name, sec.status, sectionBrokerRPC)
	}
}
