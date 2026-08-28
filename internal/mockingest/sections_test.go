package mockingest

import (
	"slices"
	"testing"

	"kafka-metrics-agent/internal/collector"
)

// TestDefaultSectionsMatchesTheCollector is the binding that DefaultSections did
// not have.
//
// DefaultSections is the required-section contract every batch is checked
// against, and it was a hand-written literal with nothing tying it to the
// collector that produces the sections. check_test.go's fixtures are themselves
// built to match that literal, so the two agreed circularly: when 0226e0c added
// three sections to internal/collector, DefaultSections kept the old five-name
// topics block and every test in this package still passed. The mock is a dev
// and CI tool run by hand, so nothing else was going to notice.
//
// This is a compile-time dependency on internal/collector confined to the test
// binary: the server itself still has no Kafka client in its import graph.
func TestDefaultSectionsMatchesTheCollector(t *testing.T) {
	want := collector.SectionNames()
	if !slices.Equal(DefaultSections, want) {
		t.Errorf("DefaultSections drifted from the collector's wire contract\n  DefaultSections = %v\n  collector       = %v",
			DefaultSections, want)
	}
	// Order is asserted, not just membership: DefaultSections is the only list
	// whose relative order the checker enforces, and that order encodes the
	// sampling order lag correctness depends on.
	if len(DefaultOptionalSections) != 0 {
		for _, name := range DefaultOptionalSections {
			if slices.Contains(want, name) {
				t.Errorf("%q is optional here but the collector emits it on every cycle; an absent one is a defect, not a configuration", name)
			}
		}
	}
}
