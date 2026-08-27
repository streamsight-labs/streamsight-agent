package collector

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"kafka-metrics-agent/internal/config"
	"kafka-metrics-agent/internal/kafka"
	"kafka-metrics-agent/internal/metrics"
)

func TestNewCompilesFilters(t *testing.T) {
	c, err := New(nil, Options{
		TopicIncludeRegex: "^prod\\.",
		TopicExcludeRegex: "-dlq$",
		GroupExcludeRegex: "^test-",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !c.topics.allow("prod.orders") {
		t.Error("prod.orders should be allowed")
	}
	if c.topics.allow("prod.orders-dlq") {
		t.Error("prod.orders-dlq should be excluded")
	}
	if c.groups.allow("test-consumer") {
		t.Error("test-consumer should be excluded")
	}
	if !c.groups.allow("payments") {
		t.Error("payments should be allowed")
	}
	if c.log == nil {
		t.Error("logger must default to slog.Default()")
	}
}

func TestNewRejectsBadFilters(t *testing.T) {
	if _, err := New(nil, Options{TopicIncludeRegex: "(unclosed"}); err == nil {
		t.Error("want error for malformed topic include regex")
	}
	if _, err := New(nil, Options{GroupExcludeRegex: "(unclosed"}); err == nil {
		t.Error("want error for malformed group exclude regex")
	}
}

func TestZeroOptionsAllowEverything(t *testing.T) {
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, name := range []string{"orders", "__consumer_offsets", ""} {
		if !c.topics.allow(name) {
			t.Errorf("topic %q should be allowed by default", name)
		}
		if !c.groups.allow(name) {
			t.Errorf("group %q should be allowed by default", name)
		}
	}
	if c.opts.IncludeInternalTopics {
		t.Error("internal topics must be excluded by default")
	}
}

func TestFinalizeReportsCollapsedErrorsAtBatchLevel(t *testing.T) {
	// Deduplication runs even with no cap configured, so the batch-level block
	// is what tells a backend len(errors) is no longer the occurrence count.
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	secs := []*section{
		newSection(sectionCluster),
		newSection(sectionTopics),
		newSection(sectionTopicsEnd),
		newSection(sectionGroups),
		newSection(sectionOffsets),
	}
	for i := 0; i < 500; i++ {
		secs[1].recordPartition("Metadata", "orders", int32(i), kerr.NotLeaderForPartition)
	}

	batch := &metrics.Batch{}
	c.finalize(batch, secs, 0)

	if batch.Truncation == nil {
		t.Fatal("a batch whose errors were collapsed must carry a truncation block")
	}
	if batch.Truncation.ErrorsCollapsed != 499 || batch.Truncation.ErrorsDropped != 0 {
		t.Errorf("truncation = %+v, want 499 collapsed and 0 dropped", *batch.Truncation)
	}
	if len(batch.Errors) != 1 || batch.Errors[0].Count != 500 {
		t.Fatalf("errors = %+v, want one entry with count 500", batch.Errors)
	}
	if batch.Limits != nil {
		t.Error("limits must stay absent when no cap is configured")
	}

	// The section is still partial: collapsing must not hide the failure.
	if len(batch.Sections) != 5 {
		t.Fatalf("sections = %d, want 5", len(batch.Sections))
	}
	if batch.Sections[1].Status != metrics.SectionPartial || !batch.Sections[1].Truncated {
		t.Errorf("topics section = %+v, want partial and truncated", batch.Sections[1])
	}
	for _, i := range []int{0, 2, 3, 4} {
		if batch.Sections[i].Status != metrics.SectionOK {
			t.Errorf("section %q = %q, want ok", batch.Sections[i].Name, batch.Sections[i].Status)
		}
	}
}

// canonicalSections is the wire order Collect emits, and the order every
// section-order assertion in this package is written against. Keeping it here
// rather than inline is what makes the next added phase a one-line change with a
// failing test behind it.
var canonicalSections = []string{
	sectionCluster,
	sectionTopics, sectionTopicsWindow, sectionTopicsLSO, sectionTopicsEnd,
	sectionTopicsMaxTS, sectionTopicsLocal, sectionTopicsRemote,
	sectionGroups, sectionOffsets,
	sectionGroupStates, sectionEpochProbes,
	sectionLogDirs, sectionReassignments,
	sectionTopicConfigs, sectionBrokerConfigs, sectionShareGroups,
	sectionAuthorizedOps, sectionBrokerRPC,
}

func TestFinalizeEmitsEverySectionEvenWhenSkipped(t *testing.T) {
	// The wire contract: every section, in this order, on every cycle. A section
	// that disappears on the cycles it was skipped would destroy the "did not
	// run" vs "ran and found nothing" distinction — which is what makes the
	// optional and trigger-driven collectors safe to default off.
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	wantOrder := canonicalSections
	secs := make([]*section, 0, len(wantOrder))
	for _, name := range wantOrder {
		s := newSection(name)
		if name == sectionTopicsLSO || name == sectionLogDirs {
			s.downgrade(metrics.SectionSkipped)
		}
		secs = append(secs, s)
	}

	batch := &metrics.Batch{}
	c.finalize(batch, secs, 0)

	if len(batch.Sections) != len(wantOrder) {
		t.Fatalf("got %d sections, want %d", len(batch.Sections), len(wantOrder))
	}
	for i, want := range wantOrder {
		if batch.Sections[i].Name != want {
			t.Errorf("sections[%d] = %q, want %q", i, batch.Sections[i].Name, want)
		}
	}
	for _, name := range []string{sectionTopicsLSO, sectionLogDirs} {
		for _, s := range batch.Sections {
			if s.Name == name && s.Status != metrics.SectionSkipped {
				t.Errorf("section %q = %q, want skipped", name, s.Status)
			}
		}
	}
	// A skipped phase is not a degraded one.
	if batch.Truncation != nil || len(batch.Errors) != 0 {
		t.Errorf("skipping a phase must not look like a failure: %+v %+v", batch.Truncation, batch.Errors)
	}
}

// Collect against a cluster that is not there. Every request fails, which is
// precisely the path that must still produce the full section list — and it is
// the only test that drives the goroutine graph end to end, so a phase waiting
// on a channel nobody closes deadlocks here rather than in production.
func TestCollectEmitsEverySectionInOrderAgainstADeadCluster(t *testing.T) {
	// Port 1 refuses immediately: the point is a failing cluster, not a slow one.
	client, err := kafka.NewClient(&config.Config{
		KafkaBrokers:       []string{"127.0.0.1:1"},
		AgentInstanceID:    "test",
		CollectionInterval: time.Second,
		CollectionTimeout:  time.Second,
	}, "test")
	if err != nil {
		t.Fatalf("kafka.NewClient: %v", err)
	}
	defer client.Close()

	// Every optional phase on, so no section owes its presence to being skipped
	// by configuration.
	c, err := New(client, Options{
		Timeout:                 2 * time.Second,
		CollectLastStableOffset: true,
		CollectLogDirs:          true,
		CollectThroughputWindow: true,
		ThroughputWindowWidth:   time.Minute,
		CollectAuthorizedOps:    true,
		CollectReassignments:    true,
		CollectEpochProbes:      true,
		CollectRPCStats:         true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	batch := c.Collect(context.Background())

	names := make([]string, 0, len(batch.Sections))
	for _, s := range batch.Sections {
		names = append(names, s.Name)
	}
	if !slices.Equal(names, canonicalSections) {
		t.Fatalf("sections = %v, want %v", names, canonicalSections)
	}
	// The one section that cannot fail: it reads counters, and the failed dials
	// are themselves part of what it reports.
	for _, s := range batch.Sections {
		if s.Name == sectionBrokerRPC && s.Status != metrics.SectionOK {
			t.Errorf("broker_rpc = %q, want ok: the snapshot issues no request", s.Status)
		}
		if s.SampledAt.IsZero() {
			t.Errorf("section %q has no sampled_at", s.Name)
		}
	}
	if batch.Agent.RPC == nil {
		t.Error("agent.rpc is nil: the collector owns the window and must fill it")
	}
}

func TestFinalizeCleanBatchCarriesNoTruncation(t *testing.T) {
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	secs := []*section{newSection(sectionCluster), newSection(sectionTopics)}
	batch := &metrics.Batch{}
	c.finalize(batch, secs, 0)

	if batch.Truncation != nil {
		t.Errorf("truncation = %+v on a complete batch, want nil", *batch.Truncation)
	}
	if len(batch.Errors) != 0 {
		t.Errorf("errors = %+v, want none", batch.Errors)
	}
}

func TestFinalizeCountsGroupListTruncationOnce(t *testing.T) {
	// MaxGroups shortens both sections from one enforcement point, so the count
	// belongs at batch level exactly once.
	c, err := New(nil, Options{Limits: Limits{MaxGroups: 10}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	groupsSec, offsetsSec := c.newSection(sectionGroups), c.newSection(sectionOffsets)
	groupsSec.truncated = true
	offsetsSec.truncated = true

	batch := &metrics.Batch{}
	c.finalize(batch, []*section{groupsSec, offsetsSec}, 7)

	if batch.Truncation == nil || batch.Truncation.Groups != 7 {
		t.Fatalf("truncation = %+v, want groups 7 counted once", batch.Truncation)
	}
	for _, s := range batch.Sections {
		if !s.Truncated {
			t.Errorf("section %q must admit it is short", s.Name)
		}
	}
	if batch.Limits == nil || batch.Limits.MaxGroups != 10 {
		t.Errorf("limits = %+v, want the caps in force echoed", batch.Limits)
	}
}
