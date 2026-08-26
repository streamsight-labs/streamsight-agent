package collector

import (
	"context"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

func urpTopic(name string, replicas, isr []int32) metrics.TopicMetrics {
	return metrics.TopicMetrics{
		Name:       name,
		Partitions: []metrics.Partition{{ID: 0, Replicas: replicas, ISR: isr}},
	}
}

func TestCollectReassignmentsSkippedPathsIssueNoRequest(t *testing.T) {
	// The nil client is the assertion: any path that reaches
	// ListPartitionReassignments panics. In steady state there are no URPs, and
	// the phase must cost nothing at all.
	moving := []metrics.TopicMetrics{urpTopic("orders", []int32{1, 2, 3}, []int32{1, 2})}

	tests := []struct {
		name   string
		topics []metrics.TopicMetrics
		run    bool
	}{
		{name: "phase disabled or cluster below 2.4", topics: moving, run: false},
		{name: "cluster metadata failed", topics: nil, run: true},
		{
			name:   "every partition is fully replicated",
			topics: []metrics.TopicMetrics{urpTopic("orders", []int32{1, 2, 3}, []int32{3, 1, 2})},
			run:    true,
		},
		{
			name:   "partition metadata failed, so no replica list",
			topics: []metrics.TopicMetrics{urpTopic("orders", nil, nil)},
			run:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(nil, Options{})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			got, sec := c.collectReassignments(context.Background(), tt.topics, tt.run)
			if got != nil {
				t.Errorf("reassignments = %+v, want none", got)
			}
			if sec.status != metrics.SectionSkipped {
				t.Errorf("reassignments = %q, want skipped", sec.status)
			}
			if len(sec.errs) != 0 {
				t.Errorf("a skipped section must record nothing, got %+v", sec.errs)
			}
		})
	}
}

func TestUnderReplicatedAsksOnlyAboutURPs(t *testing.T) {
	topics := []metrics.TopicMetrics{
		{Name: "orders", Partitions: []metrics.Partition{
			{ID: 0, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
			{ID: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1}},
			// Leaderless: the ISR is empty, which is a URP and not a partition
			// with nothing to say.
			{ID: 2, Replicas: []int32{1, 2, 3}},
		}},
		{Name: "payments", Partitions: []metrics.Partition{
			// Metadata failed for this one: no replicas is unknown, not zero.
			{ID: 0},
			{ID: 1, Replicas: []int32{4, 5}, ISR: []int32{4}},
		}},
	}

	set, truncated := underReplicated(topics)
	if truncated {
		t.Error("truncated = true, want false")
	}
	want := []kadm.TopicPartitions{
		{Topic: "orders", Partitions: []int32{1, 2}},
		{Topic: "payments", Partitions: []int32{1}},
	}
	if got := fmt.Sprint([]kadm.TopicPartitions(set.Sorted())); got != fmt.Sprint(want) {
		t.Errorf("set = %s, want %s", got, fmt.Sprint(want))
	}
}

func TestUnderReplicatedCapsTheRequestAndFlagsIt(t *testing.T) {
	// A rack outage under-replicates everything at once. The partition IDs
	// travel in the request body, so the phase must not scale with the outage.
	parts := make([]metrics.Partition, 0, maxReassignPartitions+50)
	for i := 0; i < maxReassignPartitions+50; i++ {
		parts = append(parts, metrics.Partition{ID: int32(i), Replicas: []int32{1, 2, 3}, ISR: []int32{1}})
	}
	topics := []metrics.TopicMetrics{{Name: "orders", Partitions: parts}}

	set, truncated := underReplicated(topics)
	if !truncated {
		t.Error("truncated = false, want true")
	}
	if n := len(set["orders"]); n != maxReassignPartitions {
		t.Errorf("asked about %d partitions, want %d", n, maxReassignPartitions)
	}
	// A stable prefix, so the same partitions are asked about every cycle.
	if !set.Lookup("orders", 0) || set.Lookup("orders", maxReassignPartitions) {
		t.Error("the kept partitions are not the first maxReassignPartitions")
	}
}

func TestBuildReassignmentsSortsAndDropsSettledPartitions(t *testing.T) {
	listed := kadm.ListPartitionReassignmentsResponses{
		"payments": {
			0: {Topic: "payments", Partition: 0, Replicas: []int32{4, 5}, RemovingReplicas: []int32{5}},
		},
		"orders": {
			2: {Topic: "orders", Partition: 2, Replicas: []int32{1, 2, 3}, AddingReplicas: []int32{3}},
			1: {Topic: "orders", Partition: 1, Replicas: []int32{1, 2, 3}, AddingReplicas: []int32{2, 3}, RemovingReplicas: []int32{1}},
			// Nothing is moving here: the URP has some other cause, and claiming
			// a reassignment would answer the alert's question backwards.
			0: {Topic: "orders", Partition: 0, Replicas: []int32{1, 2, 3}},
		},
	}

	got := buildReassignments(listed)
	want := []metrics.Reassignment{
		{Topic: "orders", Partition: 1, Replicas: []int32{1, 2, 3}, AddingReplicas: []int32{2, 3}, RemovingReplicas: []int32{1}},
		{Topic: "orders", Partition: 2, Replicas: []int32{1, 2, 3}, AddingReplicas: []int32{3}},
		{Topic: "payments", Partition: 0, Replicas: []int32{4, 5}, RemovingReplicas: []int32{5}},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("reassignments =\n%v\nwant\n%v", got, want)
	}
}

func TestBuildReassignmentsOnAnIdleControllerIsEmptyNotNull(t *testing.T) {
	// "Asked, nothing is moving" is a finding: it says the URP is a failure. The
	// section status carries it, so an empty list must not be mistaken for a
	// phase that did not run.
	if got := buildReassignments(kadm.ListPartitionReassignmentsResponses{}); len(got) != 0 {
		t.Errorf("reassignments = %+v, want empty", got)
	}
}
