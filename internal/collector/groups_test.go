package collector

import (
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

func strptr(s string) *string { return &s }

func TestApplyConsumerGroupFillsEpochsAndAssignment(t *testing.T) {
	gm := metrics.GroupMetrics{
		ID:          "orders",
		State:       "Stable",
		Coordinator: 1,
		Generation:  -1,
		MemberCount: 1,
		Members: []metrics.GroupMember{{
			MemberID:   "m-1",
			ClientID:   "svc",
			Host:       "10.0.0.1",
			Assignment: []metrics.TopicPartition{},
		}},
	}

	applyConsumerGroup(&gm, kadm.DescribedConsumerGroup{
		Group:           "orders",
		State:           "Stable",
		Epoch:           42,
		AssignmentEpoch: 41,
		AssignorName:    "uniform",
		Members: []kadm.ConsumerGroupMember{{
			MemberID:             "m-1",
			MemberEpoch:          42,
			SubscribedTopics:     []string{"orders"},
			SubscribedTopicRegex: strptr("^ord.*"),
			Assignment:           kadm.TopicsSet{"orders": {0: {}, 1: {}}},
			TargetAssignment:     kadm.TopicsSet{"orders": {0: {}, 1: {}, 2: {}}},
		}},
	})

	if gm.GroupEpoch == nil || *gm.GroupEpoch != 42 {
		t.Errorf("GroupEpoch = %v, want 42", gm.GroupEpoch)
	}
	if gm.AssignmentEpoch == nil || *gm.AssignmentEpoch != 41 {
		t.Errorf("AssignmentEpoch = %v, want 41", gm.AssignmentEpoch)
	}
	if gm.Assignor != "uniform" {
		t.Errorf("Assignor = %q, want uniform", gm.Assignor)
	}

	m := gm.Members[0]
	if m.MemberEpoch == nil || *m.MemberEpoch != 42 {
		t.Errorf("MemberEpoch = %v, want 42", m.MemberEpoch)
	}
	if m.SubscribedTopicRegex == nil || *m.SubscribedTopicRegex != "^ord.*" {
		t.Errorf("SubscribedTopicRegex = %v", m.SubscribedTopicRegex)
	}
	// The classic describe decodes no assignment for a new-protocol member, so
	// the overlay is the only thing that can populate it.
	want := []metrics.TopicPartition{{Topic: "orders", Partition: 0}, {Topic: "orders", Partition: 1}}
	if !reflect.DeepEqual(m.Assignment, want) {
		t.Errorf("Assignment = %+v, want %+v", m.Assignment, want)
	}
	if len(m.TargetAssignment) != 3 {
		t.Errorf("TargetAssignment = %+v, want 3 partitions", m.TargetAssignment)
	}
	// Target ahead of current is the new-protocol signal for a group
	// mid-reconciliation, replacing the cooperative owned-vs-assigned comparison.
	if len(m.TargetAssignment) <= len(m.Assignment) {
		t.Error("target assignment should exceed the current one in this fixture")
	}
}

// The classic describe stays authoritative for a classic group; the new call
// returns it with an error.
func TestApplyConsumerGroupDoesNotClobberClassicData(t *testing.T) {
	classic := []metrics.TopicPartition{{Topic: "orders", Partition: 7}}
	gm := metrics.GroupMetrics{
		ID:         "legacy",
		Generation: 9,
		Members: []metrics.GroupMember{{
			MemberID:         "m-1",
			SubscribedTopics: []string{"orders"},
			Assignment:       classic,
		}},
	}

	// Same member ID, but the coordinator reports nothing useful for it.
	applyConsumerGroup(&gm, kadm.DescribedConsumerGroup{
		Group:   "legacy",
		Members: []kadm.ConsumerGroupMember{{MemberID: "m-1"}},
	})

	if !reflect.DeepEqual(gm.Members[0].Assignment, classic) {
		t.Errorf("Assignment = %+v, want the classic value %+v", gm.Members[0].Assignment, classic)
	}
	if got := gm.Members[0].SubscribedTopics; !reflect.DeepEqual(got, []string{"orders"}) {
		t.Errorf("SubscribedTopics = %v, want the classic value", got)
	}
	if gm.Generation != 9 {
		t.Errorf("Generation = %d, want the classic value 9", gm.Generation)
	}
}

// The classic describe decides which members exist. A member it did not return
// must not reappear through the KIP-848 overlay, or the two describes would
// disagree about the membership of the same group.
func TestApplyConsumerGroupRespectsTheClassicMemberList(t *testing.T) {
	gm := metrics.GroupMetrics{
		ID:          "orders",
		MemberCount: 3, // pre-truncation count
		Members:     []metrics.GroupMember{{MemberID: "m-1"}},
	}

	applyConsumerGroup(&gm, kadm.DescribedConsumerGroup{
		Group: "orders",
		Members: []kadm.ConsumerGroupMember{
			{MemberID: "m-1", MemberEpoch: 5},
			{MemberID: "m-2", MemberEpoch: 5},
			{MemberID: "m-3", MemberEpoch: 5},
		},
	})

	if len(gm.Members) != 1 {
		t.Fatalf("members = %d, want the classic describe's 1", len(gm.Members))
	}
	if gm.MemberCount != 3 {
		t.Errorf("MemberCount = %d, want the count it was built with, 3", gm.MemberCount)
	}
}

// Epoch 0 is a legitimate value for a brand new group, so it must serialise as
// 0 rather than being indistinguishable from "not collected".
func TestApplyConsumerGroupZeroEpochIsNotAbsent(t *testing.T) {
	gm := metrics.GroupMetrics{ID: "fresh"}
	applyConsumerGroup(&gm, kadm.DescribedConsumerGroup{Group: "fresh", Epoch: 0, AssignmentEpoch: 0})

	if gm.GroupEpoch == nil {
		t.Fatal("GroupEpoch is nil for a group whose epoch is genuinely 0")
	}
	if *gm.GroupEpoch != 0 {
		t.Errorf("GroupEpoch = %d, want 0", *gm.GroupEpoch)
	}
}

// kadm.TopicsSet is a map, so without sorting the wire order would follow Go's
// randomised iteration and every cycle would look like the assignment churned.
func TestTopicsSetToPartitionsIsDeterministic(t *testing.T) {
	ts := kadm.TopicsSet{
		"payments": {2: {}, 0: {}, 1: {}},
		"orders":   {1: {}, 0: {}},
	}

	want := []metrics.TopicPartition{
		{Topic: "orders", Partition: 0},
		{Topic: "orders", Partition: 1},
		{Topic: "payments", Partition: 0},
		{Topic: "payments", Partition: 1},
		{Topic: "payments", Partition: 2},
	}

	for i := 0; i < 20; i++ {
		if got := topicsSetToPartitions(ts); !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: %+v, want %+v", i, got, want)
		}
	}

	if topicsSetToPartitions(nil) != nil {
		t.Error("an empty set must produce nil, not an empty slice")
	}
}

// The collector here has a nil client, so a regression that reaches the Kafka
// call panics rather than silently costing a request per cycle.
func TestEnrichConsumerGroupsSkippedWhenDisabled(t *testing.T) {
	c := &Collector{opts: Options{CollectConsumerGroups: false}}
	sec := newSection(sectionGroups)
	groups := []metrics.GroupMetrics{{ID: "orders"}}

	c.enrichConsumerGroups(t.Context(), sec, groups)

	if groups[0].GroupEpoch != nil {
		t.Error("epoch populated while the phase is disabled")
	}
	if len(sec.errs) != 0 {
		t.Errorf("errors recorded while disabled: %+v", sec.errs)
	}
}

// Enabled but with nothing to describe must also stay off the wire.
func TestEnrichConsumerGroupsSkippedWithNoGroups(t *testing.T) {
	c := &Collector{opts: Options{CollectConsumerGroups: true}}
	sec := newSection(sectionGroups)

	c.enrichConsumerGroups(t.Context(), sec, nil)

	if len(sec.errs) != 0 {
		t.Errorf("errors recorded with no groups: %+v", sec.errs)
	}
}
