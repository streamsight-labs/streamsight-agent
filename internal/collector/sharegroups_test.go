package collector

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// Below Kafka 4.0 the GroupType field does not exist, so nothing can ever match
// the share filter. That must read as "skipped", not as "ok, this cluster has no
// share groups" -- the second is a claim the agent is in no position to make.
func TestShareGroupsSkipWhenNoGroupIsTypedShare(t *testing.T) {
	c := &Collector{}

	for name, types := range map[string]map[string]string{
		"no types at all (pre-v5 broker)": {},
		"only classic groups":             {"orders": "classic", "events": "consumer"},
	} {
		got, sec := c.collectShareGroups(t.Context(), types, true)
		if got != nil || sec.status != metrics.SectionSkipped {
			t.Errorf("%s: got %v / %s, want nil / skipped", name, got, sec.status)
		}
	}
}

func TestShareGroupsSkipWhenDisabled(t *testing.T) {
	c := &Collector{}
	got, sec := c.collectShareGroups(t.Context(), map[string]string{"q": shareGroupType}, false)
	if got != nil || sec.status != metrics.SectionSkipped {
		t.Errorf("got %v / %s, want nil / skipped", got, sec.status)
	}
}

// A negative share-partition start offset is not an offset. It travels as null,
// exactly as every other offset in the batch does.
func TestBuildShareGroupsNullsNegativeStartOffsets(t *testing.T) {
	sec := newSection(sectionShareGroups)
	got := buildShareGroups(
		kadm.DescribedShareGroups{"q": {GroupID: "q", GroupState: "Stable"}},
		kadm.DescribedShareGroupsOffsets{"q": {Group: "q", Offsets: kadm.ShareOffsets{
			"orders": {
				0: {Topic: "orders", Partition: 0, StartOffset: 42},
				1: {Topic: "orders", Partition: 1, StartOffset: -1},
			},
		}}},
		sec,
	)

	if len(got) != 1 || len(got[0].StartOffsets) != 2 {
		t.Fatalf("got %+v", got)
	}
	if o := got[0].StartOffsets[0]; o.StartOffset == nil || *o.StartOffset != 42 {
		t.Errorf("partition 0 = %v, want 42", o.StartOffset)
	}
	if o := got[0].StartOffsets[1]; o.StartOffset != nil {
		t.Errorf("partition 1 = %d, want null: a negative offset is not an offset", *o.StartOffset)
	}
}

// A group the coordinator refuses to describe is still emitted, carrying its
// error code. A group that cannot be described is not a group that was deleted.
func TestBuildShareGroupsEmitsGroupsThatFailed(t *testing.T) {
	sec := newSection(sectionShareGroups)
	got := buildShareGroups(
		kadm.DescribedShareGroups{"q": {GroupID: "q", Err: kerr.GroupAuthorizationFailed}},
		nil, sec,
	)

	if len(got) != 1 {
		t.Fatalf("got %d groups, want the failed one emitted anyway", len(got))
	}
	if got[0].ErrorCode != kerr.GroupAuthorizationFailed.Code {
		t.Errorf("error code = %d, want %d", got[0].ErrorCode, kerr.GroupAuthorizationFailed.Code)
	}
}

// Output order is stable so a backend diffing consecutive batches does not see
// map iteration as cluster churn.
func TestBuildShareGroupsSortsDeterministically(t *testing.T) {
	sec := newSection(sectionShareGroups)
	for i := 0; i < 8; i++ {
		got := buildShareGroups(kadm.DescribedShareGroups{
			"zulu": {GroupID: "zulu"}, "alpha": {GroupID: "alpha"}, "mike": {GroupID: "mike"},
		}, nil, sec)
		if len(got) != 3 || got[0].ID != "alpha" || got[1].ID != "mike" || got[2].ID != "zulu" {
			t.Fatalf("unstable order: %+v", got)
		}
	}
}
