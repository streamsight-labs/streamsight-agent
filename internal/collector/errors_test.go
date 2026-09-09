package collector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantKind string
		wantCode int16
	}{
		{name: "nil", err: nil, wantKind: "", wantCode: 0},
		{name: "topic authorization", err: kerr.TopicAuthorizationFailed, wantKind: kindAuthorization, wantCode: 29},
		{name: "group authorization", err: kerr.GroupAuthorizationFailed, wantKind: kindAuthorization, wantCode: 30},
		{name: "cluster authorization", err: kerr.ClusterAuthorizationFailed, wantKind: kindAuthorization, wantCode: 31},
		{name: "kadm auth wrapper", err: &kadm.AuthError{Err: kerr.GroupAuthorizationFailed}, wantKind: kindAuthorization, wantCode: 30},
		{name: "not coordinator", err: kerr.NotCoordinator, wantKind: kindCoordinator, wantCode: 16},
		{name: "coordinator loading", err: kerr.CoordinatorLoadInProgress, wantKind: kindCoordinator, wantCode: 14},
		{name: "coordinator unavailable", err: kerr.CoordinatorNotAvailable, wantKind: kindCoordinator, wantCode: 15},
		{name: "unsupported version", err: kerr.UnsupportedVersion, wantKind: kindUnsupported, wantCode: 35},
		{name: "leader not available is not special", err: kerr.LeaderNotAvailable, wantKind: kindOther, wantCode: 5},
		{name: "deadline", err: context.DeadlineExceeded, wantKind: kindTransport, wantCode: 0},
		{name: "canceled", err: context.Canceled, wantKind: kindTransport, wantCode: 0},
		{name: "wrapped deadline", err: errors.Join(errors.New("dial"), context.DeadlineExceeded), wantKind: kindTransport, wantCode: 0},
		{name: "net error", err: &net.OpError{Op: "dial", Err: errors.New("refused")}, wantKind: kindTransport, wantCode: 0},
		{name: "plain error", err: errors.New("boom"), wantKind: kindOther, wantCode: 0},
		{name: "wrapped kafka error", err: errors.New("x: " + kerr.NotCoordinator.Error()), wantKind: kindOther, wantCode: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, code := classify(tt.err)
			if kind != tt.wantKind {
				t.Errorf("kind = %q, want %q", kind, tt.wantKind)
			}
			if code != tt.wantCode {
				t.Errorf("code = %d, want %d", code, tt.wantCode)
			}
		})
	}
}

func TestErrorCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int16
	}{
		{name: "nil", err: nil, want: 0},
		{name: "kafka error", err: kerr.NotCoordinator, want: 16},
		{name: "wrapped kafka error", err: errors.Join(errors.New("ctx"), kerr.GroupAuthorizationFailed), want: 30},
		{name: "non kafka error", err: errors.New("boom"), want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorCode(tt.err); got != tt.want {
				t.Errorf("errorCode() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSectionRequest(t *testing.T) {
	nodeID := func(id int32) kadm.BrokerDetail { return kgo.BrokerMetadata{NodeID: id} }

	tests := []struct {
		name       string
		err        error
		wantUsable bool
		wantStatus metrics.SectionStatus
		wantErrs   int
		wantBroker *int32
	}{
		{
			name:       "success",
			err:        nil,
			wantUsable: true,
			wantStatus: metrics.SectionOK,
		},
		{
			name:       "one shard of many failed keeps the rest",
			err:        &kadm.ShardErrors{Name: "ListGroups", AllFailed: false, Errs: []kadm.ShardError{{Err: errors.New("dial tcp: refused"), Broker: nodeID(7)}}},
			wantUsable: true,
			wantStatus: metrics.SectionPartial,
			wantErrs:   1,
			wantBroker: int32Ptr(7),
		},
		{
			name:       "every shard failed",
			err:        &kadm.ShardErrors{Name: "ListGroups", AllFailed: true, Errs: []kadm.ShardError{{Err: errors.New("dial"), Broker: nodeID(1)}, {Err: errors.New("dial"), Broker: nodeID(2)}}},
			wantUsable: false,
			wantStatus: metrics.SectionFailed,
			wantErrs:   2,
			wantBroker: int32Ptr(1),
		},
		{
			name:       "unmapped shard has no broker attribution",
			err:        &kadm.ShardErrors{Name: "ListGroups", Errs: []kadm.ShardError{{Err: errors.New("no broker"), Broker: nodeID(-1)}}},
			wantUsable: true,
			wantStatus: metrics.SectionPartial,
			wantErrs:   1,
			wantBroker: nil,
		},
		{
			// The ShardErrors-before-AuthError ordering in request(): kadm's
			// Unwrap makes errors.As match an AuthError inside a shard, and
			// testing it first would discard the shards that answered.
			name:       "one denied shard keeps the shards that answered",
			err:        &kadm.ShardErrors{Name: "ListGroups", Errs: []kadm.ShardError{{Err: kerr.GroupAuthorizationFailed, Broker: nodeID(3)}}},
			wantUsable: true,
			wantStatus: metrics.SectionPartial,
			wantErrs:   1,
			wantBroker: int32Ptr(3),
		},
		{
			// Only when every shard was denied is there genuinely no data, which
			// is what "unauthorized" promises a consumer.
			name:       "every shard denied is unauthorized",
			err:        &kadm.ShardErrors{Name: "ListGroups", AllFailed: true, Errs: []kadm.ShardError{{Err: kerr.GroupAuthorizationFailed, Broker: nodeID(3)}}},
			wantUsable: false,
			wantStatus: metrics.SectionUnauthorized,
			wantErrs:   1,
			wantBroker: int32Ptr(3),
		},
		{
			name:       "kadm auth error",
			err:        &kadm.AuthError{Err: kerr.TopicAuthorizationFailed},
			wantUsable: false,
			wantStatus: metrics.SectionUnauthorized,
			wantErrs:   1,
		},
		{
			name:       "plain request failure",
			err:        errors.New("connection reset"),
			wantUsable: false,
			wantStatus: metrics.SectionFailed,
			wantErrs:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sec := newSection(sectionGroups)
			usable := sec.request("ListGroups", tt.err)

			if usable != tt.wantUsable {
				t.Errorf("usable = %v, want %v", usable, tt.wantUsable)
			}
			if sec.status != tt.wantStatus {
				t.Errorf("status = %q, want %q", sec.status, tt.wantStatus)
			}
			if len(sec.errs) != tt.wantErrs {
				t.Fatalf("errors = %d, want %d", len(sec.errs), tt.wantErrs)
			}
			if tt.wantErrs == 0 {
				return
			}
			got := sec.errs[0]
			if got.Section != sectionGroups {
				t.Errorf("Section = %q, want %q", got.Section, sectionGroups)
			}
			if got.API != "ListGroups" {
				t.Errorf("API = %q, want ListGroups", got.API)
			}
			switch {
			case tt.wantBroker == nil && got.BrokerID != nil:
				t.Errorf("BrokerID = %d, want nil", *got.BrokerID)
			case tt.wantBroker != nil && got.BrokerID == nil:
				t.Errorf("BrokerID = nil, want %d", *tt.wantBroker)
			case tt.wantBroker != nil && *got.BrokerID != *tt.wantBroker:
				t.Errorf("BrokerID = %d, want %d", *got.BrokerID, *tt.wantBroker)
			}
		})
	}
}

func TestSectionStatusOnlyDowngrades(t *testing.T) {
	sec := newSection(sectionOffsets)
	if sec.status != metrics.SectionOK {
		t.Fatalf("new section status = %q, want ok", sec.status)
	}

	sec.recordGroup("OffsetFetch", "app", kerr.NotCoordinator)
	if sec.status != metrics.SectionPartial {
		t.Fatalf("status = %q, want partial", sec.status)
	}

	sec.downgrade(metrics.SectionOK)
	if sec.status != metrics.SectionPartial {
		t.Fatalf("status = %q after ok downgrade, want partial", sec.status)
	}

	sec.downgrade(metrics.SectionFailed)
	sec.downgrade(metrics.SectionPartial)
	if sec.status != metrics.SectionFailed {
		t.Fatalf("status = %q, want failed", sec.status)
	}

	sec.downgrade(metrics.SectionUnauthorized)
	if sec.status != metrics.SectionUnauthorized {
		t.Fatalf("status = %q, want unauthorized", sec.status)
	}
}

func TestSectionAttribution(t *testing.T) {
	sec := newSection(sectionOffsets)
	sec.recordGroup("OffsetFetch", "payments", kerr.NotCoordinator)
	sec.recordPartition("OffsetFetch", "orders", 4, kerr.LeaderNotAvailable)
	sec.recordTopic("ListEndOffsets", "orders", errTopicNotListed)

	if len(sec.errs) != 3 {
		t.Fatalf("errors = %d, want 3", len(sec.errs))
	}
	if sec.errs[0].Group != "payments" || sec.errs[0].Code != 16 || sec.errs[0].Kind != kindCoordinator {
		t.Errorf("group error = %+v", sec.errs[0])
	}
	if sec.errs[1].Topic != "orders" || sec.errs[1].Partition == nil || *sec.errs[1].Partition != 4 {
		t.Errorf("partition error = %+v", sec.errs[1])
	}
	if sec.errs[2].Topic != "orders" || sec.errs[2].Partition != nil {
		t.Errorf("topic error = %+v", sec.errs[2])
	}

	sec.stop()
	s := sec.finish()
	if s.Name != sectionOffsets || s.ErrorCount != 3 || s.Status != metrics.SectionPartial {
		t.Errorf("finish() = %+v", s)
	}
	if s.SampledAt.IsZero() {
		t.Error("SampledAt must be stamped at phase start")
	}
}

func TestNilSectionFinishes(t *testing.T) {
	var sec *section
	got := sec.finish()
	if got.Status != metrics.SectionSkipped {
		t.Errorf("nil section status = %q, want skipped", got.Status)
	}
	if got.ErrorCount != 0 || got.ErrorsCollapsed != 0 || got.ErrorsDropped != 0 || got.Truncated {
		t.Errorf("nil section must report no truncation, got %+v", got)
	}
}

func TestSectionDedupCollapsesIdenticalErrors(t *testing.T) {
	sec := newSection(sectionTopics)
	for i := 0; i < 4000; i++ {
		sec.recordPartition("Metadata", "orders", int32(i), kerr.NotLeaderForPartition)
	}

	if len(sec.errs) != 1 {
		t.Fatalf("emitted %d entries, want 1: identical failures must collapse", len(sec.errs))
	}
	got := sec.errs[0]
	if got.Count != 4000 {
		t.Errorf("Count = %d, want 4000", got.Count)
	}
	// The exemplar is the FIRST occurrence, not an arbitrary one.
	if got.Partition == nil || *got.Partition != 0 {
		t.Errorf("exemplar partition = %v, want the first occurrence (0)", got.Partition)
	}
	if sec.dropped.ErrorsCollapsed != 3999 {
		t.Errorf("collapsed = %d, want 3999", sec.dropped.ErrorsCollapsed)
	}
	if sec.dropped.ErrorsDropped != 0 {
		t.Errorf("dropped = %d, want 0: collapsing loses no occurrence", sec.dropped.ErrorsDropped)
	}
	if !sec.truncated {
		t.Error("a section whose errors were collapsed must report itself truncated")
	}

	sec.stop()
	if s := sec.finish(); s.ErrorCount != 1 || s.ErrorsCollapsed != 3999 || !s.Truncated {
		t.Errorf("finish() = %+v", s)
	}
}

func TestSectionDedupSeparatesByBroker(t *testing.T) {
	// Shard errors carry the only per-broker attribution on the wire, so
	// broker_id must be part of the dedup key.
	sec := newSection(sectionGroups)
	sec.request("ListGroups", &kadm.ShardErrors{
		Name: "ListGroups",
		Errs: []kadm.ShardError{
			{Err: errors.New("dial tcp: refused"), Broker: kgo.BrokerMetadata{NodeID: 1}},
			{Err: errors.New("dial tcp: refused"), Broker: kgo.BrokerMetadata{NodeID: 2}},
			{Err: errors.New("dial tcp: refused"), Broker: kgo.BrokerMetadata{NodeID: 1}},
		},
	})

	if len(sec.errs) != 2 {
		t.Fatalf("emitted %d entries, want 2 (one per broker)", len(sec.errs))
	}
	if sec.errs[0].BrokerID == nil || *sec.errs[0].BrokerID != 1 {
		t.Errorf("first entry broker = %v, want 1", sec.errs[0].BrokerID)
	}
	if sec.errs[1].BrokerID == nil || *sec.errs[1].BrokerID != 2 {
		t.Errorf("second entry broker = %v, want 2", sec.errs[1].BrokerID)
	}
	if sec.errs[0].Count != 2 {
		t.Errorf("broker 1 Count = %d, want 2", sec.errs[0].Count)
	}
}

func TestSectionDedupSeparatesUnattributedFromBrokerZero(t *testing.T) {
	// An unmapped shard has no broker; it must not fold into broker 0's entry.
	sec := newSection(sectionGroups)
	sec.request("ListGroups", &kadm.ShardErrors{
		Name: "ListGroups",
		Errs: []kadm.ShardError{
			{Err: errors.New("no broker"), Broker: kgo.BrokerMetadata{NodeID: -1}},
			{Err: errors.New("no broker"), Broker: kgo.BrokerMetadata{NodeID: 0}},
		},
	})
	if len(sec.errs) != 2 {
		t.Fatalf("emitted %d entries, want 2", len(sec.errs))
	}
}

func TestSectionDedupCountsSumToOccurrences(t *testing.T) {
	sec := newSection(sectionOffsets)
	sec.lim = Limits{MaxErrorSamples: 3}
	for i := 0; i < 10; i++ {
		sec.recordPartition("OffsetFetch", "orders", int32(i), kerr.NotLeaderForPartition)
	}

	if len(sec.errs) != 3 {
		t.Fatalf("emitted %d entries, want 3 verbatim samples", len(sec.errs))
	}
	total := 0
	for _, ce := range sec.errs {
		if ce.Count == 0 { // absent means one
			total++
			continue
		}
		total += ce.Count
	}
	if total != 10 {
		t.Errorf("Counts sum to %d, want 10: no occurrence may be unaccounted for", total)
	}
	// The exemplar absorbs everything the later samples do not.
	if sec.errs[0].Count != 8 || sec.errs[1].Count != 0 || sec.errs[2].Count != 0 {
		t.Errorf("counts = %d, %d, %d; want 8, 0, 0", sec.errs[0].Count, sec.errs[1].Count, sec.errs[2].Count)
	}
}

func TestSectionStatusDowngradesEvenWhenErrorCollapsed(t *testing.T) {
	// The obvious implementation returns early from the dedup funnel before
	// downgrading, which would make a cluster with every partition offline
	// report five "ok" sections.
	sec := newSection(sectionTopics)
	sec.lim = Limits{MaxErrors: 1}
	for i := 0; i < 4001; i++ {
		sec.recordPartition("Metadata", "orders", int32(i), kerr.NotLeaderForPartition)
	}
	if sec.status != metrics.SectionPartial {
		t.Fatalf("status = %q, want partial", sec.status)
	}

	// Same again where every occurrence is REFUSED rather than collapsed: the
	// budget is already full of a different key.
	sec = newSection(sectionTopics)
	sec.lim = Limits{MaxErrors: 1}
	sec.recordGroup("DescribeGroups", "payments", kerr.NotCoordinator)
	before := len(sec.errs)
	for i := 0; i < 100; i++ {
		sec.recordPartition("Metadata", "orders", int32(i), kerr.NotLeaderForPartition)
	}
	if len(sec.errs) != before {
		t.Fatalf("emitted %d entries, want %d: the budget was already full", len(sec.errs), before)
	}
	// One ENTRY was refused, not 100: all 100 share a dedup key, so after the
	// first refusal the rest are recognised and folded away. Counting
	// occurrences would report 100 lost entries when only one distinct failure
	// was ever going to be emitted.
	if sec.dropped.ErrorsDropped != 1 {
		t.Errorf("dropped = %d, want 1 refused entry", sec.dropped.ErrorsDropped)
	}
	if sec.status != metrics.SectionPartial {
		t.Fatalf("status = %q after 100 dropped failures, want partial", sec.status)
	}
}

func TestSectionRequestScopedErrorsBypassTheEntityBudget(t *testing.T) {
	// A whole-API failure has no per-entity redundancy: if the budget were
	// allowed to refuse it, the reason the section is empty would vanish.
	sec := newSection(sectionGroups)
	sec.lim = Limits{MaxErrors: 1}
	for i := 0; i < 50; i++ {
		sec.recordPartition("Metadata", "orders", int32(i), kerr.NotLeaderForPartition)
	}
	if len(sec.errs) != 1 {
		t.Fatalf("entity errors = %d, want the budget of 1", len(sec.errs))
	}

	sec.request("DescribeGroups", &kadm.AuthError{Err: kerr.GroupAuthorizationFailed})
	if len(sec.errs) != 2 {
		t.Fatalf("emitted %d entries, want the request error admitted despite the full budget", len(sec.errs))
	}
	if sec.errs[1].Kind != kindAuthorization {
		t.Errorf("second entry = %+v, want the authorization failure", sec.errs[1])
	}
	if sec.status != metrics.SectionUnauthorized {
		t.Errorf("status = %q, want unauthorized", sec.status)
	}
}

func TestSectionDistinctKeyCeiling(t *testing.T) {
	// A broker inventing error codes must not turn the dedup map into the
	// unbounded thing dedup was added to prevent.
	sec := newSection(sectionTopics)
	for i := 0; i < maxDistinctErrorKeys+50; i++ {
		// The API name is part of the key, so every iteration is a new one.
		sec.recordTopic(fmt.Sprintf("Api%d", i), "orders", kerr.NotLeaderForPartition)
	}
	// Refused entity keys are remembered so repeat occurrences are not counted
	// as fresh refusals, but only up to the ceiling.
	if len(sec.keys) > maxDistinctErrorKeys {
		t.Fatalf("dedup map holds %d keys, want at most %d", len(sec.keys), maxDistinctErrorKeys)
	}
	if sec.dropped.ErrorsDropped != 50 {
		t.Errorf("dropped = %d, want 50", sec.dropped.ErrorsDropped)
	}
	if sec.status != metrics.SectionPartial {
		t.Errorf("status = %q, want partial", sec.status)
	}
}

func int32Ptr(v int32) *int32 { return &v }
