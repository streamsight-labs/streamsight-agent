package metrics

import (
	"encoding/json"
	"testing"
	"time"
)

func marshalMap(t *testing.T, v any) map[string]any {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

func TestSchemaVersionUnchanged(t *testing.T) {
	// Everything since v1 has been an added field, and no enum gained a value,
	// so a v1 consumer that ignores unknown keys is unaffected. Adding a
	// SectionStatus value is the change that would force a bump.
	if SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", SchemaVersion)
	}
}

func TestBatchOmitsTruncationWhenComplete(t *testing.T) {
	// Absence of `truncation` is the wire's promise that the batch is complete.
	m := marshalMap(t, Batch{SchemaVersion: SchemaVersion})
	if _, ok := m["truncation"]; ok {
		t.Error("truncation must be absent on a complete batch")
	}
	if _, ok := m["limits"]; ok {
		t.Error("limits must be absent when every cap is unlimited")
	}
}

func TestTruncationPresenceMarksIncompleteBatch(t *testing.T) {
	m := marshalMap(t, Batch{SchemaVersion: SchemaVersion, Truncation: &Truncation{Offsets: 3}})
	trunc, ok := m["truncation"].(map[string]any)
	if !ok {
		t.Fatalf("truncation = %v, want an object", m["truncation"])
	}
	if trunc["offsets"] != float64(3) {
		t.Errorf("truncation.offsets = %v, want 3", trunc["offsets"])
	}
	if _, ok := trunc["topics"]; ok {
		t.Error("zero counters must be omitted")
	}
}

func TestTruncationAdd(t *testing.T) {
	total := Truncation{Topics: 1}
	total.Add(Truncation{Topics: 2, Partitions: 3, Groups: 4, Members: 5, Offsets: 6, GroupStateTransitions: 9, ErrorsCollapsed: 7, ErrorsDropped: 8})
	want := Truncation{Topics: 3, Partitions: 3, Groups: 4, Members: 5, Offsets: 6, GroupStateTransitions: 9, ErrorsCollapsed: 7, ErrorsDropped: 8}
	if total != want {
		t.Errorf("Add() = %+v, want %+v", total, want)
	}
}

func TestCollectionErrorCountOmittedWhenSingular(t *testing.T) {
	// The collector only ever writes values >= 2, so "absent means one" holds on
	// the wire without a pointer.
	m := marshalMap(t, CollectionError{Section: "topics"})
	if _, ok := m["count"]; ok {
		t.Errorf("count = %v, want absent (absent means one)", m["count"])
	}
	m = marshalMap(t, CollectionError{Section: "topics", Count: 2})
	if m["count"] != float64(2) {
		t.Errorf("count = %v, want 2", m["count"])
	}
}

func TestOffsetCountAlwaysPresent(t *testing.T) {
	// len(offsets) < offset_count is the only per-entity truncation signal a
	// group has, so the key must never be omitted. Same for the other two.
	m := marshalMap(t, ConsumerOffset{GroupID: "payments"})
	v, ok := m["offset_count"]
	if !ok || v != float64(0) {
		t.Errorf("offset_count = %v (present=%v), want an explicit 0", v, ok)
	}

	for _, tc := range []struct {
		name string
		v    any
		key  string
	}{
		{name: "partition_count", v: TopicMetrics{Name: "orders"}, key: "partition_count"},
		{name: "member_count", v: GroupMetrics{ID: "payments"}, key: "member_count"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := marshalMap(t, tc.v)
			if got, ok := m[tc.key]; !ok || got != float64(0) {
				t.Errorf("%s = %v (present=%v), want an explicit 0", tc.key, got, ok)
			}
		})
	}
}

func TestPartitionOffsetsAreExplicitlyNullable(t *testing.T) {
	// Null means "not known this cycle", which a consumer must never read as 0:
	// a lag computed against a zeroed ceiling is the whole backlog.
	m := marshalMap(t, Partition{ID: 0})
	for _, key := range []string{"start_offset", "end_offset", "last_stable_offset"} {
		v, ok := m[key]
		if !ok {
			t.Errorf("%s absent, want an explicit null", key)
		}
		if v != nil {
			t.Errorf("%s = %v, want null", key, v)
		}
	}
}

func TestLogDirVolumeFiguresAreExplicitlyNull(t *testing.T) {
	// KIP-827's total/usable bytes are unreachable through kadm today. The keys
	// are on the wire now so filling them in later is a collector change, not a
	// schema change.
	m := marshalMap(t, LogDir{Broker: 1, Dir: "/data/1"})
	for _, key := range []string{"total_bytes", "usable_bytes"} {
		v, ok := m[key]
		if !ok || v != nil {
			t.Errorf("%s = %v (present=%v), want an explicit null", key, v, ok)
		}
	}
	// An offline directory reports its code and no partitions; a healthy empty
	// one reports neither. The two must not collapse into the same JSON.
	if _, ok := m["error_code"]; ok {
		t.Error("error_code must be absent on a healthy directory")
	}
	offline := marshalMap(t, LogDir{Broker: 1, Dir: "/data/1", ErrorCode: 56})
	if offline["error_code"] != float64(56) {
		t.Errorf("error_code = %v, want 56", offline["error_code"])
	}
}

func TestBatchOmitsLogDirsWhenNotCollected(t *testing.T) {
	// log_dirs runs on a slower cadence, so most batches carry none. The section
	// status says whether it ran; the key's absence must not be read as "the
	// cluster reported no storage".
	m := marshalMap(t, Batch{SchemaVersion: SchemaVersion})
	if _, ok := m["log_dirs"]; ok {
		t.Error("log_dirs must be absent when the phase did not run")
	}
}

func TestBatchOmitsOptionalBlocksWhenNotCollected(t *testing.T) {
	// Every Tier 1 addition is optional: a batch that ran none of the new phases
	// must be byte-identical to one from before they existed, or a v1 consumer
	// gains keys it never agreed to.
	m := marshalMap(t, Batch{SchemaVersion: SchemaVersion})
	for _, key := range []string{"throughput_window", "reassignments", "group_states", "epoch_probes", "principal"} {
		if _, ok := m[key]; ok {
			t.Errorf("%s present on a batch that collected none, want absent", key)
		}
	}
	agent, ok := m["agent"].(map[string]any)
	if !ok {
		t.Fatalf("agent = %v, want an object", m["agent"])
	}
	if _, ok := agent["rpc"]; ok {
		t.Error("agent.rpc must be absent when the hooks are disabled")
	}
	cluster, ok := m["cluster"].(map[string]any)
	if !ok {
		t.Fatalf("cluster = %v, want an object", m["cluster"])
	}
	for _, key := range []string{"capabilities", "authorized_operations"} {
		if _, ok := cluster[key]; ok {
			t.Errorf("cluster.%s present when not probed, want absent", key)
		}
	}
}

func TestAuthorizedOpsAbsentDiffersFromEmpty(t *testing.T) {
	// An older broker reports no bitfield at all; a broker with a bare DESCRIBE
	// grant can report one that permits nothing. "Cannot tell" and "denied" are
	// opposite answers on the onboarding page, so they must not share a JSON.
	unreported := marshalMap(t, TopicMetrics{Name: "orders"})
	if _, ok := unreported["authorized_operations"]; ok {
		t.Error("authorized_operations must be absent when the broker did not report it")
	}

	denied := marshalMap(t, TopicMetrics{Name: "orders", AuthorizedOperations: &AuthorizedOps{}})
	ops, ok := denied["authorized_operations"].(map[string]any)
	if !ok {
		t.Fatalf("authorized_operations = %v, want an object", denied["authorized_operations"])
	}
	if v, ok := ops["operations"]; !ok || v != nil {
		t.Errorf("operations = %v (present=%v), want an explicit null", v, ok)
	}
	if v, ok := ops["bitfield"]; !ok || v != nil {
		t.Errorf("bitfield = %v (present=%v), want an explicit null", v, ok)
	}
}

func TestWindowOffsetIsExplicitlyNullable(t *testing.T) {
	// A silent partition answers with the current end offset and no timestamp;
	// a failed lookup answers with neither. Both must be readable as null rather
	// than as a zero offset, which would read as "the whole log arrived".
	if _, ok := marshalMap(t, Partition{ID: 0})["window"]; ok {
		t.Error("window must be absent when the window phase did not run")
	}
	m := marshalMap(t, WindowOffset{LeaderEpoch: -1})
	for _, key := range []string{"offset", "timestamp_ms"} {
		if v, ok := m[key]; !ok || v != nil {
			t.Errorf("%s = %v (present=%v), want an explicit null", key, v, ok)
		}
	}
	if m["leader_epoch"] != float64(-1) {
		t.Errorf("leader_epoch = %v, want an explicit -1", m["leader_epoch"])
	}
}

func TestEpochProbeCarriesFullProofContext(t *testing.T) {
	// The proof is committed_offset > end_offset; every one of those keys must be
	// present and nullable, because a probe that failed proves nothing and must
	// not look like a probe that found zero.
	m := marshalMap(t, EpochProbe{Topic: "orders", GroupID: "payments", CommittedEpoch: 4, CurrentLeaderEpoch: 5})
	for _, key := range []string{"committed_offset", "returned_epoch", "end_offset", "node_id"} {
		if v, ok := m[key]; !ok || v != nil {
			t.Errorf("%s = %v (present=%v), want an explicit null", key, v, ok)
		}
	}
	if m["committed_epoch"] != float64(4) || m["current_leader_epoch"] != float64(5) {
		t.Errorf("epoch context = %v", m)
	}
}

func TestCapabilitiesDistinguishUnprobedFromUnsupported(t *testing.T) {
	// A broker that never answered must not read as a broker that supports
	// nothing: the first is a collection failure, the second is an upgrade ticket.
	m := marshalMap(t, BrokerCapability{BrokerID: 1})
	if v, ok := m["api_max_versions"]; !ok || v != nil {
		t.Errorf("api_max_versions = %v (present=%v), want an explicit null", v, ok)
	}
	if _, ok := m["software_version"]; ok {
		t.Error("software_version must be absent when the probe failed")
	}

	caps := marshalMap(t, ClusterCapabilities{
		Features:         map[string]bool{CapabilityLastStableOffset: false},
		SoftwareVersions: []string{"3.7", "4.0"},
		MixedVersions:    true,
	})
	features, ok := caps["features"].(map[string]any)
	if !ok {
		t.Fatalf("features = %v, want an object", caps["features"])
	}
	if features[CapabilityLastStableOffset] != false {
		t.Errorf("probed-and-unsupported must survive as an explicit false, got %v", features)
	}
	if caps["mixed_versions"] != true {
		t.Errorf("mixed_versions = %v, want true", caps["mixed_versions"])
	}
}

func TestLatencyHistogramIsAggregatable(t *testing.T) {
	// Counts and sums add across cycles; a percentile would not. Bucket bounds
	// travel so the backend interpolates the same way the agent would have.
	var h LatencyHistogram
	empty := marshalMap(t, h)
	if v, ok := empty["max_us"]; !ok || v != nil {
		t.Errorf("max_us = %v (present=%v), want an explicit null on an empty histogram", v, ok)
	}

	h.Observe(100 * time.Microsecond)
	h.Observe(3 * time.Millisecond)
	h.Observe(time.Hour)
	if h.Count != 3 {
		t.Errorf("Count = %d, want 3", h.Count)
	}
	if want := int64(100 + 3_000 + 3_600_000_000); h.SumUs != want {
		t.Errorf("SumUs = %d, want %d", h.SumUs, want)
	}
	if h.MaxUs == nil || *h.MaxUs != 3_600_000_000 {
		t.Errorf("MaxUs = %v, want the largest observation", h.MaxUs)
	}
	if len(h.Counts) != len(h.BoundsUs)+1 {
		t.Fatalf("len(Counts) = %d, want len(BoundsUs)+1 = %d", len(h.Counts), len(h.BoundsUs)+1)
	}
	if h.Counts[0] != 1 {
		t.Errorf("100us landed in bucket %v, want the first", h.Counts)
	}
	if h.Counts[len(h.Counts)-1] != 1 {
		t.Errorf("an hour must land in the overflow bucket, got %v", h.Counts)
	}
	var total int64
	for _, c := range h.Counts {
		total += c
	}
	if total != h.Count {
		t.Errorf("buckets sum to %d, want Count = %d", total, h.Count)
	}
}

func TestGroupStateWindowKeepsTruePreCapCount(t *testing.T) {
	// A rebalance storm is exactly when the transition list gets capped, so the
	// count must stay true or the storm reads as calm.
	m := marshalMap(t, GroupStateWindow{GroupID: "payments", TransitionCount: 42})
	if m["transition_count"] != float64(42) {
		t.Errorf("transition_count = %v, want 42", m["transition_count"])
	}
	for _, key := range []string{"state_at_start", "state_at_end"} {
		if v, ok := m[key]; !ok || v != "" {
			t.Errorf("%s = %v (present=%v), want an explicit empty string for a pre-2.6 broker", key, v, ok)
		}
	}
}

func TestSectionOmitsTruncationFieldsWhenUntruncated(t *testing.T) {
	m := marshalMap(t, Section{Name: "topics", Status: SectionOK})
	for _, key := range []string{"error_count", "errors_collapsed", "errors_dropped", "truncated"} {
		if _, ok := m[key]; ok {
			t.Errorf("%s present on a clean section, want absent", key)
		}
	}
	m = marshalMap(t, Section{Name: "topics", Status: SectionPartial, ErrorsCollapsed: 4, ErrorsDropped: 1, Truncated: true})
	if m["errors_collapsed"] != float64(4) || m["errors_dropped"] != float64(1) || m["truncated"] != true {
		t.Errorf("truncated section = %v", m)
	}
}
