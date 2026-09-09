package metrics

import (
	"encoding/json"
	"slices"
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
	// v1 is still being shaped: the agent is unreleased and nothing consumes a
	// batch it did not produce, so fields have both arrived and left under this
	// version. The bump matters from the first release onwards, and TestSchemaV1Frozen
	// is what makes any change to the shape a deliberate one until then.
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
	if _, ok := m["selection"]; ok {
		t.Error("selection must be absent when nothing narrows the view")
	}
}

func TestSelectionPresenceMarksANarrowedView(t *testing.T) {
	// Presence is the whole signal: a filtered entity leaves no other trace in
	// the payload, so this block is the only thing that can tell a backend the
	// batch is not the whole cluster.
	m := marshalMap(t, Batch{SchemaVersion: SchemaVersion,
		Selection: &Selection{TopicExclude: []string{"shadow.audit", "/^shadow-/"}}})
	sel, ok := m["selection"].(map[string]any)
	if !ok {
		t.Fatalf("selection = %v, want an object", m["selection"])
	}
	// The entries travel exactly as configured -- a literal unescaped, a regex
	// still wrapped -- because what the operator wrote is what a backend can
	// show them and what they can diff against their own deployment.
	got, ok := sel["topic_exclude"].([]any)
	if !ok {
		t.Fatalf("selection.topic_exclude = %v, want an array", sel["topic_exclude"])
	}
	want := []any{"shadow.audit", "/^shadow-/"}
	if len(got) != len(want) {
		t.Fatalf("selection.topic_exclude = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selection.topic_exclude = %v, want %v", got, want)
		}
	}
	if _, ok := sel["topic_include"]; ok {
		t.Error("unset filters must be omitted")
	}
}

func TestTruncationPresenceMarksIncompleteBatch(t *testing.T) {
	m := marshalMap(t, Batch{SchemaVersion: SchemaVersion, Truncation: &Truncation{ErrorsDropped: 3}})
	trunc, ok := m["truncation"].(map[string]any)
	if !ok {
		t.Fatalf("truncation = %v, want an object", m["truncation"])
	}
	if trunc["errors_dropped"] != float64(3) {
		t.Errorf("truncation.errors_dropped = %v, want 3", trunc["errors_dropped"])
	}
	if _, ok := trunc["errors_collapsed"]; ok {
		t.Error("zero counters must be omitted")
	}
}

func TestTruncationAdd(t *testing.T) {
	total := Truncation{ErrorsCollapsed: 1}
	total.Add(Truncation{ErrorsCollapsed: 7, ErrorsDropped: 8})
	want := Truncation{ErrorsCollapsed: 8, ErrorsDropped: 8}
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
	for _, key := range []string{"throughput_window", "reassignments", "epoch_probes", "topic_configs", "broker_configs", "share_groups", "principal"} {
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
	// Cluster is absent, not an empty object: a zero ClusterMetrics would ship
	// broker_count 0, which is a legal value and so indistinguishable from a
	// measurement. See Batch.Cluster.
	if _, ok := m["cluster"]; ok {
		t.Errorf("cluster present on a batch whose metadata request failed, want absent")
	}

	// When the phase DID run, the object is there and capabilities stays absent
	// until the probe fills it.
	withCluster := marshalMap(t, Batch{SchemaVersion: SchemaVersion, Cluster: &ClusterMetrics{ID: "abc"}})
	cluster, ok := withCluster["cluster"].(map[string]any)
	if !ok {
		t.Fatalf("cluster = %v, want an object", withCluster["cluster"])
	}
	if _, ok := cluster["capabilities"]; ok {
		t.Error("cluster.capabilities present when not probed, want absent")
	}
	if _, ok := cluster["controller"]; ok {
		t.Error("cluster.controller is not a field: on KRaft it is a random alive broker")
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

// TestHistogramsDoNotShareBucketBounds pins the isolation EnsureBuckets exists
// to provide: a histogram may only ever be re-bucketed through itself.
//
// EnsureBuckets assigned the package-level default slice straight into
// h.BoundsUs, so every histogram the agent emitted -- one per broker row and two
// per API row, installed fresh by internal/kafka's snapshot on every cycle --
// aliased ONE backing array. A single element write through any one of them
// moved the layout for all of them, for every histogram created afterwards, and
// for every batch after that. The payload gave no sign of it: bounds_us still
// shipped, counts still summed to count, and the only symptom was a backend
// interpolating against bounds the observations were never bucketed by.
func TestHistogramsDoNotShareBucketBounds(t *testing.T) {
	var a, b LatencyHistogram
	a.EnsureBuckets()
	b.EnsureBuckets()
	want := slices.Clone(b.BoundsUs)

	a.BoundsUs[0] = 999_999_999

	if !slices.Equal(b.BoundsUs, want) {
		t.Errorf("a write through one histogram re-bucketed another: %v, want %v", b.BoundsUs, want)
	}
	var c LatencyHistogram
	c.EnsureBuckets()
	if !slices.Equal(c.BoundsUs, want) {
		t.Errorf("a histogram created after the write inherited the mutated layout: %v, want %v", c.BoundsUs, want)
	}
	// The untouched histogram must still BUCKET by the untouched layout, not
	// merely report it. Observe walks h.BoundsUs, so a moved first bound would
	// have swallowed every observation below it into bucket 0.
	b.Observe(600 * time.Microsecond)
	if b.Counts[1] != 1 {
		t.Errorf("600us landed in %v, want the second bucket of the default layout", b.Counts)
	}
}

// TestDefaultLatencyBoundsUsHandsOutCopies covers the other half of the same
// invariant. The accessor is the only way the layout leaves this package, so it
// must not hand out its source: a caller that stored the bounds once per batch
// and then normalised or sorted them in place would otherwise move the agent's
// buckets for the rest of the process.
func TestDefaultLatencyBoundsUsHandsOutCopies(t *testing.T) {
	got := DefaultLatencyBoundsUs()
	got[0] = -1
	if again := DefaultLatencyBoundsUs(); again[0] == -1 {
		t.Errorf("the accessor handed out its source: %v", again)
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
