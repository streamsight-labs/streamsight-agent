package metrics

import (
	"encoding/json"
	"testing"
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
	total.Add(Truncation{Topics: 2, Partitions: 3, Groups: 4, Members: 5, Offsets: 6, ErrorsCollapsed: 7, ErrorsDropped: 8})
	want := Truncation{Topics: 3, Partitions: 3, Groups: 4, Members: 5, Offsets: 6, ErrorsCollapsed: 7, ErrorsDropped: 8}
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
