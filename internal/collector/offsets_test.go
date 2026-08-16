package collector

import (
	"encoding/json"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"kafka-metrics-agent/internal/metrics"
)

func TestNullableOffset(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		err  error
		want *int64
	}{
		{name: "committed offset", in: 1234, want: int64Ptr(1234)},
		{name: "committed at start of log", in: 0, want: int64Ptr(0)},
		{name: "never committed is null, not zero", in: -1, want: nil},
		{name: "any negative sentinel is null", in: -2, want: nil},
		{name: "error wins over a plausible value", in: 500, err: kerr.NotCoordinator, want: nil},
		{name: "error and negative", in: -1, err: kerr.LeaderNotAvailable, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nullableOffset(tt.in, tt.err)
			switch {
			case tt.want == nil && got != nil:
				t.Fatalf("got %d, want nil", *got)
			case tt.want != nil && got == nil:
				t.Fatalf("got nil, want %d", *tt.want)
			case tt.want != nil && *got != *tt.want:
				t.Fatalf("got %d, want %d", *got, *tt.want)
			}
		})
	}
}

func TestNullableOffsetCopiesValue(t *testing.T) {
	// The pointer must not alias a loop variable or a caller's field.
	v := int64(7)
	got := nullableOffset(v, nil)
	v = 9
	if *got != 7 {
		t.Errorf("got %d, want 7 — the offset aliased its source", *got)
	}
}

func TestNullOffsetsSerializeAsNull(t *testing.T) {
	// The wire contract: a failed lookup must be visibly absent, not 0.
	po := metrics.PartitionOffset{Topic: "orders", Partition: 0, Offset: nullableOffset(-1, nil)}
	data, err := json.Marshal(po)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, ok := decoded["offset"]; !ok || v != nil {
		t.Errorf("offset = %v (present=%v), want explicit null", v, ok)
	}

	p := metrics.Partition{ID: 0, StartOffset: nullableOffset(0, nil), EndOffset: nullableOffset(0, kerr.LeaderNotAvailable)}
	data, err = json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded["start_offset"] != float64(0) {
		t.Errorf("start_offset = %v, want 0", decoded["start_offset"])
	}
	if v, ok := decoded["end_offset"]; !ok || v != nil {
		t.Errorf("end_offset = %v (present=%v), want explicit null", v, ok)
	}
}
