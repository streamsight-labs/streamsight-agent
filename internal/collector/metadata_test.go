package collector

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestReplicationFactor(t *testing.T) {
	tests := []struct {
		name  string
		parts []kadm.PartitionDetail
		want  int
	}{
		{
			name: "uniform",
			parts: []kadm.PartitionDetail{
				{Partition: 0, Replicas: []int32{1, 2, 3}},
				{Partition: 1, Replicas: []int32{2, 3, 1}},
			},
			want: 3,
		},
		{
			name: "mid reassignment takes the minimum, not partition zero",
			parts: []kadm.PartitionDetail{
				{Partition: 0, Replicas: []int32{1, 2, 3}},
				{Partition: 1, Replicas: []int32{2}},
				{Partition: 2, Replicas: []int32{1, 2, 3, 4}},
			},
			want: 1,
		},
		{
			name: "minimum is independent of order",
			parts: []kadm.PartitionDetail{
				{Partition: 0, Replicas: []int32{2}},
				{Partition: 1, Replicas: []int32{1, 2, 3}},
			},
			want: 1,
		},
		{
			name:  "no partitions",
			parts: nil,
			want:  0,
		},
		{
			name: "errored partitions carry no replicas and are skipped",
			parts: []kadm.PartitionDetail{
				{Partition: 0, Replicas: []int32{1, 2, 3}},
				{Partition: 1, Err: kerr.LeaderNotAvailable},
			},
			want: 3,
		},
		{
			name: "all partitions errored",
			parts: []kadm.PartitionDetail{
				{Partition: 0, Err: kerr.LeaderNotAvailable},
			},
			want: 0,
		},
		{
			name: "a genuinely unreplicated partition still reports one",
			parts: []kadm.PartitionDetail{
				{Partition: 0, Replicas: []int32{1}},
			},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := replicationFactor(tt.parts); got != tt.want {
				t.Errorf("replicationFactor() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPartitionDetailsSortedIsDeterministic(t *testing.T) {
	// The bug this guards: kadm keys partitions by map, so "partitions[0]" is
	// whichever partition Go's iteration order hit first.
	ds := kadm.PartitionDetails{
		3: {Partition: 3, Replicas: []int32{1}},
		0: {Partition: 0, Replicas: []int32{1, 2}},
		2: {Partition: 2, Replicas: []int32{1, 2, 3}},
		1: {Partition: 1, Replicas: []int32{1, 2}},
	}
	for i := 0; i < 20; i++ {
		sorted := ds.Sorted()
		for j, p := range sorted {
			if p.Partition != int32(j) {
				t.Fatalf("Sorted()[%d].Partition = %d, want %d", j, p.Partition, j)
			}
		}
		if got := replicationFactor(sorted); got != 1 {
			t.Fatalf("replicationFactor() = %d, want 1", got)
		}
	}
}

func TestLookupOffset(t *testing.T) {
	listed := kadm.ListedOffsets{
		"orders": {
			0: {Topic: "orders", Partition: 0, Offset: 42},
			1: {Topic: "orders", Partition: 1, Offset: 0},
			2: {Topic: "orders", Partition: 2, Offset: -1, Err: kerr.LeaderNotAvailable},
			3: {Topic: "orders", Partition: 3, Offset: -1},
		},
	}

	tests := []struct {
		name      string
		partition int32
		want      *int64
		wantErr   bool
	}{
		{name: "real offset", partition: 0, want: int64Ptr(42)},
		{name: "empty partition is zero, not null", partition: 1, want: int64Ptr(0)},
		{name: "unavailable leader is null, not zero", partition: 2, want: nil, wantErr: true},
		{name: "negative with no error is null", partition: 3, want: nil, wantErr: true},
		{name: "missing partition is null", partition: 9, want: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sec := newSection(sectionTopics)
			got, err := lookupOffset(sec, "ListEndOffsets", listed, "orders", tt.partition)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			switch {
			case tt.want == nil && got != nil:
				t.Fatalf("got %d, want nil", *got)
			case tt.want != nil && got == nil:
				t.Fatalf("got nil, want %d", *tt.want)
			case tt.want != nil && *got != *tt.want:
				t.Fatalf("got %d, want %d", *got, *tt.want)
			}
			if tt.wantErr && len(sec.errs) != 1 {
				t.Fatalf("want exactly one recorded error, got %d", len(sec.errs))
			}
			if !tt.wantErr && len(sec.errs) != 0 {
				t.Fatalf("want no recorded errors, got %d", len(sec.errs))
			}
		})
	}
}

func TestTopicListedRecordsOneErrorPerTopic(t *testing.T) {
	listed := kadm.ListedOffsets{"orders": {0: {Topic: "orders", Partition: 0}}}
	sec := newSection(sectionTopics)

	if !topicListed(sec, "ListEndOffsets", listed, "orders") {
		t.Error("orders should be listed")
	}
	if len(sec.errs) != 0 {
		t.Errorf("want no errors, got %d", len(sec.errs))
	}
	if topicListed(sec, "ListEndOffsets", listed, "gone") {
		t.Error("gone should not be listed")
	}
	if len(sec.errs) != 1 || sec.errs[0].Topic != "gone" {
		t.Errorf("want one error attributed to gone, got %+v", sec.errs)
	}
}

func TestInternalTopics(t *testing.T) {
	tds := kadm.TopicDetails{
		"__consumer_offsets": {Topic: "__consumer_offsets", IsInternal: true},
		"_schemas":           {Topic: "_schemas", IsInternal: false},
		"__events":           {Topic: "__events", IsInternal: false},
	}
	internal := internalTopics(tds)

	if !internal["__consumer_offsets"] {
		t.Error("__consumer_offsets must be internal")
	}
	// The prefix heuristic was wrong in both directions: it let _schemas
	// through and dropped a user topic named __events.
	if internal["_schemas"] {
		t.Error("_schemas is not internal")
	}
	if internal["__events"] {
		t.Error("__events is a user topic despite the prefix")
	}
	if internalTopics(nil) != nil {
		t.Error("nil details must yield a nil set")
	}
}

func int64Ptr(v int64) *int64 { return &v }
