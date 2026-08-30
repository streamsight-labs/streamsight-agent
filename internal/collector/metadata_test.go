package collector

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"kafka-metrics-agent/internal/metrics"
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
	// kadm keys partitions by map, so "partitions[0]" is whichever partition
	// Go's iteration order hit first.
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
	// A "__" prefix heuristic is wrong in both directions: it lets _schemas
	// through and drops a user topic named __events.
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

// truncatingCollector builds a Collector with no Kafka client, all buildTopic
// needs.
func truncatingCollector(t *testing.T, lim Limits) *Collector {
	t.Helper()
	c, err := New(nil, Options{Limits: lim})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func topicWithPartitions(n int) kadm.TopicDetail {
	td := kadm.TopicDetail{Topic: "orders", Partitions: kadm.PartitionDetails{}}
	for i := 0; i < n; i++ {
		// Replica count shrinks with partition ID, so the durability floor
		// lives in the partitions a prefix cap would drop.
		td.Partitions[int32(i)] = kadm.PartitionDetail{Partition: int32(i), Replicas: make([]int32, n-i)}
	}
	return td
}

// noOffsets is the three unusable offset phases, so buildTopic shapes
// partitions from metadata alone.
func noOffsets() (starts, lsos, ends offsetSample) {
	return offsetSample{}, offsetSample{}, offsetSample{}
}

// No cap shortens a partition list, so the declared count and the list must
// agree exactly -- which is what the ingest's data.partition_count check
// enforces at the far end.
func TestPartitionCountMatchesTheEmittedList(t *testing.T) {
	c := truncatingCollector(t, Limits{})
	sec := newSection(sectionTopics)
	starts, lsos, ends := noOffsets()

	tm := c.buildTopic(topicWithPartitions(5), sec, starts, lsos, ends, offsetSample{}, offsetSample{}, offsetSample{})

	if tm.PartitionCount != 5 || len(tm.Partitions) != 5 {
		t.Errorf("PartitionCount = %d with %d partitions emitted, want 5 and 5",
			tm.PartitionCount, len(tm.Partitions))
	}
	if sec.truncated {
		t.Error("nothing can truncate a partition list, so the section must not say it did")
	}
}

func TestReplicationFactorIsTheMinimumOverEveryPartition(t *testing.T) {
	// The durability floor is a minimum, not a sample: one under-replicated
	// partition has to drag it down however many others are healthy.
	c := truncatingCollector(t, Limits{})
	sec := newSection(sectionTopics)
	starts, lsos, ends := noOffsets()

	tm := c.buildTopic(topicWithPartitions(5), sec, starts, lsos, ends, offsetSample{}, offsetSample{}, offsetSample{})

	if tm.ReplicationFactor != 1 {
		t.Errorf("ReplicationFactor = %d, want 1 — the minimum over ALL five partitions", tm.ReplicationFactor)
	}
}

func TestBuildTopicUncappedEmitsEverything(t *testing.T) {
	c := truncatingCollector(t, Limits{})
	sec := newSection(sectionTopics)
	starts, lsos, ends := noOffsets()

	tm := c.buildTopic(topicWithPartitions(5), sec, starts, lsos, ends, offsetSample{}, offsetSample{}, offsetSample{})

	if len(tm.Partitions) != 5 || tm.PartitionCount != 5 {
		t.Errorf("emitted %d of %d partitions, want all five", len(tm.Partitions), tm.PartitionCount)
	}
	if sec.truncated || sec.dropped != (metrics.Truncation{}) {
		t.Errorf("an uncapped topic must report no truncation, got %+v", sec.dropped)
	}
}

// listed builds a one-topic ListedOffsets over the given partition offsets.
func listed(topic string, offsets map[int32]int64) kadm.ListedOffsets {
	ps := make(map[int32]kadm.ListedOffset, len(offsets))
	for p, o := range offsets {
		ps[p] = kadm.ListedOffset{Topic: topic, Partition: p, Offset: o}
	}
	return kadm.ListedOffsets{topic: ps}
}

func TestBuildTopicOrdersTheOffsetChain(t *testing.T) {
	// On a transactional topic the high watermark counts commit markers, so a
	// caught-up read_committed consumer is at zero lag against the LSO and at
	// false lag against the high watermark. The agent computes neither: it ships
	// all three ceilings, and they must be start <= lso <= end.
	c := truncatingCollector(t, Limits{})
	sec := newSection(sectionTopics)
	lsoSec, endSec := newSection(sectionTopicsLSO), newSection(sectionTopicsEnd)

	tm := c.buildTopic(topicWithPartitions(1), sec,
		offsetSample{api: "ListStartOffsets", sec: sec, listed: listed("orders", map[int32]int64{0: 10}), ok: true},
		offsetSample{api: "ListCommittedOffsets", sec: lsoSec, listed: listed("orders", map[int32]int64{0: 100}), ok: true},
		offsetSample{api: "ListEndOffsets", sec: endSec, listed: listed("orders", map[int32]int64{0: 137}), ok: true},
		offsetSample{}, offsetSample{}, offsetSample{},
	)

	p := tm.Partitions[0]
	if p.StartOffset == nil || p.LastStableOffset == nil || p.EndOffset == nil {
		t.Fatalf("every offset must be populated, got %+v", p)
	}
	if *p.StartOffset != 10 || *p.LastStableOffset != 100 || *p.EndOffset != 137 {
		t.Errorf("offsets = %d/%d/%d, want 10/100/137", *p.StartOffset, *p.LastStableOffset, *p.EndOffset)
	}
	for _, s := range []*section{sec, lsoSec, endSec} {
		if len(s.errs) != 0 {
			t.Errorf("section %q recorded %+v, want no errors", s.name, s.errs)
		}
	}
}

func TestBuildTopicDisabledLSOIsNullNotZero(t *testing.T) {
	// A disabled phase is null at the partition level, never 0, and records
	// nothing; "why is it null" is answered by the topics_lso section status.
	c := truncatingCollector(t, Limits{})
	sec := newSection(sectionTopics)
	lsoSec := newSection(sectionTopicsLSO)
	lsoSec.downgrade(metrics.SectionSkipped)

	tm := c.buildTopic(topicWithPartitions(1), sec,
		offsetSample{api: "ListStartOffsets", sec: sec, listed: listed("orders", map[int32]int64{0: 10}), ok: true},
		offsetSample{api: "ListCommittedOffsets", sec: lsoSec},
		offsetSample{},
		offsetSample{}, offsetSample{}, offsetSample{},
	)

	if p := tm.Partitions[0]; p.LastStableOffset != nil {
		t.Errorf("last_stable_offset = %d, want null when the phase did not run", *p.LastStableOffset)
	}
	if len(lsoSec.errs) != 0 {
		t.Errorf("a skipped phase must record no per-partition errors, got %+v", lsoSec.errs)
	}
	// The field carries no omitempty precisely so null reaches the wire.
	b, err := json.Marshal(tm.Partitions[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"last_stable_offset":null`) {
		t.Errorf("partition JSON must carry an explicit null: %s", b)
	}
}

func TestBuildTopicPartialLSOOnlyDegradesItsOwnSection(t *testing.T) {
	// A partition the LSO listing missed must null only the LSO and leave the
	// start and end offsets — and their sections — untouched.
	c := truncatingCollector(t, Limits{})
	sec := newSection(sectionTopics)
	lsoSec, endSec := newSection(sectionTopicsLSO), newSection(sectionTopicsEnd)

	full := map[int32]int64{0: 10, 1: 20}
	tm := c.buildTopic(topicWithPartitions(2), sec,
		offsetSample{api: "ListStartOffsets", sec: sec, listed: listed("orders", full), ok: true},
		// Partition 1 is absent from the LSO listing.
		offsetSample{api: "ListCommittedOffsets", sec: lsoSec, listed: listed("orders", map[int32]int64{0: 10}), ok: true},
		offsetSample{api: "ListEndOffsets", sec: endSec, listed: listed("orders", full), ok: true},
		offsetSample{}, offsetSample{}, offsetSample{},
	)

	if p := tm.Partitions[1]; p.LastStableOffset != nil || p.EndOffset == nil {
		t.Fatalf("partition 1 = %+v, want a null LSO and a real end offset", p)
	}
	if lsoSec.status != metrics.SectionPartial {
		t.Errorf("topics_lso = %q, want partial", lsoSec.status)
	}
	if len(lsoSec.errs) != 1 || lsoSec.errs[0].Partition == nil || *lsoSec.errs[0].Partition != 1 {
		t.Errorf("want one error attributed to partition 1, got %+v", lsoSec.errs)
	}
	if sec.status != metrics.SectionOK || endSec.status != metrics.SectionOK {
		t.Errorf("topics = %q and topics_end = %q, want both ok", sec.status, endSec.status)
	}
}

func TestCollectTopicsAlwaysReturnsThreeSections(t *testing.T) {
	// Section count must not depend on the path taken: a backend that sees two
	// sections one cycle and three the next cannot tell "not collected" from
	// "collected, empty". Both paths below never touch the Kafka client, which
	// is what lets them run with a nil one.
	tests := []struct {
		name       string
		tds        kadm.TopicDetails
		opts       Options
		wantTopics metrics.SectionStatus
	}{
		{
			name:       "cluster metadata failed",
			tds:        nil,
			wantTopics: metrics.SectionSkipped,
		},
		{
			name:       "filter selected no topics",
			tds:        kadm.TopicDetails{"orders": {Topic: "orders"}},
			opts:       Options{TopicInclude: []string{"nothing"}},
			wantTopics: metrics.SectionOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(nil, tt.opts)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			closed := make(chan struct{})
			close(closed)

			topics, secs := c.collectTopics(context.Background(), tt.tds, c.opts.CollectMaxTimestamp, closed)

			if topics != nil {
				t.Errorf("topics = %+v, want none", topics)
			}
			// All three are built on every path: a backend must be able to tell
			// "not collected" from "collected, empty".
			ordered := []*section{secs.starts, secs.lso, secs.end}
			for i, want := range []string{sectionTopics, sectionTopicsLSO, sectionTopicsEnd} {
				if ordered[i] == nil {
					t.Fatalf("section %d (%s) is nil", i, want)
				}
				if ordered[i].name != want {
					t.Errorf("section %d = %q, want %q", i, ordered[i].name, want)
				}
			}
			if secs.starts.status != tt.wantTopics {
				t.Errorf("topics = %q, want %q", secs.starts.status, tt.wantTopics)
			}
			// A phase that never issued a request is skipped, never ok.
			for _, s := range ordered[1:] {
				if s.status != metrics.SectionSkipped {
					t.Errorf("section %q = %q, want skipped", s.name, s.status)
				}
			}
		})
	}
}

func int64Ptr(v int64) *int64 { return &v }

// A partition with no records answers offset -1 and timestamp -1. Both must
// travel as null: a real offset zero at the Unix epoch is a different claim.
func TestMaxTimestampNullsTheEmptyPartitionSentinel(t *testing.T) {
	sec := newSection(sectionTopicsMaxTS)
	s := offsetSample{api: "ListMaxTimestampOffsets", sec: sec, ok: true, here: true,
		listed: kadm.ListedOffsets{"orders": {
			0: {Topic: "orders", Partition: 0, Offset: 41, Timestamp: 1750000000000, LeaderEpoch: 3},
			1: {Topic: "orders", Partition: 1, Offset: -1, Timestamp: -1},
		}}}

	if got := s.maxTimestamp("orders", 0); got == nil ||
		got.Offset == nil || *got.Offset != 41 ||
		got.TimestampMs == nil || *got.TimestampMs != 1750000000000 ||
		got.LeaderEpoch != 3 {
		t.Fatalf("populated partition = %+v", got)
	}

	got := s.maxTimestamp("orders", 1)
	if got == nil {
		t.Fatal("an empty partition must still produce an entry")
	}
	if got.Offset != nil || got.TimestampMs != nil {
		t.Errorf("empty partition = %+v, want both fields null", got)
	}
}

// "The phase did not run" is the section status, not a per-partition value.
func TestMaxTimestampReturnsNothingWhenThePhaseDidNotRun(t *testing.T) {
	sec := newSection(sectionTopicsMaxTS)
	s := offsetSample{api: "ListMaxTimestampOffsets", sec: sec, ok: false}
	if got := s.maxTimestamp("orders", 0); got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

// The three later flavours are absent keys when their phases did not run, so a
// backend cannot confuse "not collected" with "the broker answered null".
func TestBuildTopicOmitsTheOptionalOffsetFlavoursWhenSkipped(t *testing.T) {
	c := truncatingCollector(t, Limits{})
	sec := newSection(sectionTopics)

	tm := c.buildTopic(topicWithPartitions(1), sec,
		offsetSample{api: "ListStartOffsets", sec: sec, listed: listed("orders", map[int32]int64{0: 10}), ok: true},
		offsetSample{}, offsetSample{},
		offsetSample{}, offsetSample{}, offsetSample{},
	)

	p := tm.Partitions[0]
	if p.MaxTimestamp != nil {
		t.Errorf("max_timestamp = %+v, want absent", p.MaxTimestamp)
	}
	if p.Tiered != nil {
		t.Errorf("tiered = %+v, want absent", p.Tiered)
	}
}

// KIP-1005 landed five releases after KIP-405, so a 3.4-3.8 cluster serves the
// local start and not the remote end. One null inside a present object is a real
// state, not an error.
func TestBuildTopicCarriesLocalStartWithoutRemoteEnd(t *testing.T) {
	c := truncatingCollector(t, Limits{})
	sec := newSection(sectionTopics)
	localSec := newSection(sectionTopicsLocal)
	remoteSec := newSection(sectionTopicsRemote)
	remoteSec.downgrade(metrics.SectionSkipped)

	tm := c.buildTopic(topicWithPartitions(1), sec,
		offsetSample{api: "ListStartOffsets", sec: sec, listed: listed("orders", map[int32]int64{0: 10}), ok: true},
		offsetSample{}, offsetSample{}, offsetSample{},
		offsetSample{api: "ListLocalLogStartOffsets", sec: localSec, listed: listed("orders", map[int32]int64{0: 900}), ok: true},
		offsetSample{api: "ListLatestRemoteOffsets", sec: remoteSec},
	)

	p := tm.Partitions[0]
	if p.Tiered == nil {
		t.Fatal("tiered must be present when either flavour answered")
	}
	if p.Tiered.LocalStartOffset == nil || *p.Tiered.LocalStartOffset != 900 {
		t.Errorf("local_start_offset = %v, want 900", p.Tiered.LocalStartOffset)
	}
	if p.Tiered.RemoteEndOffset != nil {
		t.Errorf("remote_end_offset = %d, want null on a cluster below v9", *p.Tiered.RemoteEndOffset)
	}
}
