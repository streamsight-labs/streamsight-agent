package collector

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

func windowCollector(t *testing.T, opts Options) *Collector {
	t.Helper()
	c, err := New(nil, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestCollectThroughputWindowSkippedPathsIssueNoRequest(t *testing.T) {
	// The nil client is the assertion: any path reaching ListOffsetsAfterMilli
	// panics. An empty topic list must never fall through, because kadm lists
	// EVERY topic in the cluster when no names are given — the inverse of the
	// filter, and a fan-out to every leader.
	tds := kadm.TopicDetails{
		"orders":             {Topic: "orders", Partitions: kadm.PartitionDetails{0: {Partition: 0}}},
		"__consumer_offsets": {Topic: "__consumer_offsets", IsInternal: true, Partitions: kadm.PartitionDetails{0: {Partition: 0}}},
	}

	tests := []struct {
		name string
		opts Options
		tds  kadm.TopicDetails
		run  bool
	}{
		{name: "not a window cycle", tds: tds, run: false},
		{name: "cluster metadata failed", tds: nil, run: true},
		{name: "filter matches nothing", opts: Options{TopicInclude: []string{"nothing"}}, tds: tds, run: true},
		{
			name: "only internal topics exist and they are excluded",
			tds:  kadm.TopicDetails{"__consumer_offsets": tds["__consumer_offsets"]},
			run:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := windowCollector(t, tt.opts)
			window, sample, sec := c.collectThroughputWindow(context.Background(), tt.tds, time.Minute, tt.run)
			if window != nil {
				t.Errorf("window = %+v, want none", window)
			}
			if sample != nil {
				t.Errorf("sample = %+v, want none", sample)
			}
			if sec.status != metrics.SectionSkipped {
				t.Errorf("topics_window = %q, want skipped", sec.status)
			}
			if len(sec.errs) != 0 {
				t.Errorf("a skipped section must record nothing, got %+v", sec.errs)
			}
		})
	}
}

func TestThroughputWindowLooksBackwards(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)

	tests := []struct {
		name  string
		width time.Duration
		want  time.Duration
	}{
		{name: "configured width", width: 90 * time.Second, want: 90 * time.Second},
		// A non-positive width would ask about the present or the future: every
		// partition answers -1 and kadm re-lists all of them, so the cycle pays
		// two fan-outs for a window nothing can be divided by.
		{name: "zero falls back", width: 0, want: defaultWindowWidth},
		{name: "negative falls back", width: -time.Minute, want: defaultWindowWidth},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := throughputWindow(at, tt.width)
			if got := at.UnixMilli() - w.RequestedMs; got != tt.want.Milliseconds() {
				t.Errorf("requested %dms before now, want %dms", got, tt.want.Milliseconds())
			}
			if w.WidthMs != tt.want.Milliseconds() {
				t.Errorf("WidthMs = %d, want %d", w.WidthMs, tt.want.Milliseconds())
			}
		})
	}
}

func windowTopic(name string, ids ...int32) metrics.TopicMetrics {
	tm := metrics.TopicMetrics{Name: name, PartitionCount: len(ids)}
	for _, id := range ids {
		tm.Partitions = append(tm.Partitions, metrics.Partition{ID: id})
	}
	return tm
}

func TestAttachWindowOffsets(t *testing.T) {
	sample := &windowSample{
		sec: newSection(sectionTopicsWindow),
		listed: kadm.ListedOffsets{"orders": {
			0: {Topic: "orders", Partition: 0, Offset: 900, Timestamp: 1_700_000_000_000, LeaderEpoch: 7},
			// No record after the window: the broker answered -1, kadm re-listed
			// the end offset, and the timestamp of an end-offset listing is -1.
			1: {Topic: "orders", Partition: 1, Offset: 42, Timestamp: -1, LeaderEpoch: 3},
			2: {Topic: "orders", Partition: 2, Offset: -1, Timestamp: -1, LeaderEpoch: -1, Err: kerr.LeaderNotAvailable},
			3: {Topic: "orders", Partition: 3, Offset: -1, Timestamp: -1, LeaderEpoch: -1},
		}},
	}

	topics := []metrics.TopicMetrics{windowTopic("orders", 0, 1, 2, 3, 9)}
	sample.attach(topics)

	got := topics[0].Partitions
	if w := got[0].Window; w == nil || w.Offset == nil || *w.Offset != 900 ||
		w.TimestampMs == nil || *w.TimestampMs != 1_700_000_000_000 || w.LeaderEpoch != 7 {
		t.Errorf("partition 0 window = %+v, want the offset, its broker timestamp and epoch 7", w)
	}

	// The one case a naive implementation gets wrong twice: the offset is real
	// (it is the current end offset, so end - offset is legitimately 0) while
	// the timestamp must be null rather than -1, and neither is an error.
	w1 := got[1].Window
	if w1 == nil || w1.Offset == nil || *w1.Offset != 42 {
		t.Fatalf("partition 1 window = %+v, want the end offset kept", w1)
	}
	if w1.TimestampMs != nil {
		t.Errorf("partition 1 timestamp = %d, want null for a -1 sentinel", *w1.TimestampMs)
	}

	if w := got[2].Window; w == nil || w.Offset != nil || w.ErrorCode != kerr.LeaderNotAvailable.Code {
		t.Errorf("partition 2 window = %+v, want a null offset carrying error code %d", w, kerr.LeaderNotAvailable.Code)
	}
	if w := got[3].Window; w == nil || w.Offset != nil {
		t.Errorf("partition 3 window = %+v, want a null offset for a -1 with no error", w)
	}
	if got[4].Window != nil {
		t.Errorf("partition 9 was not listed; window = %+v, want absent", got[4].Window)
	}

	// Deduplication collapses the three kindOther, code-0 entries into one
	// exemplar, so assert on occurrences rather than len(errs).
	if seen := recordedFor(sample.sec, kerr.LeaderNotAvailable.Code); seen != 1 {
		t.Errorf("recorded %d leader errors, want 1", seen)
	}
	if seen := recordedFor(sample.sec, 0); seen != 2 {
		t.Errorf("recorded %d bare failures, want 2 (a -1 offset and an unlisted partition)", seen)
	}
	if sample.sec.status != metrics.SectionPartial {
		t.Errorf("status = %q, want partial", sample.sec.status)
	}
}

// recordedFor totals the occurrences of one Kafka error code, exemplars and
// collapsed duplicates alike.
func recordedFor(sec *section, code int16) int {
	seen := 0
	for k, b := range sec.keys {
		if k.code == code {
			seen += b.seen
		}
	}
	return seen
}

func TestAttachWindowOffsetsTopicScopedFailures(t *testing.T) {
	tests := []struct {
		name   string
		listed kadm.ListedOffsets
	}{
		{
			name:   "topic missing from the listing",
			listed: kadm.ListedOffsets{},
		},
		{
			// kadm reports a topic-level failure as a synthetic partition -1, and
			// the whole topic must cost one error rather than one per partition.
			name: "kadm's synthetic -1 partition",
			listed: kadm.ListedOffsets{"orders": {
				-1: {Topic: "orders", Partition: -1, Err: kerr.UnknownTopicOrPartition},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sample := &windowSample{sec: newSection(sectionTopicsWindow), listed: tt.listed}
			topics := []metrics.TopicMetrics{windowTopic("orders", 0, 1, 2)}
			sample.attach(topics)

			for _, p := range topics[0].Partitions {
				if p.Window != nil {
					t.Errorf("partition %d window = %+v, want absent", p.ID, p.Window)
				}
			}
			if len(sample.sec.errs) != 1 || sample.sec.errs[0].Topic != "orders" || sample.sec.errs[0].Partition != nil {
				t.Errorf("want one topic-scoped error, got %+v", sample.sec.errs)
			}
		})
	}
}

func TestAttachNilSampleLeavesWindowsAbsent(t *testing.T) {
	// A failed or skipped phase returns no sample; attaching it must be a no-op
	// rather than a nil dereference in the middle of an otherwise good cycle.
	var sample *windowSample
	topics := []metrics.TopicMetrics{windowTopic("orders", 0)}
	sample.attach(topics)
	if topics[0].Partitions[0].Window != nil {
		t.Errorf("window = %+v, want absent", topics[0].Partitions[0].Window)
	}
}
