package collector

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/streamsight-labs/streamsight-agent/internal/kafka"
	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

const fakeTopic = "orders"

// fakeCluster is a hand-written clusterClient that answers every phase with one
// broker, one topic, one partition and one group. It records the API name of
// every call in issue order, which is the second half of the ordering assertion
// below: sampled_at says when a section was stamped, calls says when the request
// actually went out.
//
// No mock framework and no expectation DSL: the tests here assert an ORDER, not
// a call count, and a matcher language would put a second thing to debug between
// the invariant and the failure message. The house style is a fake in a _test.go
// file -- see fakeCollector and fakeExporter in internal/agent -- and this is
// that, one method per interface member.
type fakeCluster struct {
	mu    sync.Mutex
	calls []string

	// startsIssued is closed the moment ListStartOffsets is entered, and the
	// ListGroups broadcast below blocks on it.
	//
	// This is what makes the topics-before-offsets edge deterministic, and it is
	// deliberate rather than incidental. Collect orders committed-before-LSO and
	// LSO-before-end with channel closes, but start-before-committed is NOT
	// ordered by a channel: the topics goroutine waits on metaDone alone while
	// the offsets goroutine waits on metaDone AND groupsListed, so against a
	// cluster that answers instantly the two stamp their sections in whichever
	// order the scheduler picks. Against a real broker the ListGroups broadcast
	// is a round trip and the start offsets always go first; the gate reproduces
	// that fact instead of leaving the test to race on it. It cannot deadlock:
	// ListStartOffsets depends on cluster metadata only, never on the group
	// listing.
	startsIssued chan struct{}
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{startsIssued: make(chan struct{})}
}

var _ clusterClient = (*fakeCluster)(nil)

func (f *fakeCluster) record(api string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, api)
}

func (f *fakeCluster) issued() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeCluster) Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error) {
	f.record("Metadata")
	return kadm.Metadata{
		Cluster: "fake-cluster",
		Brokers: kadm.BrokerDetails{{NodeID: 1, Host: "b1", Port: 9092}},
		Topics: kadm.TopicDetails{fakeTopic: {
			Topic: fakeTopic,
			Partitions: kadm.PartitionDetails{0: {
				Topic: fakeTopic, Partition: 0, Leader: 1,
				Replicas: []int32{1}, ISR: []int32{1},
			}},
		}},
	}, nil
}

// listed answers one List*Offsets flavour. The offsets ascend in the order the
// chain samples them, so a batch built out of them is internally consistent:
// start 0 <= committed 50 <= LSO 90 <= high watermark 100.
func (f *fakeCluster) listed(api string, offset int64) (kadm.ListedOffsets, error) {
	f.record(api)
	return kadm.ListedOffsets{fakeTopic: {0: {
		Topic: fakeTopic, Partition: 0, Offset: offset, Timestamp: 1,
	}}}, nil
}

func (f *fakeCluster) ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	select {
	case <-f.startsIssued:
	default:
		close(f.startsIssued)
	}
	return f.listed("ListStartOffsets", 0)
}

func (f *fakeCluster) ListCommittedOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return f.listed("ListCommittedOffsets", 90)
}

func (f *fakeCluster) ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return f.listed("ListEndOffsets", 100)
}

func (f *fakeCluster) ListMaxTimestampOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return f.listed("ListMaxTimestampOffsets", 99)
}

func (f *fakeCluster) ListLocalLogStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return f.listed("ListLocalLogStartOffsets", 10)
}

func (f *fakeCluster) ListLatestRemoteOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return f.listed("ListLatestRemoteOffsets", 5)
}

func (f *fakeCluster) ListOffsetsAfterMilli(ctx context.Context, ms int64, topics ...string) (kadm.ListedOffsets, error) {
	return f.listed("ListOffsetsAfterMilli", 20)
}

func (f *fakeCluster) FetchManyOffsets(ctx context.Context, groups ...string) kadm.FetchOffsetsResponses {
	f.record("OffsetFetch")
	out := make(kadm.FetchOffsetsResponses, len(groups))
	for _, g := range groups {
		out[g] = kadm.FetchOffsetsResponse{
			Group: g,
			Fetched: kadm.OffsetResponses{fakeTopic: {0: {
				Offset: kadm.Offset{Topic: fakeTopic, Partition: 0, At: 50, LeaderEpoch: 0},
			}}},
		}
	}
	return out
}

func (f *fakeCluster) DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error) {
	f.record("DescribeGroups")
	out := make(kadm.DescribedGroups, len(groups))
	for _, g := range groups {
		out[g] = kadm.DescribedGroup{Group: g, State: "Stable", Coordinator: kadm.BrokerDetail{NodeID: 1}}
	}
	return out, nil
}

func (f *fakeCluster) DescribeConsumerGroups(ctx context.Context, groups ...string) (kadm.DescribedConsumerGroups, error) {
	f.record("ConsumerGroupDescribe")
	return kadm.DescribedConsumerGroups{}, nil
}

func (f *fakeCluster) DescribeShareGroups(ctx context.Context, groups ...string) (kadm.DescribedShareGroups, error) {
	f.record("ShareGroupDescribe")
	return kadm.DescribedShareGroups{}, nil
}

func (f *fakeCluster) DescribeShareGroupOffsets(ctx context.Context, groups ...string) (kadm.DescribedShareGroupsOffsets, error) {
	f.record("DescribeShareGroupOffsets")
	return kadm.DescribedShareGroupsOffsets{}, nil
}

func (f *fakeCluster) DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error) {
	f.record("DescribeConfigs(topic)")
	return kadm.ResourceConfigs{{Name: fakeTopic}}, nil
}

func (f *fakeCluster) DescribeBrokerConfigs(ctx context.Context, brokers ...int32) (kadm.ResourceConfigs, error) {
	f.record("DescribeConfigs(broker)")
	return kadm.ResourceConfigs{{Name: "1"}}, nil
}

func (f *fakeCluster) DescribeAllLogDirs(ctx context.Context, s kadm.TopicsSet) (kadm.DescribedAllLogDirs, error) {
	f.record("DescribeLogDirs")
	return kadm.DescribedAllLogDirs{1: {"/var/lib/kafka": {
		Broker: 1, Dir: "/var/lib/kafka",
		Topics: kadm.DescribedLogDirTopics{fakeTopic: {0: {
			Broker: 1, Dir: "/var/lib/kafka", Topic: fakeTopic, Partition: 0, Size: 4096,
		}}},
	}}}, nil
}

func (f *fakeCluster) ListPartitionReassignments(ctx context.Context, s kadm.TopicsSet) (kadm.ListPartitionReassignmentsResponses, error) {
	f.record("ListPartitionReassignments")
	return kadm.ListPartitionReassignmentsResponses{}, nil
}

func (f *fakeCluster) OffsetForLeaderEpoch(ctx context.Context, r kadm.OffsetForLeaderEpochRequest) (kadm.OffsetsForLeaderEpochs, error) {
	f.record("OffsetForLeaderEpoch")
	return kadm.OffsetsForLeaderEpochs{}, nil
}

// RequestSharded serves the one raw-kmsg request a cycle like this makes: the
// ListGroups broadcast, kept raw for the GroupType field kadm decodes and drops.
// The log-dirs phase reaches its own raw path only when the probe says every
// broker serves DescribeLogDirs v4, and Probe below always fails, so it takes
// the kadm path instead and this method never sees its request.
func (f *fakeCluster) RequestSharded(ctx context.Context, req kmsg.Request) []kgo.ResponseShard {
	if _, ok := req.(*kmsg.ListGroupsRequest); !ok {
		return nil
	}
	select {
	case <-f.startsIssued:
	case <-ctx.Done():
		return nil
	}
	f.record("ListGroups")
	resp := kmsg.NewPtrListGroupsResponse()
	g := kmsg.NewListGroupsResponseGroup()
	g.Group = "billing"
	g.GroupType = "classic"
	resp.Groups = append(resp.Groups, g)
	return []kgo.ResponseShard{{Meta: kgo.BrokerMetadata{NodeID: 1}, Req: req, Resp: resp}}
}

// Probe fails deliberately. A failed probe is the one answer that leaves every
// phase exactly as the operator configured it -- the collector's own rule is
// that an unknown cluster must not have its data hidden -- so the sections in
// these tests are the sections the Options asked for, and nothing is disabled
// behind the test's back. Its only reader here is the log-dirs volume-bytes
// question, which falls back to kadm's DescribeAllLogDirs and records no error.
func (f *fakeCluster) Probe(ctx context.Context) (*kafka.Capabilities, error) {
	return nil, errors.New("fake cluster serves no ApiVersions")
}

func (f *fakeCluster) RPCSnapshot() *metrics.RPCStats {
	return &metrics.RPCStats{WindowMs: 1000}
}

// everyPhase turns on every phase the fake can answer, so no section owes its
// place in the batch to being switched off.
func everyPhase() Options {
	return Options{
		CollectLastStableOffset: true,
		CollectMaxTimestamp:     true,
		CollectTieredOffsets:    true,
		CollectLatestTiered:     true,
		CollectThroughputWindow: true,
		ThroughputWindowWidth:   time.Minute,
		CollectLogDirs:          true,
		CollectConfigs:          true,
		CollectConsumerGroups:   true,
	}
}

func sampledAt(t *testing.T, b *metrics.Batch, name string) time.Time {
	t.Helper()
	for _, s := range b.Sections {
		if s.Name == name {
			return s.SampledAt
		}
	}
	t.Fatalf("no section %q in the batch", name)
	return time.Time{}
}

func indexOf(t *testing.T, calls []string, api string) int {
	t.Helper()
	for i, c := range calls {
		if c == api {
			return i
		}
	}
	t.Fatalf("%s was never issued; calls = %v", api, calls)
	return -1
}

// TestCollectSamplesTheOffsetChainInOrder asserts, for the first time rather
// than commenting, the invariant the whole phase graph exists to hold:
//
//	start <= committed <= LSO <= high watermark
//
// Every quantity a backend derives from those samples is a difference, so the
// sampling order decides the SIGN of the residual skew between the calls: a
// caught-up consumer must never report -3 lag, and a hung transaction must never
// report a negative backlog and read as healthy. README.md calls collection order
// a correctness constraint and docs/ARCHITECTURE.md's "The wire order" restates
// it; until this test it was enforced by channel closes between eleven goroutines
// and by review, with the only end-to-end Collect test pointed at a refused port
// where every phase fails and order means nothing.
//
// Both halves are checked, because they can fail apart. sampled_at is what a
// backend actually reads and subtracts; the recorded call order is what the
// brokers actually saw, and a section stamped in the right order whose request
// went out in the wrong one would satisfy the first alone.
//
// The three flavours after topics_end are in the chain here too: they are the
// same ListOffsets key under a different timestamp sentinel and must stay after
// the high watermarks, or they inflate the very sample latency topics_end exists
// to pin down. topics_window is checked against topics_lso rather than against
// topics: it is issued in parallel with the start offsets and nothing orders it
// against them, but its offset is the earlier edge of a record count whose later
// edge is the high watermark, so it must precede both ceilings.
//
// Sections are looked up BY NAME and compared by timestamp, never by array
// index: sections[] ships in a fixed canonical order, and offsets sits at index
// nine of it while its sampled_at falls between topics and topics_lso.
func TestCollectSamplesTheOffsetChainInOrder(t *testing.T) {
	f := newFakeCluster()
	c, err := New(f, everyPhase())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	batch := c.Collect(context.Background())

	chain := []string{
		sectionTopics, sectionOffsets, sectionTopicsLSO, sectionTopicsEnd,
		sectionTopicsMaxTS, sectionTopicsLocal, sectionTopicsRemote,
	}
	for i := 1; i < len(chain); i++ {
		prev, cur := sampledAt(t, batch, chain[i-1]), sampledAt(t, batch, chain[i])
		if cur.Before(prev) {
			t.Errorf("%s sampled at %s, before %s at %s", chain[i], cur, chain[i-1], prev)
		}
	}
	if w, lso := sampledAt(t, batch, sectionTopicsWindow), sampledAt(t, batch, sectionTopicsLSO); lso.Before(w) {
		t.Errorf("topics_window sampled at %s, after topics_lso at %s", w, lso)
	}

	calls := f.issued()
	order := []string{"ListStartOffsets", "OffsetFetch", "ListCommittedOffsets", "ListEndOffsets", "ListMaxTimestampOffsets"}
	for i := 1; i < len(order); i++ {
		if indexOf(t, calls, order[i-1]) > indexOf(t, calls, order[i]) {
			t.Errorf("%s was issued after %s: %v", order[i-1], order[i], calls)
		}
	}
	if indexOf(t, calls, "ListOffsetsAfterMilli") > indexOf(t, calls, "ListEndOffsets") {
		t.Errorf("the throughput window was sampled after the high watermarks: %v", calls)
	}
}

// TestCollectAgainstAHealthyFakeShipsACleanBatch runs the same fake, healthy:
// fourteen sections ok, three untriggered, no errors, no truncation -- and the
// offsets, window and log-dir answers all landing on the partitions the topics
// phase built.
//
// It is that join that matters. The only end-to-end Collect test before this one
// ran against a refused port, where every phase fails identically and nothing
// about the batch's internal consistency is exercised; this is the first proof
// that a cycle whose requests all succeed produces a batch whose parts agree
// with each other.
func TestCollectAgainstAHealthyFakeShipsACleanBatch(t *testing.T) {
	f := newFakeCluster()
	c, err := New(f, everyPhase())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	batch := c.Collect(context.Background())

	names := make([]string, 0, len(batch.Sections))
	for _, s := range batch.Sections {
		names = append(names, s.Name)
	}
	if !slices.Equal(names, canonicalSections) {
		t.Fatalf("sections = %v, want %v", names, canonicalSections)
	}
	if len(batch.Errors) != 0 || batch.Truncation != nil {
		t.Errorf("a cluster that answered everything produced errors %+v and truncation %+v", batch.Errors, batch.Truncation)
	}

	// Only the three phases nothing triggered may be skipped: no
	// under-replicated partition, no committed-versus-current epoch mismatch,
	// and share groups not asked for.
	untriggered := map[string]bool{sectionEpochProbes: true, sectionReassignments: true, sectionShareGroups: true}
	for _, s := range batch.Sections {
		want := metrics.SectionOK
		if untriggered[s.Name] {
			want = metrics.SectionSkipped
		}
		if s.Status != want {
			t.Errorf("section %q = %q, want %q", s.Name, s.Status, want)
		}
		if s.SampledAt.IsZero() {
			t.Errorf("section %q has no sampled_at", s.Name)
		}
	}

	if len(batch.Topics) != 1 || len(batch.Topics[0].Partitions) != 1 {
		t.Fatalf("topics = %+v, want the one fake partition", batch.Topics)
	}
	p := batch.Topics[0].Partitions[0]
	for _, f := range []struct {
		name string
		got  *int64
		want int64
	}{
		{"start_offset", p.StartOffset, 0},
		{"last_stable_offset", p.LastStableOffset, 90},
		{"end_offset", p.EndOffset, 100},
	} {
		if f.got == nil || *f.got != f.want {
			t.Errorf("%s = %v, want %d", f.name, f.got, f.want)
		}
	}
	// The two phases that attach onto partitions the topics phase already built,
	// rather than building rows of their own.
	if p.Window == nil || p.Window.Offset == nil || *p.Window.Offset != 20 {
		t.Errorf("partition window = %+v, want the fake's ListOffsetsAfterMilli answer", p.Window)
	}
	if p.MaxTimestamp == nil || p.MaxTimestamp.Offset == nil || *p.MaxTimestamp.Offset != 99 {
		t.Errorf("partition max_timestamp = %+v", p.MaxTimestamp)
	}
	if len(batch.Groups) != 1 || len(batch.Offsets) != 1 || len(batch.LogDirs) != 1 {
		t.Errorf("groups=%d offsets=%d log_dirs=%d, want one of each", len(batch.Groups), len(batch.Offsets), len(batch.LogDirs))
	}
}
