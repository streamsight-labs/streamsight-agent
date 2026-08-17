package collector

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"kafka-metrics-agent/internal/metrics"
)

func logDirCollector(t *testing.T, opts Options) *Collector {
	t.Helper()
	c, err := New(nil, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestCollectLogDirsSkippedPathsIssueNoRequest(t *testing.T) {
	// The nil client is the assertion: any path reaching DescribeAllLogDirs
	// panics. An empty topic set must never fall through, because kadm encodes
	// it as a NULL topics array and the broker reads null as "describe every
	// directory on every broker" — the inverse of the filter, on the one section
	// whose response is O(cluster).
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
		{name: "not a log dirs cycle", opts: Options{CollectLogDirs: true}, tds: tds, run: false},
		{name: "cluster metadata failed", opts: Options{CollectLogDirs: true}, tds: nil, run: true},
		{
			name: "filter matches nothing",
			opts: Options{CollectLogDirs: true, TopicIncludeRegex: "^nothing$"},
			tds:  tds,
			run:  true,
		},
		{
			name: "only internal topics exist and they are excluded",
			opts: Options{CollectLogDirs: true},
			tds:  kadm.TopicDetails{"__consumer_offsets": tds["__consumer_offsets"]},
			run:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := logDirCollector(t, tt.opts)
			dirs, sec := c.collectLogDirs(context.Background(), metrics.ClusterMetrics{}, tt.tds, tt.run)
			if dirs != nil {
				t.Errorf("dirs = %+v, want none", dirs)
			}
			if sec.status != metrics.SectionSkipped {
				t.Errorf("log_dirs = %q, want skipped", sec.status)
			}
			if len(sec.errs) != 0 {
				t.Errorf("a skipped section must record nothing, got %+v", sec.errs)
			}
		})
	}
}

func TestLogDirTopicsCarriesExplicitPartitionIDs(t *testing.T) {
	// A topics-only set maps to zero partitions broker-side: the handler
	// flat-maps each requested topic's partition list, so omitting the IDs
	// yields directories with no rows.
	c := logDirCollector(t, Options{})
	tds := kadm.TopicDetails{
		"orders": {Topic: "orders", Partitions: kadm.PartitionDetails{
			0: {Partition: 0}, 1: {Partition: 1},
		}},
		"__consumer_offsets": {Topic: "__consumer_offsets", IsInternal: true, Partitions: kadm.PartitionDetails{
			0: {Partition: 0},
		}},
	}

	set, truncated := c.logDirTopics(tds)

	if truncated {
		t.Error("no cap is set, so nothing was truncated")
	}
	if len(set) != 1 {
		t.Fatalf("set = %v, want only the non-internal topic", set)
	}
	if got := len(set["orders"]); got != 2 {
		t.Errorf("orders carries %d partition IDs, want 2", got)
	}
}

func TestLogDirTopicsHonoursTheTopicCaps(t *testing.T) {
	// log_dirs must describe exactly the topics topics[] describes, or a backend
	// joining bytes onto partition inventory gets rows with no join partner.
	c := logDirCollector(t, Options{Limits: Limits{MaxTopics: 1, MaxPartitionsPerTopic: 2}})
	tds := kadm.TopicDetails{
		"a": {Topic: "a", Partitions: kadm.PartitionDetails{0: {Partition: 0}, 1: {Partition: 1}, 2: {Partition: 2}}},
		"b": {Topic: "b", Partitions: kadm.PartitionDetails{0: {Partition: 0}}},
	}

	set, truncated := c.logDirTopics(tds)

	if !truncated {
		t.Error("both caps fired; the section must admit it is short")
	}
	if len(set) != 1 || set["a"] == nil {
		t.Fatalf("set = %v, want the sorted prefix {a}", set)
	}
	if len(set["a"]) != 2 {
		t.Errorf("a carries %d partitions, want the capped 2", len(set["a"]))
	}
}

func described(dirs ...kadm.DescribedLogDir) kadm.DescribedAllLogDirs {
	all := kadm.DescribedAllLogDirs{}
	for _, d := range dirs {
		if all[d.Broker] == nil {
			all[d.Broker] = kadm.DescribedLogDirs{}
		}
		all[d.Broker][d.Dir] = d
	}
	return all
}

func TestBuildLogDirsShapesReplicaStorage(t *testing.T) {
	sec := newSection(sectionLogDirs)
	all := described(
		kadm.DescribedLogDir{Broker: 2, Dir: "/data/1", Topics: kadm.DescribedLogDirTopics{
			"orders": {0: {Broker: 2, Dir: "/data/1", Topic: "orders", Partition: 0, Size: 4096, OffsetLag: 3}},
		}},
		kadm.DescribedLogDir{Broker: 1, Dir: "/data/2", Topics: kadm.DescribedLogDirTopics{
			"orders": {1: {Broker: 1, Dir: "/data/2", Topic: "orders", Partition: 1, Size: 8, IsFuture: true}},
		}},
		kadm.DescribedLogDir{Broker: 1, Dir: "/data/1", Topics: kadm.DescribedLogDirTopics{}},
	)
	cluster := metrics.ClusterMetrics{Brokers: []metrics.Broker{{ID: 1}, {ID: 2}}}

	dirs := buildLogDirs(all, cluster, sec)

	// Broker then directory, never Go map order.
	want := []struct {
		broker int32
		dir    string
	}{{1, "/data/1"}, {1, "/data/2"}, {2, "/data/1"}}
	if len(dirs) != len(want) {
		t.Fatalf("got %d dirs, want %d", len(dirs), len(want))
	}
	for i, w := range want {
		if dirs[i].Broker != w.broker || dirs[i].Dir != w.dir {
			t.Errorf("dirs[%d] = %d %q, want %d %q", i, dirs[i].Broker, dirs[i].Dir, w.broker, w.dir)
		}
	}
	if p := dirs[2].Partitions[0]; p.Size != 4096 || p.OffsetLag != 3 || p.IsFuture {
		t.Errorf("partition = %+v, want size 4096, lag 3, not future", p)
	}
	if !dirs[1].Partitions[0].IsFuture {
		t.Error("an in-flight JBOD move must be flagged; it explains disk growth that reads as runaway")
	}
	// KIP-827 volume figures stay null until the raw-kmsg path exists; the
	// fields are declared so filling them in later is not a schema change.
	if dirs[0].TotalBytes != nil || dirs[0].UsableBytes != nil {
		t.Error("total_bytes/usable_bytes must be null: kadm drops them")
	}
	// An empty directory on a broker that reported other directories is a real
	// empty disk, not a refusal.
	if sec.status != metrics.SectionOK || len(sec.errs) != 0 {
		t.Errorf("section = %q with %+v, want a clean ok", sec.status, sec.errs)
	}
}

func TestBuildLogDirsReportsAnOfflineDirectory(t *testing.T) {
	// A directory-level error is the only positive identification of an offline
	// log dir from the broker that owns it; offline_replicas in metadata is a
	// peer's opinion.
	sec := newSection(sectionLogDirs)
	all := described(kadm.DescribedLogDir{Broker: 7, Dir: "/mnt/kafka-3", Err: kerr.KafkaStorageError})
	cluster := metrics.ClusterMetrics{Brokers: []metrics.Broker{{ID: 7}}}

	dirs := buildLogDirs(all, cluster, sec)

	if len(dirs) != 1 {
		t.Fatalf("got %d dirs, want the failed one emitted", len(dirs))
	}
	if dirs[0].ErrorCode != kerr.KafkaStorageError.Code {
		t.Errorf("error_code = %d, want %d (KAFKA_STORAGE_ERROR)", dirs[0].ErrorCode, kerr.KafkaStorageError.Code)
	}
	// nil, not empty: "the broker could not read this directory" is not "this
	// directory holds nothing".
	if dirs[0].Partitions != nil {
		t.Errorf("partitions = %+v, want nil on an offline directory", dirs[0].Partitions)
	}
	if sec.status != metrics.SectionPartial {
		t.Errorf("log_dirs = %q, want partial: the rest of the cluster's bytes are still true", sec.status)
	}
	if len(sec.errs) != 1 {
		t.Fatalf("errs = %+v, want one", sec.errs)
	}
	e := sec.errs[0]
	if e.BrokerID == nil || *e.BrokerID != 7 || e.Dir != "/mnt/kafka-3" {
		t.Errorf("error = %+v, want it attributed to broker 7 and /mnt/kafka-3", e)
	}
}

func TestBuildLogDirsDetectsSilentRefusal(t *testing.T) {
	// Below DescribeLogDirs v3 (Kafka 3.2) the response has no top-level error
	// code, so a broker that declines answers with an empty result and no
	// error. Without this check it is byte-identical to a healthy cluster.
	sec := newSection(sectionLogDirs)
	all := kadm.DescribedAllLogDirs{1: kadm.DescribedLogDirs{}}
	cluster := metrics.ClusterMetrics{Brokers: []metrics.Broker{{ID: 1}, {ID: 2}}}

	dirs := buildLogDirs(all, cluster, sec)

	if len(dirs) != 0 {
		t.Errorf("dirs = %+v, want none", dirs)
	}
	if sec.status != metrics.SectionPartial {
		t.Errorf("log_dirs = %q, want partial", sec.status)
	}
	if len(sec.errs) != 1 {
		t.Fatalf("errs = %+v, want exactly one — broker 2 shard-failed and is already attributed by request()", sec.errs)
	}
	if e := sec.errs[0]; e.BrokerID == nil || *e.BrokerID != 1 || e.Message != errNoLogDirs.Error() {
		t.Errorf("error = %+v, want broker 1 flagged as having answered with nothing", e)
	}
}

func TestLogDirsEveryClampsToEveryCycle(t *testing.T) {
	// The value is a modulus, so zero would panic.
	for _, in := range []int{-5, 0, 1} {
		if got := logDirsEvery(in); got != 1 {
			t.Errorf("logDirsEvery(%d) = %d, want 1", in, got)
		}
	}
	if got := logDirsEvery(10); got != 10 {
		t.Errorf("logDirsEvery(10) = %d, want 10", got)
	}
}

func TestLogDirsCadence(t *testing.T) {
	// A fresh agent samples on cycle 0 rather than N intervals in, and the
	// counter advances whether or not the phase is enabled.
	c := logDirCollector(t, Options{CollectLogDirs: true, LogDirsEvery: 3})
	var ran []int
	for i := 0; i < 7; i++ {
		n := c.cycle.Add(1) - 1
		if n%logDirsEvery(c.opts.LogDirsEvery) == 0 {
			ran = append(ran, i)
		}
	}
	want := []int{0, 3, 6}
	if len(ran) != len(want) {
		t.Fatalf("ran on %v, want %v", ran, want)
	}
	for i := range want {
		if ran[i] != want[i] {
			t.Fatalf("ran on %v, want %v", ran, want)
		}
	}
}
