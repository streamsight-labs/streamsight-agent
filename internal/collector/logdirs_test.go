package collector

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
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
	// The nil client is the assertion: any path reaching the capability probe or
	// the request panics. An empty topic set must never fall through, because a
	// nil topics array encodes as NULL and the broker reads null as "describe
	// every directory on every broker" — the inverse of the filter, on the one
	// section whose response is O(cluster).
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
			opts: Options{CollectLogDirs: true, TopicInclude: []string{"nothing"}},
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
			dirs, sec := c.collectLogDirs(context.Background(), &metrics.ClusterMetrics{}, tt.tds, tt.run)
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

	set := c.logDirTopics(tds)

	if len(set) != 1 {
		t.Fatalf("set = %v, want only the non-internal topic", set)
	}
	if got := len(set["orders"]); got != 2 {
		t.Errorf("orders carries %d partition IDs, want 2", got)
	}
}

func TestLogDirTopicsHonoursTheTopicFilter(t *testing.T) {
	// log_dirs must describe exactly the topics topics[] describes, or a backend
	// joining bytes onto partition inventory gets rows with no join partner. The
	// filter is also what bounds this request on the wire, since the log-dir
	// request names every partition explicitly.
	c := logDirCollector(t, Options{TopicExclude: []string{"b"}})
	tds := kadm.TopicDetails{
		"a": {Topic: "a", Partitions: kadm.PartitionDetails{0: {Partition: 0}, 1: {Partition: 1}, 2: {Partition: 2}}},
		"b": {Topic: "b", Partitions: kadm.PartitionDetails{0: {Partition: 0}}},
	}

	set := c.logDirTopics(tds)

	if len(set) != 1 || set["a"] == nil {
		t.Fatalf("set = %v, want only the unfiltered topic {a}", set)
	}
	if len(set["a"]) != 3 {
		t.Errorf("a carries %d partitions, want all 3: nothing caps the list", len(set["a"]))
	}
}

func TestDescribeLogDirsRequestNeverEncodesANullTopicsArray(t *testing.T) {
	// The nil array is the trap, not an edge case: kmsg encodes nil as NULL and
	// the broker reads NULL as "describe every partition on every broker". The
	// caller's empty-set guard is the first half of the defence; a non-nil array
	// here is the second.
	req := describeLogDirsRequest(nil)

	if req.Topics == nil {
		t.Fatal("Topics = nil, which asks the broker for the entire cluster")
	}
	if len(req.Topics) != 0 {
		t.Errorf("Topics = %+v, want empty", req.Topics)
	}
}

func TestDescribeLogDirsRequestIsSortedAndNamesPartitions(t *testing.T) {
	var set kadm.TopicsSet
	set.Add("orders", 2)
	set.Add("orders", 0)
	set.Add("audit", 1)

	req := describeLogDirsRequest(set)

	if len(req.Topics) != 2 {
		t.Fatalf("topics = %+v, want 2", req.Topics)
	}
	// Map order would make one cluster produce a different request every cycle.
	if req.Topics[0].Topic != "audit" || req.Topics[1].Topic != "orders" {
		t.Errorf("topics = %q, %q, want audit then orders", req.Topics[0].Topic, req.Topics[1].Topic)
	}
	got := req.Topics[1].Partitions
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Errorf("orders partitions = %v, want the sorted [0 2]", got)
	}
}

// logDirsResp builds one broker's answer at the given version.
func logDirsResp(version int16, dirs ...kmsg.DescribeLogDirsResponseDir) *kmsg.DescribeLogDirsResponse {
	resp := kmsg.NewPtrDescribeLogDirsResponse()
	resp.Version = version
	resp.Dirs = dirs
	return resp
}

// logDirsDir builds one directory with the KIP-827 defaults in place, so a test
// that does not set them exercises the -1 sentinel the wire actually carries.
func logDirsDir(dir string, parts ...kmsg.DescribeLogDirsResponseDirTopic) kmsg.DescribeLogDirsResponseDir {
	d := kmsg.NewDescribeLogDirsResponseDir()
	d.Dir = dir
	d.Topics = parts
	return d
}

func logDirsTopic(topic string, parts ...kmsg.DescribeLogDirsResponseDirTopicPartition) kmsg.DescribeLogDirsResponseDirTopic {
	t := kmsg.NewDescribeLogDirsResponseDirTopic()
	t.Topic = topic
	t.Partitions = parts
	return t
}

func logDirsPartition(partition int32, size, lag int64, future bool) kmsg.DescribeLogDirsResponseDirTopicPartition {
	p := kmsg.NewDescribeLogDirsResponseDirTopicPartition()
	p.Partition = partition
	p.Size = size
	p.OffsetLag = lag
	p.IsFuture = future
	return p
}

func okShard(node int32, resp *kmsg.DescribeLogDirsResponse) kgo.ResponseShard {
	return kgo.ResponseShard{Meta: kgo.BrokerMetadata{NodeID: node}, Resp: resp}
}

func TestShardLogDirsCarriesVolumeBytes(t *testing.T) {
	// The whole point of v4: growth with a denominator. Both figures are per
	// VOLUME, so they are read straight off the dir rather than derived.
	dir := logDirsDir("/data/1", logDirsTopic("orders", logDirsPartition(1, 4096, 3, false), logDirsPartition(0, 8, 0, true)))
	dir.TotalBytes = 1 << 40
	dir.UsableBytes = 1 << 30

	dirs, err := shardLogDirs(okShard(1, logDirsResp(4, dir)))

	if err != nil {
		t.Fatalf("shardLogDirs: %v", err)
	}
	if len(dirs) != 1 {
		t.Fatalf("dirs = %+v, want 1", dirs)
	}
	d := dirs[0]
	if d.totalBytes == nil || *d.totalBytes != 1<<40 {
		t.Errorf("total_bytes = %v, want %d", d.totalBytes, int64(1)<<40)
	}
	if d.usableBytes == nil || *d.usableBytes != 1<<30 {
		t.Errorf("usable_bytes = %v, want %d", d.usableBytes, int64(1)<<30)
	}
	// Broker order is not guaranteed, and a batch must be diffable.
	if len(d.partitions) != 2 || d.partitions[0].Partition != 0 || d.partitions[1].Partition != 1 {
		t.Fatalf("partitions = %+v, want partition 0 then 1", d.partitions)
	}
	if p := d.partitions[1]; p.Size != 4096 || p.OffsetLag != 3 || p.IsFuture {
		t.Errorf("partition = %+v, want size 4096, lag 3, not future", p)
	}
	if !d.partitions[0].IsFuture {
		t.Error("an in-flight JBOD move must be flagged; it explains disk growth that reads as runaway")
	}
}

func TestShardLogDirsTranslatesTheNotAvailableSentinel(t *testing.T) {
	// -1 is the broker saying "I could not stat this volume", and it is also
	// what an older broker leaves behind because the fields are not on the wire
	// at all. A -1 shipped as a byte count is a disk of negative size.
	for _, tt := range []struct {
		name    string
		version int16
		total   int64
		usable  int64
	}{
		{name: "v4 broker that could not stat the volume", version: 4, total: -1, usable: -1},
		{name: "v3 broker that never sent the fields", version: 3, total: -1, usable: -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := logDirsDir("/data/1")
			dir.TotalBytes = tt.total
			dir.UsableBytes = tt.usable

			dirs, err := shardLogDirs(okShard(1, logDirsResp(tt.version, dir)))

			if err != nil {
				t.Fatalf("shardLogDirs: %v", err)
			}
			if dirs[0].totalBytes != nil || dirs[0].usableBytes != nil {
				t.Errorf("volume figures = %v/%v, want null, never a negative size",
					dirs[0].totalBytes, dirs[0].usableBytes)
			}
			// Null capacity must not take the directory's contents with it.
			if dirs[0].partitions == nil {
				t.Error("partitions = nil, want an empty list: the directory was read, it is simply empty")
			}
		})
	}
}

func TestShardLogDirsKeepsADirectoryErrorOnTheDirectory(t *testing.T) {
	// A directory-level error is the only positive identification of an offline
	// log dir from the broker that owns it.
	dir := logDirsDir("/mnt/kafka-3")
	dir.ErrorCode = kerr.KafkaStorageError.Code

	dirs, err := shardLogDirs(okShard(7, logDirsResp(4, dir)))

	if err != nil {
		t.Fatalf("a dead disk is not a failed shard: %v", err)
	}
	if !errors.Is(dirs[0].err, kerr.KafkaStorageError) {
		t.Errorf("err = %v, want KAFKA_STORAGE_ERROR", dirs[0].err)
	}
	// nil, not empty: "the broker could not read this directory" is not "this
	// directory holds nothing".
	if dirs[0].partitions != nil {
		t.Errorf("partitions = %+v, want nil on an offline directory", dirs[0].partitions)
	}
}

func TestShardLogDirsTreatsATopLevelErrorAsThisBrokersRefusal(t *testing.T) {
	// The top-level code exists from v3 (Kafka 3.2). It is one broker's answer,
	// so it must travel as a shard error and not as a verdict on the cluster.
	resp := logDirsResp(4, logDirsDir("/data/1"))
	resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code

	dirs, err := shardLogDirs(okShard(3, resp))

	if !errors.Is(err, kerr.ClusterAuthorizationFailed) {
		t.Fatalf("err = %v, want CLUSTER_AUTHORIZATION_FAILED", err)
	}
	if dirs != nil {
		t.Errorf("dirs = %+v, want none: the broker refused before naming any", dirs)
	}
}

func TestShardLogDirsRejectsAnUnexpectedResponseType(t *testing.T) {
	shard := kgo.ResponseShard{Meta: kgo.BrokerMetadata{NodeID: 1}, Resp: kmsg.NewPtrMetadataResponse()}

	if _, err := shardLogDirs(shard); !errors.Is(err, errNotLogDirsResponse) {
		t.Errorf("err = %v, want errNotLogDirsResponse rather than a panic mid-cycle", err)
	}
}

func TestLogDirShardsKeepsTheBrokersThatAnswered(t *testing.T) {
	// One denied or dead broker must not discard the dirs the others reported:
	// a thirty-broker cluster with one bad shard is a partial section, not a
	// cluster with no disks.
	shards := []kgo.ResponseShard{
		okShard(1, logDirsResp(4, logDirsDir("/data/1"))),
		{Meta: kgo.BrokerMetadata{NodeID: 2}, Err: kerr.ClusterAuthorizationFailed},
	}

	report, err := logDirShards(shards)

	if len(report) != 1 || report[1] == nil {
		t.Fatalf("report = %+v, want broker 1's directories kept", report)
	}
	var shardErrs *kadm.ShardErrors
	if !errors.As(err, &shardErrs) {
		t.Fatalf("err = %v, want a *kadm.ShardErrors so section.request maps it as usual", err)
	}
	if shardErrs.AllFailed {
		t.Error("AllFailed = true, but broker 1 answered")
	}

	sec := newSection(sectionLogDirs)
	if !sec.request(apiDescribeLogDirs, err) {
		t.Error("request() = false, but half the cluster's bytes are still true")
	}
	if sec.status != metrics.SectionPartial {
		t.Errorf("log_dirs = %q, want partial", sec.status)
	}
	if len(sec.errs) != 1 {
		t.Fatalf("errs = %+v, want the one denied broker", sec.errs)
	}
	if e := sec.errs[0]; e.BrokerID == nil || *e.BrokerID != 2 || e.Kind != kindAuthorization {
		t.Errorf("error = %+v, want an authorization failure attributed to broker 2", e)
	}
}

func TestLogDirShardsReportsAWhollyDeniedFanOut(t *testing.T) {
	shards := []kgo.ResponseShard{
		{Meta: kgo.BrokerMetadata{NodeID: 1}, Err: kerr.ClusterAuthorizationFailed},
		{Meta: kgo.BrokerMetadata{NodeID: 2}, Err: kerr.ClusterAuthorizationFailed},
	}

	report, err := logDirShards(shards)

	if len(report) != 0 {
		t.Fatalf("report = %+v, want nothing", report)
	}
	sec := newSection(sectionLogDirs)
	if sec.request(apiDescribeLogDirs, err) {
		t.Error("request() = true, but no broker answered")
	}
	if sec.status != metrics.SectionUnauthorized {
		t.Errorf("log_dirs = %q, want unauthorized: the ACL is the actionable cause", sec.status)
	}
}

func TestLogDirShardsAttributesAShardThatNeverReachedABroker(t *testing.T) {
	// kgo answers with node ID -1 when it could not map the request to a broker.
	// A -1 must not be reported as a broker ID, and must not become a report key.
	shards := []kgo.ResponseShard{{Meta: kgo.BrokerMetadata{NodeID: -1}, Err: errors.New("no broker for topic")}}

	report, err := logDirShards(shards)

	if len(report) != 0 {
		t.Fatalf("report = %+v, want nothing keyed under a non-existent broker", report)
	}
	sec := newSection(sectionLogDirs)
	sec.request(apiDescribeLogDirs, err)
	if len(sec.errs) != 1 {
		t.Fatalf("errs = %+v, want one", sec.errs)
	}
	if sec.errs[0].BrokerID != nil {
		t.Errorf("broker_id = %d, want null: no broker was reached", *sec.errs[0].BrokerID)
	}
}

func TestKadmLogDirsLeavesTheVolumeFiguresNull(t *testing.T) {
	// The pre-v4 path is kept verbatim, and kadm's DescribedLogDir is
	// {Broker, Dir, Topics, Err}: the KIP-827 fields never reach the agent, so
	// null is the only honest answer. A fabricated zero would read as a disk
	// that is permanently 100% full.
	described := kadm.DescribedAllLogDirs{1: kadm.DescribedLogDirs{
		"/data/1": {Broker: 1, Dir: "/data/1", Topics: kadm.DescribedLogDirTopics{
			"orders": {0: {Broker: 1, Dir: "/data/1", Topic: "orders", Partition: 0, Size: 4096, OffsetLag: 3}},
		}},
	}}

	report := kadmLogDirs(described)

	if len(report[1]) != 1 {
		t.Fatalf("report = %+v, want one directory on broker 1", report)
	}
	d := report[1][0]
	if d.totalBytes != nil || d.usableBytes != nil {
		t.Errorf("volume figures = %v/%v, want null below DescribeLogDirs v4", d.totalBytes, d.usableBytes)
	}
	if len(d.partitions) != 1 || d.partitions[0].Size != 4096 || d.partitions[0].OffsetLag != 3 {
		t.Errorf("partitions = %+v, want the replica kadm decoded", d.partitions)
	}
}

func TestKadmLogDirsKeepsABrokerThatNamedNoDirectory(t *testing.T) {
	// Presence in the report is the silent-refusal tell; an empty slice must not
	// collapse into an absent broker.
	report := kadmLogDirs(kadm.DescribedAllLogDirs{1: kadm.DescribedLogDirs{}})

	if dirs, ok := report[1]; !ok || len(dirs) != 0 {
		t.Errorf("report = %+v, want broker 1 present with no directories", report)
	}
}

func report(dirs ...describedDir) logDirReport {
	r := logDirReport{}
	for _, d := range dirs {
		r[d.broker] = append(r[d.broker], d)
	}
	return r
}

func TestBuildLogDirsShapesReplicaStorage(t *testing.T) {
	sec := newSection(sectionLogDirs)
	total, usable := int64(1<<40), int64(1<<30)
	all := report(
		describedDir{broker: 2, dir: "/data/1", totalBytes: &total, usableBytes: &usable, partitions: []metrics.LogDirPartition{
			{Topic: "orders", Partition: 0, Size: 4096, OffsetLag: 3},
		}},
		describedDir{broker: 1, dir: "/data/2", partitions: []metrics.LogDirPartition{
			{Topic: "orders", Partition: 1, Size: 8, IsFuture: true},
		}},
		describedDir{broker: 1, dir: "/data/1", partitions: []metrics.LogDirPartition{}},
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
	// The denominator reaches the wire on the directory that reported it, and
	// stays null on the ones that did not.
	if dirs[2].TotalBytes == nil || *dirs[2].TotalBytes != total || dirs[2].UsableBytes == nil || *dirs[2].UsableBytes != usable {
		t.Errorf("volume figures = %v/%v, want %d/%d", dirs[2].TotalBytes, dirs[2].UsableBytes, total, usable)
	}
	if dirs[0].TotalBytes != nil || dirs[0].UsableBytes != nil {
		t.Error("total_bytes/usable_bytes must stay null when the broker did not report them")
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
	all := report(describedDir{broker: 7, dir: "/mnt/kafka-3", err: kerr.KafkaStorageError})
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
	all := logDirReport{1: {}}
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

func TestEveryNthClampsToEveryCycle(t *testing.T) {
	// The value is a modulus, so zero would panic.
	for _, in := range []int{-5, 0, 1} {
		if got := everyNth(in); got != 1 {
			t.Errorf("everyNth(%d) = %d, want 1", in, got)
		}
	}
	if got := everyNth(10); got != 10 {
		t.Errorf("everyNth(10) = %d, want 10", got)
	}
}

func TestLogDirsCadence(t *testing.T) {
	// A fresh agent samples on cycle 0 rather than N intervals in, and the
	// counter advances whether or not the phase is enabled.
	c := logDirCollector(t, Options{CollectLogDirs: true, LogDirsEvery: 3})
	var ran []int
	for i := 0; i < 7; i++ {
		n := c.cycle.Add(1) - 1
		if runsThisCycle(c.opts.CollectLogDirs, c.opts.LogDirsEvery, phaseLogDirs, n) {
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

// The offsets exist so the heavy phases never land in one collection budget.
// This proves it by exhaustion rather than by argument: over a full period of
// the three default cadences, no cycle runs two of them.
func TestCadencedPhasesNeverCoincide(t *testing.T) {
	const (
		maxTSEvery   = 12  // config.DefaultMaxTimestampEvery
		logDirsEvery = 24  // config.DefaultLogDirsEvery
		configsEvery = 360 // config.DefaultConfigsEvery
	)

	// lcm(12, 24, 360) = 360, so one period of 360 cycles covers every
	// alignment the three can ever reach.
	for n := uint64(0); n < 360; n++ {
		var ran []string
		if runsThisCycle(true, maxTSEvery, phaseMaxTS, n) {
			ran = append(ran, "max_timestamp")
		}
		if runsThisCycle(true, logDirsEvery, phaseLogDirs, n) {
			ran = append(ran, "log_dirs")
		}
		if runsThisCycle(true, configsEvery, phaseConfigs, n) {
			ran = append(ran, "configs")
		}
		if len(ran) > 1 {
			t.Errorf("cycle %d runs %v together; the phase offsets are supposed to keep them apart", n, ran)
		}
	}

	// And each still runs at its stated rate over that period: the offsets
	// stagger the phases without changing how often they sample.
	for _, tt := range []struct {
		name   string
		every  int
		offset uint64
		want   int
	}{
		{"max_timestamp", maxTSEvery, phaseMaxTS, 360 / maxTSEvery},
		{"log_dirs", logDirsEvery, phaseLogDirs, 360 / logDirsEvery},
		{"configs", configsEvery, phaseConfigs, 360 / configsEvery},
	} {
		var got int
		for n := uint64(0); n < 360; n++ {
			if runsThisCycle(true, tt.every, tt.offset, n) {
				got++
			}
		}
		if got != tt.want {
			t.Errorf("%s ran %d times in 360 cycles, want %d", tt.name, got, tt.want)
		}
	}
}

// The max-timestamp phase is the reason the cadence was added: it is the only
// ListOffsets sentinel that is not O(1) on the broker, and its per-partition
// object is the largest single field the agent ships.
func TestMaxTimestampCadence(t *testing.T) {
	c := &Collector{opts: Options{CollectMaxTimestamp: true, MaxTimestampEvery: 12}}

	var ran []uint64
	for n := uint64(0); n < 26; n++ {
		if runsThisCycle(c.opts.CollectMaxTimestamp, c.opts.MaxTimestampEvery, phaseMaxTS, n) {
			ran = append(ran, n)
		}
	}
	if want := []uint64{10, 22}; !slices.Equal(ran, want) {
		t.Errorf("sampled on cycles %v, want %v", ran, want)
	}

	// Off means off, whatever the cadence says.
	off := &Collector{opts: Options{CollectMaxTimestamp: false, MaxTimestampEvery: 1}}
	for n := uint64(0); n < 5; n++ {
		if runsThisCycle(off.opts.CollectMaxTimestamp, off.opts.MaxTimestampEvery, phaseMaxTS, n) {
			t.Fatalf("cycle %d sampled with COLLECT_MAX_TIMESTAMP off", n)
		}
	}
}
