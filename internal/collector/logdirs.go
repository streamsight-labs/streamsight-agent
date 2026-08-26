package collector

import (
	"context"
	"errors"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"kafka-metrics-agent/internal/metrics"
)

// apiDescribeLogDirs names the request every error in this phase is attributed
// to. Both request paths below issue the same API key; only the version differs,
// so a backend sees one API name whatever the cluster serves.
const apiDescribeLogDirs = "DescribeLogDirs"

// errNotLogDirsResponse guards the type assertion on the raw path. kgo answers
// with the response kind the request declares, so this is unreachable in
// practice and is here so a future protocol change surfaces as a recorded error
// rather than a panic in the collection goroutine.
var errNotLogDirsResponse = errors.New("broker answered DescribeLogDirs with another response type")

// collectLogDirs describes the on-disk footprint of every replica of every
// selected topic, on every broker, and the free space of the volumes underneath.
//
// DescribeLogDirs is the only request in the client protocol that returns BYTES,
// and the only positive identification of an offline log directory from the
// broker that owns it: offline_replicas in metadata is a peer's opinion, and a
// broker whose disk died may still be in every ISR its peers can see.
//
// It adds no ACL: the broker gates it on DESCRIBE of CLUSTER, which Metadata
// already needs. v4 (KIP-827) adds none either — see describeLogDirs.
//
// run is false on the cycles between samples; the section is still built and
// returned, so "not collected" and "collected, empty" are never the same batch.
func (c *Collector) collectLogDirs(ctx context.Context, cluster metrics.ClusterMetrics, tds kadm.TopicDetails, run bool) ([]metrics.LogDir, *section) {
	sec := c.newSection(sectionLogDirs)
	defer sec.stop()

	if !run || tds == nil {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	set, truncated := c.logDirTopics(tds)
	sec.truncated = truncated
	if len(set) == 0 {
		// MANDATORY guard, and the inverse of the bug it looks like. An empty set
		// leaves the topics array nil, kmsg encodes a nil array as NULL, and the
		// broker reads a null topics array as "describe everything". A filter
		// matching nothing would therefore describe every log directory on every
		// broker — on the one section whose response is O(cluster). Same class of
		// bug as the List*Offsets guard in collectTopics.
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	described, err := c.describeLogDirs(ctx, set)
	// Shard errors leave the brokers that did answer in the report: one
	// unreachable broker out of thirty is a partial section, not a cluster with
	// no disks.
	if !sec.request(apiDescribeLogDirs, err) && len(described) == 0 {
		return nil, sec
	}

	return buildLogDirs(described, cluster, sec), sec
}

// describeLogDirs issues this phase's one request, at the highest version the
// cluster serves.
//
// v4 is the SAME request as today's: same API key, same kgo sharding, same
// DESCRIBE on CLUSTER. All the version buys is TotalBytes and UsableBytes per
// directory (KIP-827), which kadm's decoder drops — DescribedLogDir is
// {Broker, Dir, Topics, Err} at kadm v1.18.0 — and which are the denominator
// under every days-to-full forecast. Without them the phase reports growth with
// no capacity to compare it against.
//
// Below v4 the kadm path is kept verbatim and both figures stay null. A broker
// that cannot answer must produce null, never a fabricated zero: a disk reported
// as 0 bytes total is a disk that is always 100% full.
func (c *Collector) describeLogDirs(ctx context.Context, set kadm.TopicsSet) (logDirReport, error) {
	if c.supportsLogDirVolumeBytes(ctx) {
		return logDirShards(c.client.RequestSharded(ctx, describeLogDirsRequest(set)))
	}
	described, err := c.client.Admin.DescribeAllLogDirs(ctx, set)
	// Converted even on error: kadm returns the brokers that did answer
	// alongside a *ShardErrors, and discarding them would blank a cluster over
	// one dead broker.
	return kadmLogDirs(described), err
}

// supportsLogDirVolumeBytes reports whether EVERY broker serves DescribeLogDirs
// v4. Every broker matters because the request is sharded and the report is
// merged: one old broker would otherwise contribute rows whose null volume
// figures are indistinguishable from a broker that failed to stat its disk.
//
// The probe is cached for the client's lifetime, so on a healthy cluster this
// costs nothing after startup. A probe that failed at startup is retried here,
// on the log-dirs cadence rather than every cycle, and until it answers the two
// fields stay null rather than guessed.
func (c *Collector) supportsLogDirVolumeBytes(ctx context.Context) bool {
	caps, err := c.client.Probe(ctx)
	if err != nil {
		return false
	}
	return caps.SupportsLogDirTotalBytes()
}

// describedDir is one log directory on one broker, in the shape both request
// paths converge on: what kadm decodes, plus the volume figures its decoder
// drops.
type describedDir struct {
	broker      int32
	dir         string
	err         error
	totalBytes  *int64
	usableBytes *int64
	// partitions stays nil when err is set: "the broker could not read this
	// directory" is not "this directory holds nothing".
	partitions []metrics.LogDirPartition
}

// logDirReport is one cycle's directories, keyed by the broker that reported
// them.
//
// The key set is load-bearing and not derivable from the values: a broker that
// answered with NO directories at all is the only tell for a silent refusal
// below DescribeLogDirs v3, so "present with an empty slice" must stay
// distinguishable from "absent because its shard failed".
type logDirReport map[int32][]describedDir

// sorted flattens the report by broker, then directory, so the same cluster
// produces the same bytes every cycle and a batch stays diffable.
func (r logDirReport) sorted() []describedDir {
	n := 0
	for _, dirs := range r {
		n += len(dirs)
	}
	all := make([]describedDir, 0, n)
	for _, dirs := range r {
		all = append(all, dirs...)
	}
	sort.Slice(all, func(i, j int) bool {
		l, r := all[i], all[j]
		return l.broker < r.broker || l.broker == r.broker && l.dir < r.dir
	})
	return all
}

// describeLogDirsRequest builds the raw request kadm would have built, with the
// topics and their partitions in sorted order so that one topic set always
// produces the same bytes on the wire.
//
// The topics array is allocated non-nil, which together with the caller's
// empty-set guard is what keeps a filter matching nothing from asking for the
// whole cluster: kmsg encodes a nil array as NULL and the broker reads NULL as
// "describe every partition on every broker".
//
// Partition IDs are mandatory, not an optimisation; see logDirTopics.
func describeLogDirsRequest(set kadm.TopicsSet) *kmsg.DescribeLogDirsRequest {
	req := kmsg.NewPtrDescribeLogDirsRequest()
	req.Topics = make([]kmsg.DescribeLogDirsRequestTopic, 0, len(set))
	for _, tp := range set.Sorted() {
		t := kmsg.NewDescribeLogDirsRequestTopic()
		t.Topic = tp.Topic
		t.Partitions = tp.Partitions
		req.Topics = append(req.Topics, t)
	}
	return req
}

// logDirShards folds a sharded fan-out into one report plus, when some brokers
// did not answer, a *kadm.ShardErrors naming them.
//
// The error type is kadm's on purpose. section.request already implements the
// policy for a partly failed fan-out — per-broker attribution, all-failed versus
// partly-failed status, an authorization failure in one shard not collapsing a
// thirty-broker partial into "unauthorized, no data" — and that policy must not
// fork just because this path builds its own request.
func logDirShards(shards []kgo.ResponseShard) (logDirReport, error) {
	report := make(logDirReport, len(shards))
	shardErrs := kadm.ShardErrors{Name: apiDescribeLogDirs}
	for _, shard := range shards {
		dirs, err := shardLogDirs(shard)
		if err != nil {
			shardErrs.Errs = append(shardErrs.Errs, kadm.ShardError{
				Req:    shard.Req,
				Err:    err,
				Broker: shard.Meta,
			})
			continue
		}
		// One shard per broker, so no unique check; a shard that never reached a
		// broker carries node ID -1 and always has an error, so it cannot land
		// here.
		report[shard.Meta.NodeID] = dirs
	}
	if len(shardErrs.Errs) == 0 {
		return report, nil
	}
	shardErrs.AllFailed = len(shardErrs.Errs) == len(shards)
	return report, &shardErrs
}

// shardLogDirs decodes one broker's answer.
//
// A top-level error code is this BROKER's refusal, not the cluster's, so it
// becomes this shard's error and the brokers that did answer keep their
// directories. The code exists only from v3 (Kafka 3.2); below that a refusal
// arrives as an empty directory list with no error at all, which buildLogDirs
// catches instead.
func shardLogDirs(shard kgo.ResponseShard) ([]describedDir, error) {
	if shard.Err != nil {
		return nil, shard.Err
	}
	resp, ok := shard.Resp.(*kmsg.DescribeLogDirsResponse)
	if !ok {
		return nil, errNotLogDirsResponse
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return nil, err
	}

	// Non-nil even when the broker named no directory: presence in the report is
	// what the silent-refusal check reads.
	dirs := make([]describedDir, 0, len(resp.Dirs))
	for _, d := range resp.Dirs {
		dd := describedDir{
			broker: shard.Meta.NodeID,
			dir:    d.Dir,
			err:    kerr.ErrorForCode(d.ErrorCode),
			// kmsg defaults both to -1, so a broker that answered below v4 lands
			// on the same sentinel a v4 broker sends when it could not stat the
			// volume, and both travel as null.
			totalBytes:  nullableBytes(d.TotalBytes),
			usableBytes: nullableBytes(d.UsableBytes),
		}
		if dd.err == nil {
			dd.partitions = logDirPartitions(d)
		}
		dirs = append(dirs, dd)
	}
	return dirs, nil
}

// nullableBytes maps a KIP-827 volume figure onto the wire representation.
//
// -1 is the broker's "not available" sentinel — the volume could not be
// stat'ed, or the response predates the field — and shipping it as a byte count
// would report a disk of negative size and a days-to-full forecast built on it.
// Same funnel, and the same reasoning, as nullableOffset.
func nullableBytes(v int64) *int64 { return nullableOffset(v, nil) }

// logDirPartitions flattens one directory's replicas into topic, then partition
// order. The broker's own order is not guaranteed, and a batch must be diffable
// against the last one.
func logDirPartitions(d kmsg.DescribeLogDirsResponseDir) []metrics.LogDirPartition {
	n := 0
	for _, t := range d.Topics {
		n += len(t.Partitions)
	}
	parts := make([]metrics.LogDirPartition, 0, n)
	for _, t := range d.Topics {
		for _, p := range t.Partitions {
			parts = append(parts, metrics.LogDirPartition{
				Topic:     t.Topic,
				Partition: p.Partition,
				Size:      p.Size,
				OffsetLag: p.OffsetLag,
				IsFuture:  p.IsFuture,
			})
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		l, r := parts[i], parts[j]
		return l.Topic < r.Topic || l.Topic == r.Topic && l.Partition < r.Partition
	})
	return parts
}

// kadmLogDirs converts a kadm-decoded response into the common shape, for a
// cluster below DescribeLogDirs v4.
//
// Both volume figures stay null, because kadm's DescribedLogDir carries only
// {Broker, Dir, Topics, Err}: on this path the fields never reach the agent at
// all, which is a different reason for null than the broker's -1 and the same
// answer.
func kadmLogDirs(described kadm.DescribedAllLogDirs) logDirReport {
	report := make(logDirReport, len(described))
	for broker, brokerDirs := range described {
		sorted := brokerDirs.Sorted()
		dirs := make([]describedDir, 0, len(sorted))
		for _, d := range sorted {
			dd := describedDir{broker: broker, dir: d.Dir, err: d.Err}
			if d.Err == nil {
				parts := d.Topics.Sorted()
				dd.partitions = make([]metrics.LogDirPartition, 0, len(parts))
				for _, p := range parts {
					dd.partitions = append(dd.partitions, metrics.LogDirPartition{
						Topic:     p.Topic,
						Partition: p.Partition,
						Size:      p.Size,
						OffsetLag: p.OffsetLag,
						IsFuture:  p.IsFuture,
					})
				}
			}
			dirs = append(dirs, dd)
		}
		report[broker] = dirs
	}
	return report
}

// logDirTopics builds the explicit topic+partition set to describe.
//
// Partition IDs are mandatory, not an optimisation: the broker flat-maps the
// partition list of each requested topic, so a topics-only set — what
// TopicsSet.MergeTopics produces — resolves to zero partitions and returns
// directories with no rows in them.
//
// It reuses selectTopics with the same inputs as the topics phase, so log_dirs
// describes exactly the topics in topics[]; otherwise a backend joining bytes
// onto partition inventory gets rows with no join partner. Unlike List*Offsets,
// the partition cap genuinely reduces broker work here, because the partitions
// are named on the wire.
//
// Drops are a truncation FLAG only, never counts: the topics phase already
// counts the same entities into the batch total.
func (c *Collector) logDirTopics(tds kadm.TopicDetails) (set kadm.TopicsSet, truncated bool) {
	selected, droppedTopics := selectTopics(tds, c.topics, c.opts.IncludeInternalTopics, c.limits.MaxTopics)
	truncated = droppedTopics > 0
	for _, td := range selected {
		parts := td.Partitions.Sorted()
		keep, dropped := capLen(len(parts), c.limits.MaxPartitionsPerTopic)
		if dropped > 0 {
			truncated = true
		}
		for _, p := range parts[:keep] {
			set.Add(td.Topic, p.Partition)
		}
	}
	return set, truncated
}

// buildLogDirs shapes the described directories and records their failures.
func buildLogDirs(described logDirReport, cluster metrics.ClusterMetrics, sec *section) []metrics.LogDir {
	sorted := described.sorted()
	dirs := make([]metrics.LogDir, 0, len(sorted))
	for _, d := range sorted {
		lg := metrics.LogDir{
			Broker: d.broker,
			Dir:    d.dir,
			// Carried on the failing directory too: a dead disk still has a size,
			// and the volume figures are per VOLUME rather than per directory, so a
			// sibling dir on the same mount would report them anyway.
			TotalBytes:  d.totalBytes,
			UsableBytes: d.usableBytes,
		}
		if d.err != nil {
			// The broker could not read this directory at all; error code 56
			// (KAFKA_STORAGE_ERROR) means it is OFFLINE. Partitions stays nil,
			// which is not the same as an empty directory.
			lg.ErrorCode = errorCode(d.err)
			sec.recordLogDir(apiDescribeLogDirs, d.broker, d.dir, d.err)
			dirs = append(dirs, lg)
			continue
		}
		lg.Partitions = d.partitions
		dirs = append(dirs, lg)
	}

	// Silent-refusal detection: see errNoLogDirs. Brokers whose shard failed are
	// absent from the report and already attributed by sec.request, so this
	// catches only "present, and answered with nothing".
	for _, b := range cluster.Brokers {
		if d, ok := described[b.ID]; ok && len(d) == 0 {
			sec.recordLogDir(apiDescribeLogDirs, b.ID, "", errNoLogDirs)
		}
	}

	return dirs
}
