package collector

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

// collectLogDirs describes the on-disk footprint of every replica of every
// selected topic, on every broker.
//
// DescribeLogDirs is the only request in the client protocol that returns BYTES,
// and the only positive identification of an offline log directory from the
// broker that owns it: offline_replicas in metadata is a peer's opinion, and a
// broker whose disk died may still be in every ISR its peers can see.
//
// It adds no ACL: the broker gates it on DESCRIBE of CLUSTER, which Metadata
// already needs.
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
		// leaves kadm's topics array nil, kmsg encodes a nil array as NULL, and
		// the broker reads a null topics array as "describe everything". A filter
		// matching nothing would therefore describe every log directory on every
		// broker — on the one section whose response is O(cluster). Same class of
		// bug as the List*Offsets guard in collectTopics.
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	described, err := c.client.Admin.DescribeAllLogDirs(ctx, set)
	// Shard errors leave the brokers that did answer in the map: one unreachable
	// broker out of thirty is a partial section, not a cluster with no disks.
	if !sec.request("DescribeLogDirs", err) && len(described) == 0 {
		return nil, sec
	}

	return buildLogDirs(described, cluster, sec), sec
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
func buildLogDirs(described kadm.DescribedAllLogDirs, cluster metrics.ClusterMetrics, sec *section) []metrics.LogDir {
	// Sorted() orders by broker then directory, so the same cluster produces the
	// same bytes every cycle and a batch stays diffable.
	sorted := described.Sorted()
	dirs := make([]metrics.LogDir, 0, len(sorted))
	for _, d := range sorted {
		lg := metrics.LogDir{Broker: d.Broker, Dir: d.Dir}
		if d.Err != nil {
			// The broker could not read this directory at all; error code 56
			// (KAFKA_STORAGE_ERROR) means it is OFFLINE. Partitions stays nil,
			// which is not the same as an empty directory.
			lg.ErrorCode = errorCode(d.Err)
			sec.recordLogDir("DescribeLogDirs", d.Broker, d.Dir, d.Err)
			dirs = append(dirs, lg)
			continue
		}
		parts := d.Topics.Sorted()
		lg.Partitions = make([]metrics.LogDirPartition, 0, len(parts))
		for _, p := range parts {
			lg.Partitions = append(lg.Partitions, metrics.LogDirPartition{
				Topic:     p.Topic,
				Partition: p.Partition,
				Size:      p.Size,
				OffsetLag: p.OffsetLag,
				IsFuture:  p.IsFuture,
			})
		}
		dirs = append(dirs, lg)
	}

	// Silent-refusal detection: see errNoLogDirs. Brokers that shard-failed are
	// absent from the map and already attributed by sec.request, so this catches
	// only "present, and answered with nothing".
	for _, b := range cluster.Brokers {
		if d, ok := described[b.ID]; ok && len(d) == 0 {
			sec.recordLogDir("DescribeLogDirs", b.ID, "", errNoLogDirs)
		}
	}

	return dirs
}
