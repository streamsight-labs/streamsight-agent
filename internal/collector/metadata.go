package collector

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

// collectCluster issues the single metadata request the cycle is built on. It
// also hands back the raw topic details so the topics phase does not have to
// ask again.
func (c *Collector) collectCluster(ctx context.Context) (metrics.ClusterMetrics, kadm.TopicDetails, *section) {
	sec := newSection(sectionCluster)
	defer sec.stop()

	meta, err := c.client.Admin.Metadata(ctx)
	if !sec.request("Metadata", err) {
		return metrics.ClusterMetrics{}, nil, sec
	}

	brokers := make([]metrics.Broker, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		brokers = append(brokers, metrics.Broker{
			ID:   b.NodeID,
			Host: b.Host,
			Port: b.Port,
			Rack: b.Rack,
		})
	}

	cluster := metrics.ClusterMetrics{
		ID:          meta.Cluster,
		Controller:  meta.Controller,
		BrokerCount: len(brokers),
		Brokers:     brokers,
	}
	return cluster, meta.Topics, sec
}

// collectTopics samples log start offsets and high watermarks for every topic
// that survives the filter.
//
// It blocks on committed before listing end offsets: the two are read from
// different brokers at different instants, and lag is end-committed, so
// sampling the committed side first keeps the residual error positive.
func (c *Collector) collectTopics(ctx context.Context, tds kadm.TopicDetails, committed <-chan struct{}) ([]metrics.TopicMetrics, []*section) {
	sec := newSection(sectionTopics)
	defer sec.stop()

	// endSec is stamped later, immediately before ListEndOffsets, so its
	// SampledAt is the true sample time of every high watermark below.
	var endSec *section

	if tds == nil {
		// Cluster metadata failed; there is nothing to sample. "skipped" is
		// not "no topics".
		sec.downgrade(metrics.SectionSkipped)
		endSec = newSection(sectionTopicsEnd)
		endSec.downgrade(metrics.SectionSkipped)
		endSec.stop()
		return nil, []*section{sec, endSec}
	}

	selected := make([]kadm.TopicDetail, 0, len(tds))
	for _, td := range tds.Sorted() {
		if td.IsInternal && !c.opts.IncludeInternalTopics {
			continue
		}
		if !c.topics.allow(td.Topic) {
			continue
		}
		selected = append(selected, td)
	}
	if len(selected) == 0 {
		// Never fall through to a bare List*Offsets: with no topic arguments
		// kadm lists the entire cluster, which is the opposite of a filter.
		endSec = newSection(sectionTopicsEnd)
		endSec.stop()
		return nil, []*section{sec, endSec}
	}

	names := make([]string, 0, len(selected))
	for _, td := range selected {
		names = append(names, td.Topic)
	}

	starts, err := c.client.Admin.ListStartOffsets(ctx, names...)
	startsOK := sec.request("ListStartOffsets", err)

	select {
	case <-committed:
	case <-ctx.Done():
	}

	endSec = newSection(sectionTopicsEnd)
	ends, err := c.client.Admin.ListEndOffsets(ctx, names...)
	endsOK := endSec.request("ListEndOffsets", err)
	endSec.stop()

	topics := make([]metrics.TopicMetrics, 0, len(selected))
	for _, td := range selected {
		tm := metrics.TopicMetrics{
			Name:     td.Topic,
			Internal: td.IsInternal,
		}
		if td.ID != (kadm.TopicID{}) {
			tm.ID = td.ID.String()
		}
		if td.Err != nil {
			tm.ErrorCode = errorCode(td.Err)
			sec.recordTopic("Metadata", td.Topic, td.Err)
		}

		parts := td.Partitions.Sorted()
		tm.PartitionCount = len(parts)
		tm.ReplicationFactor = replicationFactor(parts)

		startsHere := startsOK && topicListed(sec, "ListStartOffsets", starts, td.Topic)
		endsHere := endsOK && topicListed(endSec, "ListEndOffsets", ends, td.Topic)

		tm.Partitions = make([]metrics.Partition, 0, len(parts))
		for _, p := range parts {
			part := metrics.Partition{
				ID:              p.Partition,
				Leader:          p.Leader,
				LeaderEpoch:     p.LeaderEpoch,
				Replicas:        p.Replicas,
				ISR:             p.ISR,
				OfflineReplicas: p.OfflineReplicas,
			}
			if p.Err != nil {
				part.ErrorCode = errorCode(p.Err)
				sec.recordPartition("Metadata", td.Topic, p.Partition, p.Err)
			}
			if startsHere {
				var err error
				part.StartOffset, err = lookupOffset(sec, "ListStartOffsets", starts, td.Topic, p.Partition)
				if err != nil && part.ErrorCode == 0 {
					part.ErrorCode = errorCode(err)
				}
			}
			if endsHere {
				var err error
				part.EndOffset, err = lookupOffset(endSec, "ListEndOffsets", ends, td.Topic, p.Partition)
				if err != nil && part.ErrorCode == 0 {
					part.ErrorCode = errorCode(err)
				}
			}
			tm.Partitions = append(tm.Partitions, part)
		}

		topics = append(topics, tm)
	}

	return topics, []*section{sec, endSec}
}

// replicationFactor is the minimum replica count across partitions.
//
// It is deliberately not "the replica count of partition 0": kadm keys
// partitions by a map, so any single sample is whichever partition Go's
// iteration order hit first, and during a reassignment partitions genuinely
// disagree. The minimum is the number that matters for durability. Partitions
// that failed to load carry no replica list and are skipped rather than
// dragging the answer to zero.
func replicationFactor(parts []kadm.PartitionDetail) int {
	lowest := 0
	seen := false
	for _, p := range parts {
		if p.Err != nil {
			continue
		}
		n := len(p.Replicas)
		if !seen || n < lowest {
			lowest, seen = n, true
		}
	}
	return lowest
}

// topicListed reports whether the listing contains the topic at all, recording
// one topic-scoped error instead of one per partition when it does not.
func topicListed(sec *section, api string, listed kadm.ListedOffsets, topic string) bool {
	if _, ok := listed[topic]; ok {
		return true
	}
	sec.recordTopic(api, topic, errTopicNotListed)
	return false
}

// lookupOffset maps one listed offset onto the nullable wire field. A miss
// stays nil and is recorded: an unreachable leader must not be byte-identical
// to an empty partition.
func lookupOffset(sec *section, api string, listed kadm.ListedOffsets, topic string, partition int32) (*int64, error) {
	o, ok := listed.Lookup(topic, partition)
	if !ok {
		sec.recordPartition(api, topic, partition, errNotListed)
		return nil, errNotListed
	}
	if v := nullableOffset(o.Offset, o.Err); v != nil {
		return v, nil
	}
	err := o.Err
	if err == nil {
		err = errNegativeOffset
	}
	sec.recordPartition(api, topic, partition, err)
	return nil, err
}

// internalTopics is the set of broker-flagged internal topics, used to filter
// committed offsets without re-applying the wrong "__" prefix guess.
func internalTopics(tds kadm.TopicDetails) map[string]bool {
	if tds == nil {
		return nil
	}
	internal := make(map[string]bool)
	for name, td := range tds {
		if td.IsInternal {
			internal[name] = true
		}
	}
	return internal
}
