package collector

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

// collectCluster issues the single metadata request the cycle is built on, and
// hands back the raw topic details so the topics phase need not ask again.
func (c *Collector) collectCluster(ctx context.Context) (metrics.ClusterMetrics, kadm.TopicDetails, *section) {
	sec := c.newSection(sectionCluster)
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

// collectTopics samples log start offsets, last stable offsets and high
// watermarks for every topic that survives the filter.
//
// PHASE ORDER HERE IS A CORRECTNESS CONSTRAINT, NOT AN OPTIMISATION. The three
// listings are read at different instants and every quantity derived from them
// is a difference, so the sampling order decides the sign of the residual skew:
//
//	start  <=  committed  <=  LSO  <=  high watermark
//
//	consumer overrun    committed < start   needs start before committed
//	classic lag         end - committed     needs committed before end
//	read_committed lag  lso - committed     needs committed before LSO
//	transaction backlog end - lso           needs LSO before end
//
// Only this order keeps all four non-negative at once. Running the LSO call
// concurrently with, or after, ListEndOffsets would let the backlog go negative
// and a hung transaction would silently read as "not hung".
//
// before are the phases whose own samples must precede the two ceilings: the
// committed offsets, and — when it runs — the throughput window, whose
// ListOffsetsAfterMilli is the earlier edge of a record count that ends at the
// high watermark. Each is a channel the owning goroutine closes when its
// request has returned.
func (c *Collector) collectTopics(ctx context.Context, tds kadm.TopicDetails, before ...<-chan struct{}) ([]metrics.TopicMetrics, topicSections) {
	sec := c.newSection(sectionTopics)
	defer sec.stop()

	// skipped builds the two later sections for a path that never reached them.
	// Every return below carries all three, so a backend can always tell "not
	// collected" from "collected, empty".
	skipped := func() topicSections {
		lso, end := c.newSection(sectionTopicsLSO), c.newSection(sectionTopicsEnd)
		lso.downgrade(metrics.SectionSkipped)
		end.downgrade(metrics.SectionSkipped)
		lso.stop()
		end.stop()
		return topicSections{starts: sec, lso: lso, end: end}
	}

	if tds == nil {
		// Cluster metadata failed: "skipped" is not "no topics".
		sec.downgrade(metrics.SectionSkipped)
		return nil, skipped()
	}

	selected, droppedTopics := selectTopics(tds, c.topics, c.opts.IncludeInternalTopics, c.limits.MaxTopics)
	if droppedTopics > 0 {
		sec.dropped.Topics = droppedTopics
		sec.truncated = true
	}
	if len(selected) == 0 {
		// Never fall through to a bare List*Offsets: with no topic arguments
		// kadm lists the entire cluster, which is the opposite of a filter.
		return nil, skipped()
	}

	names := make([]string, 0, len(selected))
	for _, td := range selected {
		names = append(names, td.Topic)
	}

	starts, err := c.client.Admin.ListStartOffsets(ctx, names...)
	startsOK := sec.requestPartial("ListStartOffsets", err, len(starts) > 0)

	for _, done := range before {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}

	// Last stable offsets, strictly between the committed sample and the high
	// watermarks. ListCommittedOffsets is ListOffsets at isolation level
	// READ_COMMITTED: a round trip, no new permission.
	//
	// lsoSec and endSec are stamped immediately before their own request, not at
	// entry, so SampledAt is the true sample instant of the offsets they carry.
	lsoSec := c.newSection(sectionTopicsLSO)
	var (
		lsos   kadm.ListedOffsets
		lsosOK bool
	)
	if c.opts.CollectLastStableOffset {
		lsos, err = c.client.Admin.ListCommittedOffsets(ctx, names...)
		lsosOK = lsoSec.requestPartial("ListCommittedOffsets", err, len(lsos) > 0)
	} else {
		lsoSec.downgrade(metrics.SectionSkipped)
	}
	lsoSec.stop()

	endSec := c.newSection(sectionTopicsEnd)
	ends, err := c.client.Admin.ListEndOffsets(ctx, names...)
	endsOK := endSec.requestPartial("ListEndOffsets", err, len(ends) > 0)
	endSec.stop()

	topics := make([]metrics.TopicMetrics, 0, len(selected))
	for _, td := range selected {
		topics = append(topics, c.buildTopic(td, sec,
			offsetSample{api: "ListStartOffsets", sec: sec, listed: starts, ok: startsOK},
			offsetSample{api: "ListCommittedOffsets", sec: lsoSec, listed: lsos, ok: lsosOK},
			offsetSample{api: "ListEndOffsets", sec: endSec, listed: ends, ok: endsOK},
		))
	}

	return topics, topicSections{starts: sec, lso: lsoSec, end: endSec}
}

// topicSections are the three sections the topics phase emits, in the order
// their offsets were sampled. They are named rather than a slice because Collect
// splices the throughput window between the first two, where it was sampled.
type topicSections struct {
	starts *section
	lso    *section
	end    *section
}

// offsetSample is one List*Offsets result together with the section that owns
// its errors and whether the result is usable at all. It keeps the null-vs-zero
// and error-attribution rules identical across the three offset phases.
type offsetSample struct {
	api    string
	sec    *section
	listed kadm.ListedOffsets
	ok     bool

	// here is set per topic by enter. It is separate from ok because a missing
	// topic is recorded once per topic, not once per partition.
	here bool
}

// enter resolves whether this sample covers the topic, recording one
// topic-scoped error if not.
func (s *offsetSample) enter(topic string) {
	s.here = s.ok && topicListed(s.sec, s.api, s.listed, topic)
}

// lookup maps one partition onto the nullable wire field. An unusable sample
// yields nil with no error: "the phase did not run" is already in the section
// status, and recording it per partition would bury the real failures.
func (s *offsetSample) lookup(topic string, partition int32) (*int64, error) {
	if !s.here {
		return nil, nil
	}
	return lookupOffset(s.sec, s.api, s.listed, topic, partition)
}

// selectTopics applies the internal-topic rule, the regex filter and maxTopics,
// in that order, over kadm's deterministic Sorted() order.
//
// The order matters: a topic the operator already excluded by regex must not
// consume cap budget. Truncation is a stable prefix, so the same topics are kept
// every cycle and the backend does not see churn that looks like a cluster
// event. maxTopics reduces broker load only because it is applied here, before
// the List*Offsets name list is built.
func selectTopics(tds kadm.TopicDetails, f *filter, includeInternal bool, maxTopics int) (selected []kadm.TopicDetail, dropped int) {
	selected = make([]kadm.TopicDetail, 0, len(tds))
	for _, td := range tds.Sorted() {
		if td.IsInternal && !includeInternal {
			continue
		}
		if !f.allow(td.Topic) {
			continue
		}
		selected = append(selected, td)
	}
	keep, dropped := capLen(len(selected), maxTopics)
	return selected[:keep], dropped
}

// buildTopic shapes one topic's metrics and records its per-partition failures.
// It touches no Kafka client, so the truncation invariants below are reachable
// from a test.
func (c *Collector) buildTopic(td kadm.TopicDetail, sec *section, starts, lsos, ends offsetSample) metrics.TopicMetrics {
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
	// Both summaries are computed over the FULL partition slice, before the cap:
	// PartitionCount stays true so len(Partitions) < PartitionCount is the
	// truncation signal, and a minimum over an arbitrary prefix would make a
	// truncated topic report a durability floor it does not have.
	tm.PartitionCount = len(parts)
	tm.ReplicationFactor = replicationFactor(parts)

	keep, dropped := capLen(len(parts), c.limits.MaxPartitionsPerTopic)
	if dropped > 0 {
		sec.dropped.Partitions += dropped
		sec.truncated = true
	}
	emit := parts[:keep]

	// Once per topic: a topic missing from a listing is one error, not
	// len(partitions) of them.
	starts.enter(td.Topic)
	lsos.enter(td.Topic)
	ends.enter(td.Topic)

	tm.Partitions = make([]metrics.Partition, 0, len(emit))
	for _, p := range emit {
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
		// The FIRST failure wins the partition's error code: a partition whose
		// leader is gone fails every subsequent phase for the same reason.
		var err error
		part.StartOffset, err = starts.lookup(td.Topic, p.Partition)
		if err != nil && part.ErrorCode == 0 {
			part.ErrorCode = errorCode(err)
		}
		part.LastStableOffset, err = lsos.lookup(td.Topic, p.Partition)
		if err != nil && part.ErrorCode == 0 {
			part.ErrorCode = errorCode(err)
		}
		part.EndOffset, err = ends.lookup(td.Topic, p.Partition)
		if err != nil && part.ErrorCode == 0 {
			part.ErrorCode = errorCode(err)
		}
		tm.Partitions = append(tm.Partitions, part)
	}
	return tm
}

// replicationFactor is the minimum replica count across partitions.
//
// Deliberately not "the replica count of partition 0": kadm keys partitions by
// a map, so any single sample is whichever one Go's iteration order hit first,
// and during a reassignment partitions genuinely disagree. Partitions that
// failed to load carry no replica list and are skipped rather than dragging the
// answer to zero.
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

// topicListed reports whether the listing contains the topic at all.
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
// committed offsets without falling back on the wrong "__" prefix guess.
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
