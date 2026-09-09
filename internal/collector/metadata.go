package collector

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

// collectCluster issues the single metadata request the cycle is built on, and
// hands back the raw topic details so the topics phase need not ask again.
func (c *Collector) collectCluster(ctx context.Context) (*metrics.ClusterMetrics, kadm.TopicDetails, *section) {
	sec := c.newSection(sectionCluster)
	defer sec.stop()

	meta, err := c.client.Metadata(ctx)
	if !sec.request("Metadata", err) {
		// Nil, not a zero struct: see metrics.Batch.Cluster. An empty
		// ClusterMetrics would ship broker_count 0 as though it were measured.
		return nil, nil, sec
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

	// meta.Controller is deliberately not carried. On KRaft the broker answers
	// MetadataResponse.controller_id from getRandomAliveBrokerId -- a random live
	// broker chosen so pre-KIP-500 clients have somewhere to route admin
	// requests, NOT the quorum leader. Measured on this project's own captures it
	// changes on roughly two of every three consecutive healthy batches, which at
	// a 5s interval is ~11,500 fabricated controller-change events a day. It was
	// also the only field making the cluster section non-static. If a real
	// controller identity is ever wanted, DescribeQuorum (KIP-642) has it.
	cluster := &metrics.ClusterMetrics{
		ID:          meta.Cluster,
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
func (c *Collector) collectTopics(ctx context.Context, tds kadm.TopicDetails, runMaxTS bool, before ...<-chan struct{}) ([]metrics.TopicMetrics, topicSections) {
	sec := c.newSection(sectionTopics)
	defer sec.stop()

	// skipped builds the five later sections for a path that never reached them.
	// Every return below carries all six, so a backend can always tell "not
	// collected" from "collected, empty" -- and so that a section never arrives
	// nameless, which is what a nil *section serialises to.
	skipped := func() topicSections {
		out := topicSections{starts: sec}
		for _, p := range []struct {
			name string
			dst  **section
		}{
			{sectionTopicsLSO, &out.lso},
			{sectionTopicsEnd, &out.end},
			{sectionTopicsMaxTS, &out.maxTS},
			{sectionTopicsLocal, &out.local},
			{sectionTopicsRemote, &out.remote},
		} {
			sub := c.newSection(p.name)
			sub.downgrade(metrics.SectionSkipped)
			sub.stop()
			*p.dst = sub
		}
		return out
	}

	if tds == nil {
		// Cluster metadata failed: "skipped" is not "no topics".
		sec.downgrade(metrics.SectionSkipped)
		return nil, skipped()
	}

	selected := selectTopics(tds, c.topics, c.opts.IncludeInternalTopics)
	if len(selected) == 0 {
		// Never fall through to a bare List*Offsets: with no topic arguments
		// kadm lists the entire cluster, which is the opposite of a filter.
		return nil, skipped()
	}

	names := make([]string, 0, len(selected))
	for _, td := range selected {
		names = append(names, td.Topic)
	}

	starts, err := c.client.ListStartOffsets(ctx, names...)
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
		lsos, err = c.client.ListCommittedOffsets(ctx, names...)
		lsosOK = lsoSec.requestPartial("ListCommittedOffsets", err, len(lsos) > 0)
	} else {
		lsoSec.downgrade(metrics.SectionSkipped)
	}
	lsoSec.stop()

	endSec := c.newSection(sectionTopicsEnd)
	ends, err := c.client.ListEndOffsets(ctx, names...)
	endsOK := endSec.requestPartial("ListEndOffsets", err, len(ends) > 0)
	endSec.stop()

	// The three flavours below are OUTSIDE the start <= committed <= LSO <= end
	// chain and must stay after it. They are the same ListOffsets API key with a
	// different timestamp sentinel, so they need no permission the four above did
	// not already need -- but each costs a round trip, and issuing any of them
	// earlier would inflate the very sample latency topics_end exists to pin
	// down.
	maxTSSec := c.newSection(sectionTopicsMaxTS)
	var (
		maxTS   kadm.ListedOffsets
		maxTSOK bool
	)
	// Cadenced, not per-cycle: see Options.MaxTimestampEvery. The section is
	// still emitted on the cycles it skips, so "not sampled this cycle" and
	// "sampled, the broker had nothing" stay different answers.
	if runMaxTS {
		maxTS, err = c.client.ListMaxTimestampOffsets(ctx, names...)
		maxTSOK = maxTSSec.requestPartial("ListMaxTimestampOffsets", err, len(maxTS) > 0)
	} else {
		maxTSSec.downgrade(metrics.SectionSkipped)
	}
	maxTSSec.stop()

	localSec := c.newSection(sectionTopicsLocal)
	var (
		locals   kadm.ListedOffsets
		localsOK bool
	)
	if c.opts.CollectTieredOffsets {
		locals, err = c.client.ListLocalLogStartOffsets(ctx, names...)
		localsOK = localSec.requestPartial("ListLocalLogStartOffsets", err, len(locals) > 0)
	} else {
		localSec.downgrade(metrics.SectionSkipped)
	}
	localSec.stop()

	// Gated separately from local starts: KIP-1005 landed five releases after
	// KIP-405, so a 3.4-3.8 cluster serves -4 and not -5.
	remoteSec := c.newSection(sectionTopicsRemote)
	var (
		remotes   kadm.ListedOffsets
		remotesOK bool
	)
	if c.opts.CollectTieredOffsets && c.opts.CollectLatestTiered {
		remotes, err = c.client.ListLatestRemoteOffsets(ctx, names...)
		remotesOK = remoteSec.requestPartial("ListLatestRemoteOffsets", err, len(remotes) > 0)
	} else {
		remoteSec.downgrade(metrics.SectionSkipped)
	}
	remoteSec.stop()

	topics := make([]metrics.TopicMetrics, 0, len(selected))
	for _, td := range selected {
		topics = append(topics, c.buildTopic(td, sec,
			offsetSample{api: "ListStartOffsets", sec: sec, listed: starts, ok: startsOK},
			offsetSample{api: "ListCommittedOffsets", sec: lsoSec, listed: lsos, ok: lsosOK},
			offsetSample{api: "ListEndOffsets", sec: endSec, listed: ends, ok: endsOK},
			offsetSample{api: "ListMaxTimestampOffsets", sec: maxTSSec, listed: maxTS, ok: maxTSOK},
			offsetSample{api: "ListLocalLogStartOffsets", sec: localSec, listed: locals, ok: localsOK},
			offsetSample{api: "ListLatestRemoteOffsets", sec: remoteSec, listed: remotes, ok: remotesOK},
		))
	}

	return topics, topicSections{
		starts: sec, lso: lsoSec, end: endSec,
		maxTS: maxTSSec, local: localSec, remote: remoteSec,
	}
}

// topicSections are the three sections the topics phase emits, in the order
// their offsets were sampled. They are named rather than a slice because Collect
// splices the throughput window between the first two, where it was sampled.
type topicSections struct {
	starts *section
	lso    *section
	end    *section
	maxTS  *section
	local  *section
	remote *section
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

// selectTopics applies the internal-topic rule and the regex filter over kadm's
// deterministic Sorted() order.
//
// Sorted, not map order: the emitted order must be identical cycle to cycle or a
// backend diffing consecutive batches sees churn that looks like a cluster event.
//
// Nothing here caps the result. The filter is the only thing that narrows it, and
// it reduces broker load rather than just payload because it runs HERE -- before
// the List*Offsets name list and the DescribeLogDirs partition set are built.
func selectTopics(tds kadm.TopicDetails, f *filter, includeInternal bool) []kadm.TopicDetail {
	selected := make([]kadm.TopicDetail, 0, len(tds))
	for _, td := range tds.Sorted() {
		if td.IsInternal && !includeInternal {
			continue
		}
		if !f.allow(td.Topic) {
			continue
		}
		selected = append(selected, td)
	}
	return selected
}

// buildTopic shapes one topic's metrics and records its per-partition failures.
// It touches no Kafka client, so the truncation invariants below are reachable
// from a test.
func (c *Collector) buildTopic(td kadm.TopicDetail, sec *section, starts, lsos, ends, maxTS, locals, remotes offsetSample) metrics.TopicMetrics {
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
	// PartitionCount is the topic's own count rather than len(Partitions), which
	// is now the same number: nothing caps the list. It stays a separate field
	// because a partition whose metadata failed still counts as existing.
	tm.PartitionCount = len(parts)
	tm.ReplicationFactor = replicationFactor(parts)

	emit := parts

	// Once per topic: a topic missing from a listing is one error, not
	// len(partitions) of them.
	starts.enter(td.Topic)
	lsos.enter(td.Topic)
	ends.enter(td.Topic)
	maxTS.enter(td.Topic)
	locals.enter(td.Topic)
	remotes.enter(td.Topic)

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

		// The three below attach as their own nested objects rather than as more
		// fields on Partition, so that "the phase did not run" is an absent key
		// and cannot be confused with "the broker answered null".
		if maxTS.here {
			part.MaxTimestamp = maxTS.maxTimestamp(td.Topic, p.Partition)
		}
		if locals.here || remotes.here {
			t := &metrics.TieredOffsets{}
			t.LocalStartOffset, _ = locals.lookup(td.Topic, p.Partition)
			t.RemoteEndOffset, _ = remotes.lookup(td.Topic, p.Partition)
			part.Tiered = t
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

// maxTimestamp resolves one partition's ListOffsets(-3) answer.
//
// It cannot reuse lookup because this is the one offset flavour whose timestamp
// is the point: lookup returns only the offset, and a max-timestamp offset
// without its timestamp answers nothing. A partition holding no records reports
// offset -1 and timestamp -1, which both travel as null rather than as a real
// offset zero at the epoch.
func (s *offsetSample) maxTimestamp(topic string, partition int32) *metrics.MaxTimestampOffset {
	if !s.here {
		return nil
	}
	lo, ok := s.listed.Lookup(topic, partition)
	if !ok {
		return nil
	}
	out := &metrics.MaxTimestampOffset{LeaderEpoch: lo.LeaderEpoch}
	if lo.Err != nil {
		out.ErrorCode = errorCode(lo.Err)
		s.sec.recordPartition(s.api, topic, partition, lo.Err)
		return out
	}
	if lo.Offset >= 0 {
		offset := lo.Offset
		out.Offset = &offset
	}
	if lo.Timestamp >= 0 {
		ts := lo.Timestamp
		out.TimestampMs = &ts
	}
	return out
}
