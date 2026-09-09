package collector

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

// maxEpochProbeRounds bounds the requests one cycle issues. kadm's request maps
// (topic, partition) to ONE epoch, so two groups that committed the same
// partition at different epochs cannot share a request; each extra distinct
// epoch costs another sharded round. After a single leader election every group
// committed at the same old epoch, so one round is the normal case and a
// cluster needing more than a handful is pathological.
const maxEpochProbeRounds = 4

// maxEpochProbesPerCycle bounds the fan-out. The trigger normally yields
// nothing, but a cluster-wide unclean election yields one probe per (group,
// partition) at once — the one moment the agent must not amplify an incident.
const maxEpochProbesPerCycle = 1024

// maxProbedEpochs bounds the suppression set. Evicting wholesale when it fills
// is deliberate: the alternative is an LRU whose eviction order decides which
// partitions get re-probed, and a periodic clean slate is easier to reason about
// than a silent, order-dependent one.
const maxProbedEpochs = 8192

// epochProbeKey identifies one question already answered. topicID is in the key
// so a deleted-and-recreated topic is asked again rather than inheriting the
// old topic's answer.
type epochProbeKey struct {
	topic     string
	topicID   string
	partition int32
	committed int32
	current   int32
}

// probedEpochs remembers questions already answered, because "the committed
// epoch differs from the current one" is NOT a transient condition.
//
// A committed leader epoch is the epoch of the record AT the committed offset;
// the metadata epoch is the leader's current one. They converge only when the
// group consumes a record produced under the new epoch, so after any leader
// election an idle or abandoned partition mismatches indefinitely — for
// offsets.retention.minutes, seven days by default. Without suppression a
// routine rolling restart would make this phase fan out on every cycle forever,
// re-asking a question the leader has already answered.
type probedEpochs struct {
	mu   sync.Mutex
	seen map[epochProbeKey]struct{}
}

// answered reports whether a leader has already answered this question.
//
// It records NOTHING. The read and the write are separate on purpose: a
// question is only settled once a leader has answered it, and recording at ask
// time would retire a question the agent asked and got nothing back from. The
// write is remember, below, and it runs after the response.
func (p *probedEpochs) answered(k epochProbeKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.seen[k]
	return ok
}

// remember retires a question a leader answered, so later cycles stop asking.
// Idempotent: two groups mismatching identically on one partition ask one
// question and both record it.
func (p *probedEpochs) remember(k epochProbeKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.seen) >= maxProbedEpochs {
		p.seen = nil
	}
	if p.seen == nil {
		p.seen = make(map[epochProbeKey]struct{}, 64)
	}
	p.seen[k] = struct{}{}
}

// errEpochNotReturned is the tell for a partition the request named and the
// response did not: without it a missing answer would be indistinguishable from
// a leader that reported no data loss.
var errEpochNotReturned = errors.New("broker returned no leader epoch offset for partition")

// collectEpochProbes turns committed-vs-current leader-epoch mismatches into
// positive proof of truncation.
//
// THE TRIGGER IS THE DESIGN. A group's committed leader epoch and the
// partition's current one are both already on the wire, and they differ only
// after a leader election touched a partition some group has committed on. In
// steady state they match, this issues NOTHING, and the section is skipped —
// which is the point: an unconditional call would be a per-cycle fan-out to
// every leader for an event that is normally absent.
//
// When they differ, the COMMITTED epoch is requested. If the leader answers
// with an end offset below the committed offset, everything in between both
// existed and was consumed: proof of loss rather than the "high watermark went
// backwards" heuristic, which cannot tell a truncation nobody read from one
// that ate a consumer's records.
//
// It needs no ACL beyond DESCRIBE on TOPIC, which Metadata already requires;
// run gates it on kafka.Capabilities.SupportsOffsetForLeaderEpoch.
func (c *Collector) collectEpochProbes(ctx context.Context, run bool, topics []metrics.TopicMetrics, offsets []metrics.ConsumerOffset) ([]metrics.EpochProbe, *section) {
	sec := c.newSection(sectionEpochProbes)
	defer sec.stop()

	if !run {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	triggers, dropped := epochTriggers(topics, offsets, maxEpochProbesPerCycle)
	if dropped {
		sec.truncated = true
	}
	// Drop the questions a leader already answered in an earlier cycle. Without
	// this the phase re-asks about every idle partition after any leader
	// election, for as long as the group's offsets are retained. This is a READ:
	// the matching write happens after the response, so a question that goes
	// unasked or unanswered survives to the next cycle.
	fresh := triggers[:0]
	for _, t := range triggers {
		if !c.probed.answered(t.probeKey()) {
			fresh = append(fresh, t)
		}
	}
	triggers = fresh
	if len(triggers) == 0 {
		// Skipped, not ok-and-empty: nothing was asked, so absence of probes is
		// not evidence about the cluster.
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	rounds, roundDropped := epochRounds(triggers)
	if roundDropped {
		sec.truncated = true
	}

	asked := make(map[epochKey]epochAnswer, len(triggers))
	for _, round := range rounds {
		resp, err := c.client.OffsetForLeaderEpoch(ctx, round.req)
		// kadm aborts the whole response on the first partition-level
		// authorization failure while keeping the topics it had already decoded,
		// and a shard failure leaves the leaders that did answer in the map.
		ok := sec.requestPartial("OffsetForLeaderEpoch", err, len(resp) > 0)
		for _, k := range round.keys {
			o, found := resp[k.topic][k.partition]
			asked[k] = epochAnswer{offset: o, found: found, usable: ok}
		}
	}

	probes := make([]metrics.EpochProbe, 0, len(triggers))
	for _, t := range triggers {
		// A trigger the round cap refused was never asked, so it gets no row:
		// an empty probe would read as "the leader had nothing to say". It is
		// also not remembered — it is still an open question.
		a, ok := asked[t.key()]
		if !ok {
			continue
		}
		probes = append(probes, t.probe(sec, a))
		if a.settles() {
			c.probed.remember(t.probeKey())
		}
	}
	return probes, sec
}

// epochAnswer is one question's outcome: usable is false when the whole request
// failed, found false when it succeeded without naming this partition.
type epochAnswer struct {
	offset kadm.OffsetForLeaderEpoch
	found  bool
	usable bool
}

// settles reports whether this answer retires the question for good.
//
// Only a leader that answered THIS partition without a partition-level error
// does. The three cases it excludes — the request failed, the response omitted
// the partition, the leader returned an error code — are exactly what a leader
// election in flight produces, which is the one moment this phase must not go
// blind. Note a LeaderEpoch of -1 DOES settle: "I do not know that epoch" is an
// answer, and re-asking cannot change it.
//
// The asymmetry is deliberate. Re-asking costs one request on a later cycle;
// retiring a question the leader never answered costs the only positive proof
// of truncation the agent can obtain, permanently, because the mismatch that
// triggers it does not resolve on its own — see probedEpochs.
func (a epochAnswer) settles() bool {
	return a.usable && a.found && a.offset.Err == nil
}

// epochKey identifies one question: an epoch asked about for one partition.
type epochKey struct {
	topic     string
	partition int32
	epoch     int32
}

// epochTrigger is one group's mismatch on one partition.
type epochTrigger struct {
	topic     string
	topicID   string
	partition int32
	group     string

	committedOffset *int64
	committedEpoch  int32
	currentEpoch    int32
}

func (t epochTrigger) key() epochKey {
	return epochKey{topic: t.topic, partition: t.partition, epoch: t.committedEpoch}
}

// probe shapes one result. When the whole request failed, every answer field
// stays null and NO per-partition error is recorded: sec.request attributed the
// failure once already, and repeating it per partition would bury it.
func (t epochTrigger) probe(sec *section, a epochAnswer) metrics.EpochProbe {
	p := metrics.EpochProbe{
		Topic:              t.topic,
		TopicID:            t.topicID,
		Partition:          t.partition,
		GroupID:            t.group,
		CommittedOffset:    t.committedOffset,
		CommittedEpoch:     t.committedEpoch,
		CurrentLeaderEpoch: t.currentEpoch,
	}
	if !a.usable {
		return p
	}
	if !a.found {
		sec.recordPartition("OffsetForLeaderEpoch", t.topic, t.partition, errEpochNotReturned)
		return p
	}

	o := a.offset
	node := o.NodeID
	p.NodeID = &node
	if o.Err != nil {
		p.ErrorCode = errorCode(o.Err)
		sec.recordPartition("OffsetForLeaderEpoch", t.topic, t.partition, o.Err)
		return p
	}

	// -1 is a real answer, not a missing one: the leader does not know the epoch
	// that was asked about, and the end offset beside it proves nothing. Only a
	// probe with no answer at all reports null.
	epoch := o.LeaderEpoch
	p.ReturnedEpoch = &epoch
	p.EndOffset = nullableOffset(o.EndOffset, nil)
	return p
}

// epochTriggers pairs every committed offset against its partition's current
// leader epoch and keeps the mismatches.
//
// Both epochs are read from what the batch is already shipping — topics[] and
// offsets[] — rather than from the raw metadata, so every probe has a join
// partner in the same batch and a capped topic or partition list cannot produce
// a probe for a partition the backend never received.
// probeKey is the suppression identity: the exact question this trigger asks.
func (t epochTrigger) probeKey() epochProbeKey {
	return epochProbeKey{
		topic:     t.topic,
		topicID:   t.topicID,
		partition: t.partition,
		committed: t.committedEpoch,
		current:   t.currentEpoch,
	}
}

func epochTriggers(topics []metrics.TopicMetrics, offsets []metrics.ConsumerOffset, max int) (triggers []epochTrigger, dropped bool) {
	if len(topics) == 0 || len(offsets) == 0 {
		return nil, false
	}

	type partition struct {
		topicID string
		epoch   int32
	}
	current := make(map[string]map[int32]partition, len(topics))
	for _, t := range topics {
		parts := make(map[int32]partition, len(t.Partitions))
		for _, p := range t.Partitions {
			parts[p.ID] = partition{topicID: t.ID, epoch: p.LeaderEpoch}
		}
		current[t.Name] = parts
	}

	for _, co := range offsets {
		for _, o := range co.Offsets {
			// A group that never committed here proves nothing, and an unknown
			// committed epoch (-1, the pre-KIP-320 case) can never be asked
			// about — the request would name an epoch the leader cannot resolve.
			if o.Offset == nil || o.LeaderEpoch < 0 {
				continue
			}
			p, ok := current[o.Topic][o.Partition]
			if !ok || p.epoch < 0 || p.epoch == o.LeaderEpoch {
				continue
			}
			triggers = append(triggers, epochTrigger{
				topic:           o.Topic,
				topicID:         p.topicID,
				partition:       o.Partition,
				group:           co.GroupID,
				committedOffset: o.Offset,
				committedEpoch:  o.LeaderEpoch,
				currentEpoch:    p.epoch,
			})
		}
	}

	// Sorted before the cap so the retained prefix is the same set every cycle
	// while the mismatch persists, rather than whichever partitions Go's map
	// iteration reached first.
	sort.Slice(triggers, func(i, j int) bool {
		a, b := triggers[i], triggers[j]
		if a.topic != b.topic {
			return a.topic < b.topic
		}
		if a.partition != b.partition {
			return a.partition < b.partition
		}
		return a.group < b.group
	})

	keep, cut := capLen(len(triggers), max)
	return triggers[:keep], cut > 0
}

// epochRound is one OffsetForLeaderEpoch request and the questions it carries.
type epochRound struct {
	req  kadm.OffsetForLeaderEpochRequest
	keys []epochKey
}

// epochRounds packs the triggers into as few requests as the protocol allows.
//
// kadm.OffsetForLeaderEpochRequest is map[topic]map[partition]epoch, so one
// request can ask exactly one epoch per partition. Triggers agreeing on the
// epoch — the normal case, since one election moves every group on the
// partition at once — collapse into a single question; a partition needing a
// second distinct epoch spills into the next round. Rounds past
// maxEpochProbeRounds are refused rather than fanned out.
func epochRounds(triggers []epochTrigger) (rounds []epochRound, dropped bool) {
	for _, t := range triggers {
		k := t.key()
		placed := false
		for i := range rounds {
			e, taken := rounds[i].req[k.topic][k.partition]
			if taken && e != k.epoch {
				continue
			}
			// Already asked in this round by another group: one question, many
			// rows.
			if !taken {
				rounds[i].req.Add(k.topic, k.partition, k.epoch)
				rounds[i].keys = append(rounds[i].keys, k)
			}
			placed = true
			break
		}
		if placed {
			continue
		}
		if len(rounds) >= maxEpochProbeRounds {
			dropped = true
			continue
		}
		var round epochRound
		round.req.Add(k.topic, k.partition, k.epoch)
		round.keys = append(round.keys, k)
		rounds = append(rounds, round)
	}
	return rounds, dropped
}
