package collector

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// maxReassignPartitions bounds how many partitions one request may name. The
// partition IDs travel in the request body, so a cluster whose every partition
// is under-replicated — a controller failover, a rack outage — would otherwise
// turn the cheapest phase in the cycle into the largest, in exactly the moment
// the rest of the batch matters most. A real reassignment is planned in
// batches far below this.
const maxReassignPartitions = 1000

// collectReassignments asks the controller which of the under-replicated
// partitions are moving on purpose. It is the difference between "a broker is
// failing" and "an operator is rebalancing", which is the largest source of
// false positives in URP alerting.
//
// THE TRIGGER IS THE DESIGN, NOT THE CALL. A partition being reassigned is
// necessarily under-replicated while the new replicas catch up, so the URPs
// already in topics[] are a complete superset of what the controller could
// answer for. In a healthy cluster this phase therefore issues no request at
// all and costs one pass over a slice; an unconditional poll would spend a
// controller round trip every cycle to be told "nothing" forever.
//
// It adds no ACL: the broker gates ListPartitionReassignments on DESCRIBE of
// CLUSTER, which Metadata already needs.
//
// run is false when the phase is disabled or the cluster cannot serve it
// (ApiVersions does not advertise key 46, i.e. below Kafka 2.4). The section is
// built and returned either way, so "not asked" and "asked, nothing moving"
// are never the same batch.
//
// Percent-complete is not computed here and cannot be: it is the adding
// replica's log_dirs size over the leader's, joined at the backend.
func (c *Collector) collectReassignments(ctx context.Context, topics []metrics.TopicMetrics, run bool) ([]metrics.Reassignment, *section) {
	sec := c.newSection(sectionReassignments)
	defer sec.stop()

	if !run {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	set, truncated := underReplicated(topics)
	sec.truncated = truncated
	if len(set) == 0 {
		// The trigger did not fire, which is the steady state. kadm answers an
		// empty TopicsSet with an empty map and issues no request (partas.go),
		// the inverse of the List*Offsets and DescribeLogDirs convention where an
		// empty argument means "the whole cluster" — so this guard is about
		// honesty rather than load: an unasked question must report "skipped",
		// never "ok, nothing is moving".
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	listed, err := c.client.ListPartitionReassignments(ctx, set)
	// One request to one broker, the controller: there are no shards that could
	// leave a partial result behind, and kadm returns a nil map with every
	// error, so requestPartial would have nothing to keep.
	if !sec.request("ListPartitionReassignments", err) {
		return nil, sec
	}

	return buildReassignments(listed), sec
}

// underReplicated builds the query set from the partitions already observed
// under-replicated, in the batch's own order.
//
// The trigger is read off the topics the cycle is SHIPPING, not off raw
// metadata, so every row this phase emits has a join partner in topics[] and
// the two views cannot disagree about which partitions exist. The cost is that
// a partition dropped by MaxPartitionsPerTopic is never asked about, which is
// the same trade the log-dirs phase makes.
func underReplicated(topics []metrics.TopicMetrics) (set kadm.TopicsSet, truncated bool) {
	n := 0
	for _, t := range topics {
		for _, p := range t.Partitions {
			// An empty replica list is a partition whose metadata failed, not a
			// partition with no replicas; comparing lengths would make every one
			// of them look under-replicated and invert the trigger during the
			// exact outage that produces them.
			if len(p.Replicas) == 0 || len(p.ISR) >= len(p.Replicas) {
				continue
			}
			if n >= maxReassignPartitions {
				return set, true
			}
			set.Add(t.Name, p.ID)
			n++
		}
	}
	return set, false
}

// buildReassignments shapes the controller's answer, sorted by topic and
// partition so the same cluster produces the same bytes every cycle.
//
// A row with nothing being added or removed is dropped. The controller answers
// only for partitions with an active reassignment, so such a row asserts a move
// that is not happening; the partition stays visible as a URP in topics[], and
// its absence here is the finding — under-replicated because something failed,
// not because something is moving.
func buildReassignments(listed kadm.ListPartitionReassignmentsResponses) []metrics.Reassignment {
	sorted := listed.Sorted()
	out := make([]metrics.Reassignment, 0, len(sorted))
	for _, r := range sorted {
		if len(r.AddingReplicas) == 0 && len(r.RemovingReplicas) == 0 {
			continue
		}
		// Replica order is the controller's own and carries the preferred leader
		// first, so it is passed through rather than sorted.
		out = append(out, metrics.Reassignment{
			Topic:     r.Topic,
			Partition: r.Partition,
			// Normalised: kmsg leaves a zero-length response array nil, and
			// an add-only move has nothing being removed. Shipping null there
			// would make "none being removed" indistinguishable from "unknown".
			Replicas:         int32sOrEmpty(r.Replicas),
			AddingReplicas:   int32sOrEmpty(r.AddingReplicas),
			RemovingReplicas: int32sOrEmpty(r.RemovingReplicas),
		})
	}
	return out
}

// int32sOrEmpty normalises a nil replica list to an empty one, so an absent list
// on the wire always means "none" rather than "not reported".
func int32sOrEmpty(v []int32) []int32 {
	if v == nil {
		return []int32{}
	}
	return v
}
