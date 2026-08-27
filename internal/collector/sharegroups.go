package collector

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

const (
	apiShareGroupDescribe        = "ShareGroupDescribe"
	apiDescribeShareGroupOffsets = "DescribeShareGroupOffsets"

	// shareGroupType is the ListGroups v5 GroupType value for a KIP-932 share
	// group. The listing is filtered on it rather than describing every group
	// and discarding the misses: ShareGroupDescribe answers with an error for a
	// classic group, and a section full of expected errors is indistinguishable
	// from one full of real ones.
	shareGroupType = "share"
)

// collectShareGroups describes the cluster's KIP-932 share groups.
//
// A separate phase and a separate section from groups[], because a share group
// is a different data model rather than a variant of a consumer group. Members
// do not own partitions -- they share them and acknowledge individual records --
// so there is no committed offset per partition, no assignment to diff between
// cycles, and no lag in the committed-versus-end sense. Every consumer-group
// derivation the backend runs (rebalance churn, assignment imbalance, wedged
// cooperative revocation, burn-down ETA) is unsound here. Folding these into
// groups[] would let all of them run silently against a shape they do not model.
//
// It needs the GroupType from the cycle's existing ListGroups broadcast, which
// is why the agent reads that response raw; see listGroupsWithTypes.
//
// UNTESTED against a real broker. No Kafka 4.x cluster has answered
// ShareGroupDescribe for this agent, the same gap Open item 3 records for
// KIP-848. It defaults off and the capability probe disables it below 4.0, so
// the failure mode on every cluster in existence today is "section: skipped".
func (c *Collector) collectShareGroups(ctx context.Context, types map[string]string, run bool) ([]metrics.ShareGroup, *section) {
	sec := c.newSection(sectionShareGroups)
	defer sec.stop()

	if !run {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	ids := make([]string, 0, len(types))
	for group, t := range types {
		if t == shareGroupType && c.groups.allow(group) && !isInternalGroup(group) {
			ids = append(ids, group)
		}
	}
	if len(ids) == 0 {
		// Not an error and not an empty cluster: on any broker below 4.0 the
		// GroupType field does not exist, so nothing can ever match. The section
		// says "skipped" so that "no share groups" and "cannot see share groups"
		// stay distinguishable.
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}
	sort.Strings(ids)

	keep, dropped := capLen(len(ids), c.limits.MaxGroups)
	if dropped > 0 {
		sec.dropped.Groups += dropped
		sec.truncated = true
	}
	ids = ids[:keep]

	described, err := c.client.Admin.DescribeShareGroups(ctx, ids...)
	if !sec.request(apiShareGroupDescribe, err) && len(described) == 0 {
		return nil, sec
	}

	// Offsets are a second request and are allowed to fail on their own: a
	// described group with no start offsets is still worth shipping, and
	// StartOffsets being empty is covered by the section status.
	offsets, offErr := c.client.Admin.DescribeShareGroupOffsets(ctx, ids...)
	if offErr != nil {
		sec.requestPartial(apiDescribeShareGroupOffsets, offErr, len(described) > 0)
	}

	return buildShareGroups(described, offsets, sec), sec
}

// buildShareGroups shapes the two responses into the wire type. It touches no
// client, so every branch below is reachable from a test -- which is the only
// way any of this is exercised until a 4.x cluster exists to point at.
func buildShareGroups(described kadm.DescribedShareGroups, offsets kadm.DescribedShareGroupsOffsets, sec *section) []metrics.ShareGroup {
	byGroup := map[string]kadm.DescribedShareGroupOffsets{}
	for _, o := range offsets {
		byGroup[o.Group] = o
	}

	out := make([]metrics.ShareGroup, 0, len(described))
	for _, g := range described {
		sg := metrics.ShareGroup{
			ID:              g.GroupID,
			State:           g.GroupState,
			Coordinator:     g.Coordinator.NodeID,
			GroupEpoch:      g.GroupEpoch,
			AssignmentEpoch: g.AssignmentEpoch,
			Assignor:        g.Assignor,
			MemberCount:     len(g.Members),
		}
		if g.Err != nil {
			// Emitted anyway, carrying its code: a coordinator that will not
			// answer is not a group that was deleted.
			sg.ErrorCode = errorCode(g.Err)
			sec.recordGroup(apiShareGroupDescribe, g.GroupID, g.Err)
		}
		sg.AuthorizedOperations = decodedAuthorizedOps(g.AuthorizedOperations, g.Err == nil)

		sg.Members = make([]metrics.ShareGroupMember, 0, len(g.Members))
		for _, m := range g.Members {
			sm := metrics.ShareGroupMember{
				MemberID:         m.MemberID,
				ClientID:         m.ClientID,
				Host:             m.ClientHost,
				Rack:             m.RackID,
				MemberEpoch:      m.MemberEpoch,
				SubscribedTopics: m.SubscribedTopicNames,
			}
			for _, tp := range m.Assignment.Sorted() {
				for _, p := range tp.Partitions {
					sm.Assignment = append(sm.Assignment, metrics.TopicPartition{
						Topic:     tp.Topic,
						Partition: p,
					})
				}
			}
			sg.Members = append(sg.Members, sm)
		}

		o, ok := byGroup[g.GroupID]
		if !ok {
			out = append(out, sg)
			continue
		}
		if o.Err != nil {
			sec.recordGroup(apiDescribeShareGroupOffsets, g.GroupID, o.Err)
			if sg.ErrorCode == 0 {
				sg.ErrorCode = errorCode(o.Err)
			}
		}
		for _, topic := range sortedShareTopics(o.Offsets) {
			for _, partition := range sortedSharePartitions(o.Offsets[topic]) {
				so := o.Offsets[topic][partition]
				row := metrics.ShareGroupOffset{
					Topic:       topic,
					Partition:   partition,
					LeaderEpoch: so.LeaderEpoch,
				}
				if so.Err != nil {
					row.ErrorCode = errorCode(so.Err)
					sec.recordPartition(apiDescribeShareGroupOffsets, topic, partition, so.Err)
				} else if so.StartOffset >= 0 {
					// Same rule as every other offset in this batch: a negative
					// value is not an offset, and must travel as null rather
					// than as a start of zero.
					v := so.StartOffset
					row.StartOffset = &v
				}
				sg.StartOffsets = append(sg.StartOffsets, row)
			}
		}
		out = append(out, sg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func sortedShareTopics(o kadm.ShareOffsets) []string {
	ts := make([]string, 0, len(o))
	for t := range o {
		ts = append(ts, t)
	}
	sort.Strings(ts)
	return ts
}

func sortedSharePartitions(ps map[int32]kadm.ShareOffset) []int32 {
	out := make([]int32, 0, len(ps))
	for p := range ps {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
