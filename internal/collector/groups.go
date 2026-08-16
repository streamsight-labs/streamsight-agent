package collector

import (
	"context"
	"time"

	"kafka-metrics-agent/internal/metrics"
)

// listGroups performs the one all-broker ListGroups broadcast of the cycle.
// Both the groups and offsets phases consume its result; issuing it twice
// doubled the fan-out for no new information.
//
// The error is returned rather than recorded so each consuming section can
// attribute it to itself — a listing failure degrades both.
func (c *Collector) listGroups(ctx context.Context) ([]string, error) {
	listed, err := c.client.Admin.ListGroups(ctx, c.opts.GroupStates...)

	ids := make([]string, 0, len(listed))
	for _, g := range listed.Sorted() {
		if isInternalGroup(g.Group) {
			continue
		}
		if !c.groups.allow(g.Group) {
			continue
		}
		ids = append(ids, g.Group)
	}
	return ids, err
}

// collectGroups describes every listed group. listedAt is when the shared
// ListGroups call was issued, so the section's SampledAt covers the whole
// phase including the listing.
func (c *Collector) collectGroups(ctx context.Context, ids []string, listErr error, listedAt time.Time) ([]metrics.GroupMetrics, *section) {
	sec := newSectionAt(sectionGroups, listedAt)
	defer sec.stop()

	sec.request("ListGroups", listErr)
	if len(ids) == 0 {
		return nil, sec
	}

	described, err := c.client.Admin.DescribeGroups(ctx, ids...)
	// A shard failure still returns the groups whose coordinators answered.
	if !sec.request("DescribeGroups", err) && len(described) == 0 {
		return nil, sec
	}

	groups := make([]metrics.GroupMetrics, 0, len(described))
	for _, g := range described.Sorted() {
		gm := metrics.GroupMetrics{
			ID:           g.Group,
			State:        g.State,
			Coordinator:  g.Coordinator.NodeID,
			Protocol:     g.Protocol,
			ProtocolType: g.ProtocolType,
			// -1 until a member's join metadata proves otherwise. DescribeGroups
			// itself carries no generation.
			Generation: -1,
		}
		if g.Err != nil {
			// Emit the group anyway, carrying its error code: a coordinator
			// that will not answer is not a group that was deleted.
			gm.ErrorCode = errorCode(g.Err)
			sec.recordGroup("DescribeGroups", g.Group, g.Err)
		}

		members := make([]metrics.GroupMember, 0, len(g.Members))
		for _, m := range g.Members {
			member := metrics.GroupMember{
				MemberID:   m.MemberID,
				InstanceID: m.InstanceID,
				ClientID:   m.ClientID,
				Host:       m.ClientHost,
				Assignment: make([]metrics.TopicPartition, 0),
			}

			// Join metadata: what the member asked for, plus the generation and
			// what it still claims to own under cooperative rebalancing.
			if join, ok := m.Join.AsConsumer(); ok {
				member.SubscribedTopics = join.Topics
				member.Rack = join.Rack
				if join.Generation > gm.Generation {
					gm.Generation = join.Generation
				}
				for _, owned := range join.OwnedPartitions {
					for _, p := range owned.Partitions {
						member.Owned = append(member.Owned, metrics.TopicPartition{
							Topic:     owned.Topic,
							Partition: p,
						})
					}
				}
			}

			// Assignment: what the leader handed it.
			if assigned, ok := m.Assigned.AsConsumer(); ok {
				for _, t := range assigned.Topics {
					for _, p := range t.Partitions {
						member.Assignment = append(member.Assignment, metrics.TopicPartition{
							Topic:     t.Topic,
							Partition: p,
						})
					}
				}
			}

			members = append(members, member)
		}

		gm.MemberCount = len(members)
		gm.Members = members
		groups = append(groups, gm)
	}

	return groups, sec
}
