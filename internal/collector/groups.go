package collector

import (
	"context"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"kafka-metrics-agent/internal/metrics"
)

// listGroups performs the one all-broker ListGroups broadcast of the cycle,
// shared by the groups and offsets phases. The error is returned rather than
// recorded so each consuming section can attribute it to itself.
//
// MaxGroups is enforced here and nowhere else. One enforcement point shrinks
// both the DescribeGroups and the FetchManyOffsets fan-out, and guarantees
// groups[] and offsets[] describe the SAME set of groups; capping independently
// in each phase would let the two sections disagree about which groups exist —
// a data integrity bug, not a payload one. listed.Sorted() makes the retained
// prefix the same set every cycle.
func (c *Collector) listGroups(ctx context.Context) (ids []string, types map[string]string, dropped int, err error) {
	listed, types, err := c.listGroupsWithTypes(ctx)

	ids = make([]string, 0, len(listed))
	for _, g := range listed {
		if isInternalGroup(g) {
			continue
		}
		if !c.groups.allow(g) {
			continue
		}
		ids = append(ids, g)
	}
	keep, dropped := capLen(len(ids), c.limits.MaxGroups)
	return ids[:keep], types, dropped, err
}

// listGroupsWithTypes issues the ListGroups broadcast this cycle needs anyway,
// and keeps the GroupType field that kadm's ListedGroup discards.
//
// This is NOT an extra request. kadm.ListGroups issues exactly the same
// broadcast; it simply drops GroupType on the floor when converting the
// response, the same way it drops TotalBytes/UsableBytes from DescribeLogDirs
// (T1.9 solves that the same way).
//
// Below ListGroups v5 the field does not exist on the wire and every group maps
// to "". Empty is deliberately NOT "classic": on a 2.6-3.x broker every group IS
// classic, but the agent has not been told so, and a backend that renders the
// two identically would report a completed KIP-848 migration on a cluster that
// cannot run one.
func (c *Collector) listGroupsWithTypes(ctx context.Context) ([]string, map[string]string, error) {
	req := kmsg.NewPtrListGroupsRequest()
	req.StatesFilter = c.opts.GroupStates

	var (
		groups   []string
		types    = map[string]string{}
		firstErr error
	)
	for _, shard := range c.client.RequestSharded(ctx, req) {
		if shard.Err != nil {
			if firstErr == nil {
				firstErr = shard.Err
			}
			continue
		}
		resp, ok := shard.Resp.(*kmsg.ListGroupsResponse)
		if !ok {
			continue
		}
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil && firstErr == nil {
			firstErr = err
		}
		for _, g := range resp.Groups {
			groups = append(groups, g.Group)
			if g.GroupType != "" {
				types[g.Group] = g.GroupType
			}
		}
	}
	sort.Strings(groups)
	return groups, types, firstErr
}

// collectGroups describes every listed group. listedAt is when the shared
// ListGroups call was issued, so SampledAt covers the listing too. listDropped
// is how many groups MaxGroups removed: the count is reported once at batch
// level, but this section must still admit it is short.
func (c *Collector) collectGroups(ctx context.Context, ids []string, types map[string]string, listDropped int, listErr error, listedAt time.Time) ([]metrics.GroupMetrics, *section) {
	sec := c.newSectionAt(sectionGroups, listedAt)
	defer sec.stop()

	if listDropped > 0 {
		sec.truncated = true
	}

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
			// Empty below ListGroups v5, and empty is NOT "classic" -- see
			// listGroupsWithTypes.
			GroupType: types[g.Group],
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

		// kadm sorts DescribedGroup.Members by InstanceID (nil last) then
		// MemberID (kadm@v1.18.0 groups.go:401), so the retained prefix is
		// stable across cycles without sorting here.
		keep, droppedMembers := capLen(len(g.Members), c.limits.MaxMembersPerGroup)
		if droppedMembers > 0 {
			sec.dropped.Members += droppedMembers
			sec.truncated = true
		}

		members := make([]metrics.GroupMember, 0, keep)
		for _, m := range g.Members[:keep] {
			member := metrics.GroupMember{
				MemberID:   m.MemberID,
				InstanceID: m.InstanceID,
				ClientID:   m.ClientID,
				Host:       m.ClientHost,
				Assignment: make([]metrics.TopicPartition, 0),
			}

			// Join metadata: what the member asked for, the generation, and what
			// it still claims to own under cooperative rebalancing.
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

		// The PRE-truncation count, so len(members) < member_count says the
		// member list was cut. Generation is derived from the emitted members, so
		// a truncated group under-reports it — one more reason
		// MaxMembersPerGroup defaults to unlimited.
		gm.MemberCount = len(g.Members)
		gm.Members = members
		groups = append(groups, gm)
	}

	c.enrichConsumerGroups(ctx, sec, groups)

	return groups, sec
}

// enrichConsumerGroups overlays ConsumerGroupDescribe (KIP-848) data onto groups
// that DescribeGroups has already shaped.
//
// It is an overlay, not a replacement: DescribeGroups answers for every group on
// every broker version, while this call answers only for new-protocol groups on
// Kafka 4.0+, so the classic path stays authoritative.
//
// Groups are named explicitly rather than letting kadm discover them: passing no
// groups makes it call ListGroupsByType, whose TypesFilter franz-go silently
// drops when downgrading, which would quietly describe the wrong set.
func (c *Collector) enrichConsumerGroups(ctx context.Context, sec *section, groups []metrics.GroupMetrics) {
	if !c.opts.CollectConsumerGroups || len(groups) == 0 {
		return
	}

	ids := make([]string, 0, len(groups))
	for _, g := range groups {
		ids = append(ids, g.ID)
	}

	described, err := c.client.Admin.DescribeConsumerGroups(ctx, ids...)
	// kadm aborts a whole shard on the first GROUP_AUTHORIZATION_FAILED, so one
	// denied group would otherwise discard every other group's epochs.
	if !sec.requestPartial("ConsumerGroupDescribe", err, len(described) > 0) {
		return
	}

	for i := range groups {
		d, ok := described[groups[i].ID]
		if !ok {
			continue
		}
		// A per-group error here is the ORDINARY case, not a failure: the
		// coordinator answers GROUP_ID_NOT_FOUND for every classic-protocol
		// group. Recording it would emit one error per classic group per cycle,
		// forever, describing nothing wrong.
		if d.Err != nil {
			continue
		}
		applyConsumerGroup(&groups[i], d)
	}
}

// applyConsumerGroup overlays one ConsumerGroupDescribe result onto the group
// the classic describe already shaped. It is separate from the request so the
// merge rules are testable without a Kafka client.
func applyConsumerGroup(gm *metrics.GroupMetrics, d kadm.DescribedConsumerGroup) {
	epoch, assignmentEpoch := d.Epoch, d.AssignmentEpoch
	gm.GroupEpoch = &epoch
	gm.AssignmentEpoch = &assignmentEpoch
	gm.Assignor = d.AssignorName

	byID := make(map[string]kadm.ConsumerGroupMember, len(d.Members))
	for _, m := range d.Members {
		byID[m.MemberID] = m
	}
	// Iterate the members already emitted, so MaxMembersPerGroup still bounds the
	// list and the two describes cannot disagree about which members exist.
	for j := range gm.Members {
		m, ok := byID[gm.Members[j].MemberID]
		if !ok {
			continue
		}
		memberEpoch := m.MemberEpoch
		gm.Members[j].MemberEpoch = &memberEpoch
		gm.Members[j].SubscribedTopicRegex = m.SubscribedTopicRegex
		gm.Members[j].TargetAssignment = topicsSetToPartitions(m.TargetAssignment)
		if len(gm.Members[j].SubscribedTopics) == 0 {
			gm.Members[j].SubscribedTopics = m.SubscribedTopics
		}
		// The classic describe leaves Assignment empty for a new-protocol
		// member: there is no consumer-protocol join metadata to decode.
		if len(gm.Members[j].Assignment) == 0 {
			gm.Members[j].Assignment = topicsSetToPartitions(m.Assignment)
		}
	}
}

// topicsSetToPartitions flattens kadm's topic->partitions map into the wire's
// sorted pair list. TopicsSet is a map, so Sorted() is what keeps the output
// stable across cycles instead of following Go's map order.
func topicsSetToPartitions(ts kadm.TopicsSet) []metrics.TopicPartition {
	if len(ts) == 0 {
		return nil
	}
	out := make([]metrics.TopicPartition, 0, len(ts))
	for _, tp := range ts.Sorted() {
		for _, p := range tp.Partitions {
			out = append(out, metrics.TopicPartition{Topic: tp.Topic, Partition: p})
		}
	}
	return out
}
