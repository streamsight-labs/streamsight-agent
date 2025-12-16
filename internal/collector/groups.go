package collector

import (
	"context"
	"strings"

	"kafka-metrics-agent/internal/metrics"
)

func (c *Collector) collectGroups(ctx context.Context) ([]metrics.GroupMetrics, []string) {
	admin := c.client.Admin

	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, []string{err.Error()}
	}

	var groupIDs []string
	for id := range listed {
		if !strings.HasPrefix(id, "__") {
			groupIDs = append(groupIDs, id)
		}
	}

	if len(groupIDs) == 0 {
		return nil, nil
	}

	described, err := admin.DescribeGroups(ctx, groupIDs...)
	if err != nil {
		return nil, []string{err.Error()}
	}

	groups := make([]metrics.GroupMetrics, 0, len(described))
	for _, g := range described {
		if g.Err != nil {
			continue
		}

		members := make([]metrics.GroupMember, 0, len(g.Members))
		for _, m := range g.Members {
			var assignment []metrics.TopicPartition
			if ca, ok := m.Assigned.AsConsumer(); ok {
				for _, t := range ca.Topics {
					for _, p := range t.Partitions {
						assignment = append(assignment, metrics.TopicPartition{
							Topic:     t.Topic,
							Partition: p,
						})
					}
				}
			}
			members = append(members, metrics.GroupMember{
				MemberID:   m.MemberID,
				ClientID:   m.ClientID,
				Host:       m.ClientHost,
				Assignment: assignment,
			})
		}

		groups = append(groups, metrics.GroupMetrics{
			ID:          g.Group,
			State:       g.State,
			Coordinator: g.Coordinator.NodeID,
			MemberCount: len(members),
			Members:     members,
		})
	}

	return groups, nil
}
