package collector

import (
	"context"
	"strings"

	"kafka-metrics-agent/internal/metrics"
)

func (c *Collector) collectOffsets(ctx context.Context) ([]metrics.ConsumerOffset, []string) {
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

	allOffsets := admin.FetchManyOffsets(ctx, groupIDs...)

	result := make([]metrics.ConsumerOffset, 0, len(allOffsets))
	for groupID, resp := range allOffsets {
		if resp.Err != nil {
			continue
		}

		var partitions []metrics.PartitionOffset
		for topic, parts := range resp.Fetched {
			if strings.HasPrefix(topic, "__") {
				continue
			}
			for partition, offset := range parts {
				if offset.Err != nil {
					continue
				}
				committed := offset.Offset.At
				if committed < 0 {
					committed = 0
				}
				partitions = append(partitions, metrics.PartitionOffset{
					Topic:     topic,
					Partition: partition,
					Offset:    committed,
				})
			}
		}

		result = append(result, metrics.ConsumerOffset{
			GroupID: groupID,
			Offsets: partitions,
		})
	}

	return result, nil
}
