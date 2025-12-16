package collector

import (
	"context"
	"strings"

	"kafka-metrics-agent/internal/metrics"
)

func (c *Collector) collectMetadata(ctx context.Context) (metrics.ClusterMetrics, []metrics.TopicMetrics, []string) {
	var errs []string
	admin := c.client.Admin

	meta, err := admin.Metadata(ctx)
	if err != nil {
		return metrics.ClusterMetrics{}, nil, []string{err.Error()}
	}

	// Brokers
	brokers := make([]metrics.Broker, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		brokers = append(brokers, metrics.Broker{
			ID:   b.NodeID,
			Host: b.Host,
			Port: b.Port,
		})
	}

	cluster := metrics.ClusterMetrics{
		ID:          meta.Cluster,
		Controller:  meta.Controller,
		BrokerCount: len(brokers),
		Brokers:     brokers,
	}

	// Filter topics
	var topicNames []string
	for name := range meta.Topics {
		if !strings.HasPrefix(name, "__") {
			topicNames = append(topicNames, name)
		}
	}

	if len(topicNames) == 0 {
		return cluster, nil, errs
	}

	// Get offsets
	startOffsets, err := admin.ListStartOffsets(ctx, topicNames...)
	if err != nil {
		errs = append(errs, err.Error())
	}
	endOffsets, err := admin.ListEndOffsets(ctx, topicNames...)
	if err != nil {
		errs = append(errs, err.Error())
	}

	// Build topics
	topics := make([]metrics.TopicMetrics, 0, len(topicNames))
	for name, topic := range meta.Topics {
		if strings.HasPrefix(name, "__") {
			continue
		}

		partitions := make([]metrics.Partition, 0, len(topic.Partitions))
		for _, p := range topic.Partitions {
			part := metrics.Partition{
				ID:       p.Partition,
				Leader:   p.Leader,
				Replicas: p.Replicas,
				ISR:      p.ISR,
			}
			if startOffsets != nil {
				if so, ok := startOffsets[name][p.Partition]; ok && so.Err == nil {
					part.StartOffset = so.Offset
				}
			}
			if endOffsets != nil {
				if eo, ok := endOffsets[name][p.Partition]; ok && eo.Err == nil {
					part.EndOffset = eo.Offset
				}
			}
			partitions = append(partitions, part)
		}

		rf := 0
		if len(partitions) > 0 {
			rf = len(partitions[0].Replicas)
		}

		topics = append(topics, metrics.TopicMetrics{
			Name:              name,
			PartitionCount:    len(partitions),
			ReplicationFactor: rf,
			Partitions:        partitions,
		})
	}

	return cluster, topics, errs
}
