package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"kafka-metrics-agent/internal/kafka"
	"kafka-metrics-agent/internal/metrics"
)

type Collector struct {
	client *kafka.Client
}

func New(client *kafka.Client) *Collector {
	return &Collector{client: client}
}

func (c *Collector) Collect(ctx context.Context) *metrics.Batch {
	start := time.Now()

	cluster, topics, errs := c.collectMetadata(ctx)
	groups, groupErrs := c.collectGroups(ctx)
	offsets, offsetErrs := c.collectOffsets(ctx)

	errs = append(errs, groupErrs...)
	errs = append(errs, offsetErrs...)

	return &metrics.Batch{
		CollectedAt:  start,
		CollectionMs: time.Since(start).Milliseconds(),
		Cluster:      cluster,
		Topics:       topics,
		Groups:       groups,
		Offsets:      offsets,
		Errors:       errs,
	}
}

func (c *Collector) Print(batch *metrics.Batch) {
	data, _ := json.MarshalIndent(batch, "", "  ")
	fmt.Println(string(data))
}
