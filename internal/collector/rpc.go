package collector

import "kafka-metrics-agent/internal/metrics"

// collectRPC detaches the counters the kgo hooks accumulated since the previous
// cycle: per-broker latency, bytes, connect failures and quota throttling,
// measured on the requests the agent was already sending.
//
// It is not part of the phase graph. It issues no request, takes no context and
// cannot fail, so it runs after wg.Wait() — which is also what makes the window
// cover this cycle's own traffic rather than the previous one's.
//
// EXACTLY ONE CALLER PER CYCLE: the snapshot resets the window, so a second call
// would silently halve the first's numbers.
//
// A nil snapshot means the hooks were never installed, which is reported as
// skipped for the same reason every other phase does: "not measured" and
// "measured, no traffic" are different facts.
func (c *Collector) collectRPC(run bool) (*metrics.RPCStats, *section) {
	sec := c.newSection(sectionBrokerRPC)
	defer sec.stop()

	if !run {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}
	stats := c.client.RPCSnapshot()
	if stats == nil {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}
	return stats, sec
}
