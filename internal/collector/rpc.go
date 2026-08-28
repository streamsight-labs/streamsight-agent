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
// There is no switch for it: the counters come off traffic the agent sends
// anyway, so it costs no request, no ACL and no broker work, and the only thing
// an off position ever bought was a blind spot in the agent's own health.
//
// A nil snapshot means the hooks were never installed, which is reported as
// skipped for the same reason every other phase does: "not measured" and
// "measured, no traffic" are different facts.
func (c *Collector) collectRPC() (*metrics.RPCStats, *section) {
	sec := c.newSection(sectionBrokerRPC)
	defer sec.stop()

	stats := c.client.RPCSnapshot()
	if stats == nil {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}
	return stats, sec
}
