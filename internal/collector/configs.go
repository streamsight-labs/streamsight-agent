package collector

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"kafka-metrics-agent/internal/metrics"
)

const (
	apiDescribeTopicConfigs  = "DescribeConfigs"
	apiDescribeBrokerConfigs = "DescribeConfigs"
)

// topicConfigKeys is the allowlist for DescribeTopicConfigs.
//
// An allowlist rather than a filter for two reasons. A broker answers with
// roughly forty topic keys and well over two hundred broker keys, so shipping
// everything would make this the largest section in the batch for data that
// changes on human timescales. And an allowlist can be read: every key below
// has a named consumer, and a key nobody consumes is a key nobody has to reason
// about when it turns out to be sensitive.
//
// Order is the wire order, chosen so the two corrective keys come first.
var topicConfigKeys = []string{
	// Corrections. Without these the product ships wrong numbers.
	"cleanup.policy",      // compacted => offset deltas are not record counts
	"min.insync.replicas", // under-min-ISR => producers are failing, not just degraded

	// Retention headroom and time-to-data-loss, with T1.1 log dir sizes.
	"retention.ms",
	"retention.bytes",

	// Risk posture. Turns "the high watermark went backwards" into "and the
	// config permitted it".
	"unclean.leader.election.enable",

	// Explains disk sawtooth, and bounds when retention can actually act: a
	// segment is only deleted once closed.
	"segment.bytes",
	"segment.ms",

	// Whether broker-side timestamps -- and therefore any time-based lag -- are
	// producer-supplied or broker-stamped.
	"message.timestamp.type",

	// Observed record size against the configured ceiling.
	"max.message.bytes",

	// Changes the bytes-on-disk to records relationship in T1.1.
	"compression.type",
}

// brokerConfigKeys is the allowlist for DescribeBrokerConfigs.
//
// Deliberately excluded: anything path-shaped (log.dirs), anything credential
// shaped, and every listener/SASL/SSL key. Those are the keys most likely to be
// SENSITIVE, and the agent has no consumer for any of them.
var brokerConfigKeys = []string{
	// The prediction key. How long the coordinator keeps a group's committed
	// offsets after it empties, i.e. when "consumer restarted and reset to
	// earliest" becomes inevitable.
	"offsets.retention.minutes",

	// Drift. A per-broker disagreement on either of these is silently fatal.
	"min.insync.replicas",
	"unclean.leader.election.enable",

	// Cluster defaults, and what a topic created today would inherit.
	"default.replication.factor",
	"num.partitions",
	"auto.create.topics.enable",

	// Broker-level retention: the fallback for a topic that sets none.
	"log.retention.ms",
	"log.retention.bytes",
	"log.segment.bytes",

	// Replication throughput, which bounds how fast a URP can recover.
	"num.replica.fetchers",

	// The ceiling a hung transaction has to cross before it is provably hung.
	"transaction.max.timeout.ms",
}

// collectTopicConfigs and collectBrokerConfigs are two phases, not one, because
// DESCRIBE_CONFIGS is granted separately on TOPIC and on CLUSTER. A principal
// holding one and not the other has to see one section ok and the other
// unauthorized; collapsing them would report both as failed and send the
// operator looking for an outage instead of a grant.
//
// Neither participates in the offset chain. Both are called after the
// end-offset sample for the same reason log_dirs is: their responses are
// O(topics) and O(brokers), and inflating the latency that topics_end exists to
// pin down would corrupt the lag uncertainty bound.
//
// run is false on the cycles between samples. The section is still emitted, so
// "not collected this cycle" and "collected, nothing to report" are never the
// same batch.
func (c *Collector) collectTopicConfigs(ctx context.Context, topics []metrics.TopicMetrics, run bool) ([]metrics.TopicConfig, *section) {
	sec := c.newSection(sectionTopicConfigs)
	defer sec.stop()

	if !run {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	names := make([]string, 0, len(topics))
	for _, t := range topics {
		names = append(names, t.Name)
	}
	if len(names) == 0 {
		// kadm returns (nil, nil) for an empty topic list rather than describing
		// everything, so this guard is not the load-bearing one that logdirs
		// needs. It is here so an empty cluster reports "skipped" rather than
		// "ok with no configs", which would read as "no topics are configured".
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	described, err := c.client.DescribeTopicConfigs(ctx, names...)
	// Shard errors keep the topics that did answer: one topic this principal may
	// not describe must not blank out the configs of every other.
	if !sec.request(apiDescribeTopicConfigs, err) && len(described) == 0 {
		return nil, sec
	}

	out := make([]metrics.TopicConfig, 0, len(described))
	for _, rc := range described {
		tc := metrics.TopicConfig{Topic: rc.Name}
		if rc.Err != nil {
			sec.recordTopic(apiDescribeTopicConfigs, rc.Name, rc.Err)
			tc.ErrorCode = errorCode(rc.Err)
		}
		tc.Configs = selectConfigs(rc.Configs, topicConfigKeys)
		out = append(out, tc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out, sec
}

func (c *Collector) collectBrokerConfigs(ctx context.Context, cluster *metrics.ClusterMetrics, run bool) ([]metrics.BrokerConfig, *section) {
	sec := c.newSection(sectionBrokerConfigs)
	defer sec.stop()

	// A nil cluster means the metadata request failed, so there is no broker
	// list to name -- and naming none is the one thing this phase must never do
	// (see the empty-ids guard below).
	if !run || cluster == nil {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	ids := make([]int32, 0, len(cluster.Brokers))
	for _, b := range cluster.Brokers {
		ids = append(ids, b.ID)
	}
	if len(ids) == 0 {
		// MANDATORY guard, and the inverse of the bug it looks like. kadm sends
		// ONE request for cluster-level dynamic config when the broker list is
		// empty (kadm@v1.18.0 configs.go:96-99). That answer is not per-broker,
		// so it cannot show drift -- and it would arrive labelled with whichever
		// broker happened to serve it, which is worse than no answer at all.
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	described, err := c.client.DescribeBrokerConfigs(ctx, ids...)
	if !sec.request(apiDescribeBrokerConfigs, err) && len(described) == 0 {
		return nil, sec
	}

	out := make([]metrics.BrokerConfig, 0, len(described))
	for _, rc := range described {
		bc := metrics.BrokerConfig{Broker: brokerIDOf(rc.Name)}
		if rc.Err != nil {
			ce := sec.newError(apiDescribeBrokerConfigs, rc.Err)
			id := bc.Broker
			ce.BrokerID = &id
			sec.record(ce, false)
			bc.ErrorCode = errorCode(rc.Err)
		}
		bc.Configs = selectConfigs(rc.Configs, brokerConfigKeys)
		out = append(out, bc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Broker < out[j].Broker })
	return out, sec
}

// selectConfigs projects a broker's answer onto the allowlist, in allowlist
// order, dropping keys nobody asked for.
//
// Synonyms are deliberately discarded. kadm requests them by default and they
// roughly triple the section for a fallback chain whose top entry is already the
// effective value; Source already answers "was this set or is it the default",
// which is the only question the synonyms were going to be used for.
func selectConfigs(cfgs []kadm.Config, allow []string) []metrics.ConfigEntry {
	if len(cfgs) == 0 {
		return nil
	}
	byKey := make(map[string]kadm.Config, len(cfgs))
	for _, cfg := range cfgs {
		byKey[cfg.Key] = cfg
	}

	out := make([]metrics.ConfigEntry, 0, len(allow))
	for _, key := range allow {
		cfg, ok := byKey[key]
		if !ok {
			// Absent, not null: a broker that does not know a key (an older
			// release, or a key that only exists at the other resource level)
			// must not be reported as having it unset.
			continue
		}
		out = append(out, metrics.ConfigEntry{
			Key:       cfg.Key,
			Value:     stripSensitive(cfg),
			Sensitive: cfg.Sensitive,
			Source:    configSourceName(cfg.Source),
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// stripSensitive is the second of two guards, and it is here because the first
// one is a promise rather than a mechanism.
//
// Kafka nils the value of a SENSITIVE config before it leaves the broker, so in
// practice this never fires. It fires if that ever stops being true, or if a key
// is added to an allowlist above without anyone checking. The agent shipping a
// credential to the backend is not a bug worth being one release away from.
func stripSensitive(cfg kadm.Config) *string {
	if cfg.Sensitive {
		return nil
	}
	return cfg.Value
}

// configSourceName renders kmsg.ConfigSource as its wire name.
//
// The number is not shipped: it is a protocol enum that a backend would have to
// carry a copy of, and the whole value of this field is that DYNAMIC and DEFAULT
// are legible without one.
func configSourceName(src kmsg.ConfigSource) string {
	switch src {
	case kmsg.ConfigSourceDynamicTopicConfig:
		return "DYNAMIC_TOPIC_CONFIG"
	case kmsg.ConfigSourceDynamicBrokerConfig:
		return "DYNAMIC_BROKER_CONFIG"
	case kmsg.ConfigSourceDynamicDefaultBrokerConfig:
		return "DYNAMIC_DEFAULT_BROKER_CONFIG"
	case kmsg.ConfigSourceStaticBrokerConfig:
		return "STATIC_BROKER_CONFIG"
	case kmsg.ConfigSourceDefaultConfig:
		return "DEFAULT_CONFIG"
	case kmsg.ConfigSourceDynamicBrokerLoggerConfig:
		return "DYNAMIC_BROKER_LOGGER_CONFIG"
	case kmsg.ConfigSourceClientMetricsConfig:
		return "CLIENT_METRICS_CONFIG"
	case kmsg.ConfigSourceGroupConfig:
		return "GROUP_CONFIG"
	default:
		return ""
	}
}

// brokerIDOf parses the resource name kadm echoes back for a broker config,
// which is the broker ID rendered as a string. A name that does not parse is
// reported as -1 rather than 0, because 0 is a legal broker ID and would attach
// another broker's config to it.
func brokerIDOf(name string) int32 {
	if name == "" {
		return -1
	}
	var id int32
	for _, r := range name {
		if r < '0' || r > '9' {
			return -1
		}
		id = id*10 + (r - '0')
	}
	return id
}
