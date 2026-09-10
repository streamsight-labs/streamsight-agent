package collector

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

func strp(s string) *string { return &s }

// The allowlist is the section's cost control and its security boundary, so the
// projection is tested before anything that calls it.
func TestSelectConfigsKeepsOnlyTheAllowlistInAllowlistOrder(t *testing.T) {
	// Deliberately shuffled relative to the allowlist, and padded with the kind
	// of keys a real broker sends by the dozen.
	got := selectConfigs([]kadm.Config{
		{Key: "retention.ms", Value: strp("604800000"), Source: kmsg.ConfigSourceDynamicTopicConfig},
		{Key: "flush.messages", Value: strp("9223372036854775807")},
		{Key: "cleanup.policy", Value: strp("compact"), Source: kmsg.ConfigSourceDynamicTopicConfig},
		{Key: "leader.replication.throttled.replicas", Value: strp("")},
		{Key: "min.insync.replicas", Value: strp("2"), Source: kmsg.ConfigSourceStaticBrokerConfig},
	}, topicConfigKeys)

	want := []string{"cleanup.policy", "min.insync.replicas", "retention.ms"}
	if len(got) != len(want) {
		t.Fatalf("got %d entries %v, want %d", len(got), got, len(want))
	}
	for i, key := range want {
		if got[i].Key != key {
			t.Errorf("entry %d = %q, want %q (allowlist order is the wire order)", i, got[i].Key, key)
		}
	}
	if got[0].Source != "DYNAMIC_TOPIC_CONFIG" {
		t.Errorf("source = %q, want the name, not the enum number", got[0].Source)
	}
}

// A key the broker never sent must be absent, not present-and-null: an older
// release that does not know a key has not "unset" it.
func TestSelectConfigsOmitsKeysTheBrokerDidNotSend(t *testing.T) {
	got := selectConfigs([]kadm.Config{
		{Key: "cleanup.policy", Value: strp("delete")},
	}, topicConfigKeys)

	if len(got) != 1 || got[0].Key != "cleanup.policy" {
		t.Fatalf("got %+v, want only cleanup.policy", got)
	}
}

// The agent must never ship a credential, even if a broker sends one. Kafka
// nils sensitive values itself, so this guards the day that stops being true.
func TestSelectConfigsNeverShipsASensitiveValue(t *testing.T) {
	got := selectConfigs([]kadm.Config{
		{Key: "min.insync.replicas", Value: strp("hunter2"), Sensitive: true},
	}, topicConfigKeys)

	if len(got) != 1 {
		t.Fatalf("got %+v, want one entry", got)
	}
	if got[0].Value != nil {
		t.Errorf("value = %q, want nil: a sensitive config must never leave the agent", *got[0].Value)
	}
	if !got[0].Sensitive {
		t.Error("sensitive flag dropped: a backend must be able to render 'redacted' rather than 'unset'")
	}
}

// nil and "" are different answers and both are legal, so the pointer has to
// survive a value that is genuinely the empty string.
func TestSelectConfigsDistinguishesEmptyFromUnset(t *testing.T) {
	got := selectConfigs([]kadm.Config{
		{Key: "cleanup.policy", Value: strp("")},
		{Key: "min.insync.replicas", Value: nil},
	}, topicConfigKeys)

	if len(got) != 2 {
		t.Fatalf("got %+v, want two entries", got)
	}
	if got[0].Value == nil || *got[0].Value != "" {
		t.Errorf("cleanup.policy = %v, want a non-nil empty string", got[0].Value)
	}
	if got[1].Value != nil {
		t.Errorf("min.insync.replicas = %q, want nil", *got[1].Value)
	}
}

// 0 is a legal broker ID, so an unparseable resource name must not decode to it
// and attach one broker's config to another.
func TestBrokerIDOfRefusesToGuess(t *testing.T) {
	for name, want := range map[string]int32{
		"0": 0, "3": 3, "42": 42,
		"":         -1,
		"broker-1": -1,
		"1a":       -1,
	} {
		if got := brokerIDOf(name); got != want {
			t.Errorf("brokerIDOf(%q) = %d, want %d", name, got, want)
		}
	}
}

// Every ConfigSource the pinned kmsg defines must render, or a backend silently
// loses the DYNAMIC-vs-DEFAULT distinction that drift detection is built on.
func TestConfigSourceNameCoversEveryKnownSource(t *testing.T) {
	for _, src := range []kmsg.ConfigSource{
		kmsg.ConfigSourceDynamicTopicConfig,
		kmsg.ConfigSourceDynamicBrokerConfig,
		kmsg.ConfigSourceDynamicDefaultBrokerConfig,
		kmsg.ConfigSourceStaticBrokerConfig,
		kmsg.ConfigSourceDefaultConfig,
		kmsg.ConfigSourceDynamicBrokerLoggerConfig,
		kmsg.ConfigSourceClientMetricsConfig,
		kmsg.ConfigSourceGroupConfig,
	} {
		if configSourceName(src) == "" {
			t.Errorf("ConfigSource %d renders empty", src)
		}
	}
	if got := configSourceName(kmsg.ConfigSourceUnknown); got != "" {
		t.Errorf("unknown source = %q, want empty", got)
	}
}

// Off-cadence is "skipped", never "ok with nothing": the two must never be the
// same batch, or a backend reads a quiet cycle as a cluster with no configs.
func TestConfigPhasesSkipRatherThanReportNothing(t *testing.T) {
	c := &Collector{}

	cfgs, sec := c.collectTopicConfigs(t.Context(), []metrics.TopicMetrics{{Name: "orders"}}, false)
	if cfgs != nil || sec.status != metrics.SectionSkipped {
		t.Errorf("off-cadence topic configs = %v / %s, want nil / skipped", cfgs, sec.status)
	}

	bcfgs, bsec := c.collectBrokerConfigs(t.Context(), &metrics.ClusterMetrics{
		Brokers: []metrics.Broker{{ID: 1}},
	}, false)
	if bcfgs != nil || bsec.status != metrics.SectionSkipped {
		t.Errorf("off-cadence broker configs = %v / %s, want nil / skipped", bcfgs, bsec.status)
	}
}

// The guard that matters: DescribeBrokerConfigs with NO broker IDs answers with
// cluster-level dynamic config only, which cannot show drift and would arrive
// labelled with whichever broker served it. Skipping is the correct answer.
func TestBrokerConfigsSkipRatherThanAskAboutNoBroker(t *testing.T) {
	c := &Collector{}

	cfgs, sec := c.collectBrokerConfigs(t.Context(), &metrics.ClusterMetrics{}, true)
	if cfgs != nil {
		t.Errorf("configs = %v, want nil", cfgs)
	}
	if sec.status != metrics.SectionSkipped {
		t.Errorf("status = %s, want skipped: an empty broker list must never become a cluster-wide request", sec.status)
	}
}

func TestTopicConfigsSkipOnAnEmptyTopicList(t *testing.T) {
	c := &Collector{}

	cfgs, sec := c.collectTopicConfigs(t.Context(), nil, true)
	if cfgs != nil || sec.status != metrics.SectionSkipped {
		t.Errorf("= %v / %s, want nil / skipped", cfgs, sec.status)
	}
}

// The allowlists are the payload budget. A key added without a consumer is a key
// nobody has to think about until it turns out to be sensitive.
func TestAllowlistsStayBounded(t *testing.T) {
	if n := len(topicConfigKeys); n > 12 {
		t.Errorf("topicConfigKeys = %d, want <= 12: this section is O(topics)", n)
	}
	if n := len(brokerConfigKeys); n > 15 {
		t.Errorf("brokerConfigKeys = %d, want <= 15", n)
	}
	for _, keys := range [][]string{topicConfigKeys, brokerConfigKeys} {
		seen := map[string]bool{}
		for _, k := range keys {
			if seen[k] {
				t.Errorf("duplicate key %q would ship twice", k)
			}
			seen[k] = true
		}
	}
	// The two corrective keys are why this phase exists at all.
	var hasCleanup, hasMinISR bool
	for _, k := range topicConfigKeys {
		hasCleanup = hasCleanup || k == "cleanup.policy"
		hasMinISR = hasMinISR || k == "min.insync.replicas"
	}
	if !hasCleanup || !hasMinISR {
		t.Error("cleanup.policy and min.insync.replicas are the reason for this section")
	}
}
