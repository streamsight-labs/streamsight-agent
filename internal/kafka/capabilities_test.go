package kafka

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// modernBroker offers every probed key well above the version any capability
// needs, so a test only has to state the one key it is about.
func modernBroker(nodeID int32, keys ...map[int16]int16) brokerKeys {
	b := brokerKeys{nodeID: nodeID, version: "v4.0", keys: map[int16]int16{
		listOffsetsKey:                11,
		metadataKey:                   13,
		listGroupsKey:                 5,
		offsetForLeaderEpochKey:       4,
		describeLogDirsKey:            4,
		listPartitionReassignmentsKey: 0,
		consumerGroupDescribeKey:      1,
	}}
	for _, override := range keys {
		for k, v := range override {
			b.keys[k] = v
		}
	}
	return b
}

// withoutKey is a broker that does not advertise the key at all, which is how
// every pre-introduction Kafka looks.
func withoutKey(nodeID int32, key int16) brokerKeys {
	b := modernBroker(nodeID)
	delete(b.keys, key)
	return b
}

// Trusting the maximum would report the cluster as capable precisely when it is
// not: kadm and kgo shard these requests, so one old broker makes the merged
// answer wrong while the cluster still looks modern.
func TestCapabilitiesTakeTheMinimumNotTheMaximum(t *testing.T) {
	tests := []struct {
		name    string
		brokers []brokerKeys
		want    func(*Capabilities) bool
		wantOK  bool
	}{
		{
			name:    "isolation level: one pre-0.11 broker disables the last stable offset",
			brokers: []brokerKeys{modernBroker(1), modernBroker(2, map[int16]int16{listOffsetsKey: 1}), modernBroker(3)},
			want:    (*Capabilities).SupportsListOffsetsIsolation,
		},
		{
			name:    "isolation level: every broker at v2",
			brokers: []brokerKeys{modernBroker(1, map[int16]int16{listOffsetsKey: 2}), modernBroker(2)},
			want:    (*Capabilities).SupportsListOffsetsIsolation,
			wantOK:  true,
		},
		{
			name:    "timestamp lookup needs only v1, not v7",
			brokers: []brokerKeys{modernBroker(1, map[int16]int16{listOffsetsKey: 1})},
			want:    (*Capabilities).SupportsListOffsetsAfterMilli,
			wantOK:  true,
		},
		{
			name:    "timestamp lookup: v0 answers with no timestamp at all",
			brokers: []brokerKeys{modernBroker(1, map[int16]int16{listOffsetsKey: 0})},
			want:    (*Capabilities).SupportsListOffsetsAfterMilli,
		},
		{
			name:    "group states filter: one broker below v4",
			brokers: []brokerKeys{modernBroker(1), modernBroker(2, map[int16]int16{listGroupsKey: 3})},
			want:    (*Capabilities).SupportsGroupStatesFilter,
		},
		{
			name:    "consumer group describe: absent on one broker",
			brokers: []brokerKeys{modernBroker(1), withoutKey(2, consumerGroupDescribeKey)},
			want:    (*Capabilities).SupportsConsumerGroupDescribe,
		},
		{
			name:    "consumer group describe: present everywhere",
			brokers: []brokerKeys{modernBroker(1), modernBroker(2)},
			want:    (*Capabilities).SupportsConsumerGroupDescribe,
			wantOK:  true,
		},
		{
			name:    "log dir total bytes: one broker below KIP-827",
			brokers: []brokerKeys{modernBroker(1, map[int16]int16{describeLogDirsKey: 3}), modernBroker(2)},
			want:    (*Capabilities).SupportsLogDirTotalBytes,
		},
		{
			name:    "offset for leader epoch: absent on one broker",
			brokers: []brokerKeys{modernBroker(1), withoutKey(2, offsetForLeaderEpochKey)},
			want:    (*Capabilities).SupportsOffsetForLeaderEpoch,
		},
		{
			name:    "list partition reassignments: present at v0",
			brokers: []brokerKeys{modernBroker(1), modernBroker(2)},
			want:    (*Capabilities).SupportsListPartitionReassignments,
			wantOK:  true,
		},
		{
			name:    "list partition reassignments: absent on one broker",
			brokers: []brokerKeys{modernBroker(1), withoutKey(2, listPartitionReassignmentsKey)},
			want:    (*Capabilities).SupportsListPartitionReassignments,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.want(foldCapabilities(tt.brokers)); got != tt.wantOK {
				t.Fatalf("capability = %t, want %t", got, tt.wantOK)
			}
		})
	}
}

// A fold over no brokers must not report everything as supported by vacuous
// truth — that would re-enable exactly the phases the probe exists to gate.
func TestEmptyFoldSupportsNothing(t *testing.T) {
	caps := foldCapabilities(nil)
	for name, ok := range caps.All() {
		if ok {
			t.Errorf("%s reported as supported with no brokers probed", name)
		}
	}
	if caps.MixedVersions() {
		t.Error("MixedVersions is true with no brokers probed")
	}
}

// Every capability name is always on the wire, so a false means "probed and
// unsupported" rather than "not asked".
func TestAllCoversEveryCapability(t *testing.T) {
	all := foldCapabilities([]brokerKeys{modernBroker(1)}).All()
	names := []string{
		CapabilityGroupStatesFilter,
		CapabilityConsumerGroupDescribe,
		CapabilityListOffsetsIsolation,
		CapabilityListOffsetsAfterMilli,
		CapabilityLogDirTotalBytes,
		CapabilityOffsetForLeaderEpoch,
		CapabilityListPartitionReassignments,
	}
	if len(all) != len(names) {
		t.Fatalf("All() has %d entries, want %d", len(all), len(names))
	}
	for _, name := range names {
		if ok, present := all[name]; !present || !ok {
			t.Errorf("%s = %t (present %t), want a supported entry on a modern cluster", name, ok, present)
		}
	}
}

func TestBrokerSoftwareIsSortedAndDetectsMixedVersions(t *testing.T) {
	rolling := []brokerKeys{
		{nodeID: 1, version: "v3.9", keys: modernBroker(1).keys},
		{nodeID: 2, version: "v4.0", keys: modernBroker(2).keys},
	}
	caps := foldCapabilities(rolling)
	got := caps.Brokers()
	if len(got) != 2 || got[0].NodeID != 1 || got[1].Version != "v4.0" {
		t.Fatalf("Brokers() = %+v", got)
	}
	if !caps.MixedVersions() {
		t.Error("a cluster mid-rolling-upgrade did not report mixed versions")
	}

	// Mutating the returned slice must not reach into the cached fingerprint.
	got[0].Version = "tampered"
	if caps.Brokers()[0].Version == "tampered" {
		t.Error("Brokers() handed out the internal slice")
	}

	uniform := foldCapabilities([]brokerKeys{modernBroker(1), modernBroker(2)})
	if uniform.MixedVersions() {
		t.Error("a uniform cluster reported mixed versions")
	}
}

func TestCheckGroupStateFilterExplainsTheFailure(t *testing.T) {
	tests := []struct {
		name    string
		brokers []brokerKeys
		wantErr string
	}{
		{
			name:    "every broker current",
			brokers: []brokerKeys{modernBroker(1), modernBroker(2, map[int16]int16{listGroupsKey: 4})},
		},
		{
			name:    "one old broker in a modern cluster",
			brokers: []brokerKeys{modernBroker(1), modernBroker(2, map[int16]int16{listGroupsKey: 3}), modernBroker(3)},
			wantErr: "broker 2 offers ListGroups v3",
		},
		{
			// A tie must name the same broker on every run, or the failure
			// message churns across restarts of an unchanged cluster.
			name: "ties name the lowest broker id",
			brokers: []brokerKeys{
				modernBroker(4, map[int16]int16{listGroupsKey: 2}),
				modernBroker(9, map[int16]int16{listGroupsKey: 2}),
			},
			wantErr: "broker 4 offers",
		},
		{
			name:    "a broker that does not know ListGroups at all",
			brokers: []brokerKeys{modernBroker(1), withoutKey(7, listGroupsKey)},
			wantErr: "broker 7 does not support ListGroups",
		},
		{
			name:    "an empty probe is a failure, not a pass",
			brokers: nil,
			wantErr: "no broker answered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := foldCapabilities(tt.brokers).checkGroupStateFilter()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("checkGroupStateFilter = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkGroupStateFilter = nil, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %v does not contain %q", err, tt.wantErr)
			}
			// Operators run Kafka releases, not wire versions.
			if strings.Contains(tt.wantErr, "offers") && !strings.Contains(err.Error(), listGroupsStatesFilterKafka) {
				t.Errorf("error %v does not name the required Kafka version", err)
			}
		})
	}
}

// A broker that cannot be interrogated is treated as unknown, not as fine:
// assuming it is fine would reintroduce the silent degradations the probe
// exists to prevent, on the one broker already behaving oddly.
func TestProbeBrokersFailsClosed(t *testing.T) {
	t.Run("no brokers", func(t *testing.T) {
		if _, err := probeBrokers(kadm.BrokersApiVersions{}); err == nil {
			t.Fatal("an empty probe result was accepted")
		}
	})

	t.Run("broker whose probe errored", func(t *testing.T) {
		vs := kadm.BrokersApiVersions{
			3: kadm.BrokerApiVersions{NodeID: 3, Err: errors.New("dial tcp: connection refused")},
		}
		_, err := probeBrokers(vs)
		if err == nil {
			t.Fatal("a broker that did not answer was accepted")
		}
		if !strings.Contains(err.Error(), "broker 3") {
			t.Errorf("error %v does not name the broker", err)
		}
	})

	t.Run("broker advertising nothing", func(t *testing.T) {
		// A zero-value BrokerApiVersions reports every key as absent, and a nil
		// raw response must not panic the version guess.
		vs := kadm.BrokersApiVersions{1: kadm.BrokerApiVersions{NodeID: 1}}
		brokers, err := probeBrokers(vs)
		if err != nil {
			t.Fatalf("probeBrokers = %v, want the broker recorded as capable of nothing", err)
		}
		caps := foldCapabilities(brokers)
		for name, ok := range caps.All() {
			if ok {
				t.Errorf("%s reported as supported by a broker advertising no keys", name)
			}
		}
		if got := caps.Brokers(); len(got) != 1 || got[0].Version != "unknown" {
			t.Errorf("Brokers() = %+v, want one broker of unknown version", got)
		}
	})
}

// The wire fingerprint must answer "absent because the broker is too old"
// without a backend re-deriving anything, and must not answer questions the
// probe never asked.
func TestWireCarriesTheEvidenceNotJustTheVerdict(t *testing.T) {
	old := modernBroker(2, map[int16]int16{listOffsetsKey: 1})
	old.version = "v0.10.2"
	// Node order as probeBrokers produces it, which is what makes the rows
	// join against cluster.brokers[] without re-sorting.
	wire := foldCapabilities([]brokerKeys{modernBroker(1), old}).Wire()

	// One old broker decides the cluster's answer: the request can land on any
	// of them.
	if wire.Features[metrics.CapabilityLastStableOffset] {
		t.Error("last_stable_offset reported despite a broker at ListOffsets v1")
	}
	if !wire.Features[metrics.CapabilityListOffsetsAfterMilli] {
		t.Error("list_offsets_after_milli must still be supported at v1")
	}
	// A capability nothing probed is ABSENT, not false: false is a claim about
	// the cluster, and no request was made to back it.

	if len(wire.Brokers) != 2 {
		t.Fatalf("brokers = %+v, want one row per broker", wire.Brokers)
	}
	for _, b := range wire.Brokers {
		if b.APIMaxVersions == nil {
			t.Fatalf("broker %d has a null api_max_versions, which means it did not answer", b.BrokerID)
		}
		if _, ok := b.APIMaxVersions[metrics.APIKeyListOffsets]; !ok {
			t.Errorf("broker %d does not report its ListOffsets version", b.BrokerID)
		}
	}
	if wire.Brokers[1].BrokerID != 2 {
		t.Fatalf("brokers = %+v, want them in node order", wire.Brokers)
	}
	if got := wire.Brokers[1].APIMaxVersions[metrics.APIKeyListOffsets]; got != 1 {
		t.Errorf("broker 2 reports ListOffsets v%d, want the raw v1 the floor was derived from", got)
	}

	// Two guesses over the brokers that answered is a rolling upgrade in flight,
	// which explains otherwise inexplicable per-broker behaviour.
	if !wire.MixedVersions || len(wire.SoftwareVersions) != 2 {
		t.Errorf("versions = %v mixed=%t, want both releases reported", wire.SoftwareVersions, wire.MixedVersions)
	}
	if !sort.StringsAreSorted(wire.SoftwareVersions) {
		t.Errorf("versions = %v, want them sorted", wire.SoftwareVersions)
	}
}

// A broker that answered without a version range has no release to name, and
// "unknown" is a placeholder rather than a release.
func TestWireOmitsTheUnknownVersionPlaceholder(t *testing.T) {
	brokers, err := probeBrokers(kadm.BrokersApiVersions{1: kadm.BrokerApiVersions{NodeID: 1}})
	if err != nil {
		t.Fatalf("probeBrokers: %v", err)
	}
	wire := foldCapabilities(brokers).Wire()
	if len(wire.SoftwareVersions) != 0 {
		t.Errorf("software_versions = %v, want none", wire.SoftwareVersions)
	}
	if wire.Brokers[0].SoftwareVersion != "" {
		t.Errorf("software_version = %q, want empty", wire.Brokers[0].SoftwareVersion)
	}
	// Still not null: the broker answered, it just advertises no key the agent
	// uses.
	if wire.Brokers[0].APIMaxVersions == nil {
		t.Error("api_max_versions is null for a broker that did answer")
	}
}
