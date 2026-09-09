package kafka

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// unknownVersion is the placeholder for a broker that answered without a
// version range to guess a release from. It is not a release name, so it never
// reaches the wire.
const unknownVersion = "unknown"

// API keys, and for each capability the first API version that carries the
// field it needs. Every number below was read out of kmsg@v1.13.1
// generated.go; API keys are wire constants and never change, so they are one
// table here rather than a request constructor per line.
const (
	// listGroupsKey is the ListGroups API key.
	listGroupsKey = 16
	// listGroupsStatesFilterVersion is the first ListGroups version carrying
	// KIP-518's StatesFilter field. kmsg only serialises the field "if version
	// >= 4" (ListGroupsRequest.AppendTo) and kgo silently downgrades a request
	// to whatever the broker offers, so against an older broker the filter is
	// dropped on the floor with no error.
	listGroupsStatesFilterVersion = 4
	// listGroupsStatesFilterKafka is the broker release that first served
	// ListGroups v4, named in the failure message because operators run Kafka
	// versions, not API versions.
	listGroupsStatesFilterKafka = "2.6"

	// consumerGroupDescribeKey is the ConsumerGroupDescribe API key (KIP-848).
	// Brokers below Kafka 4.0 do not advertise it at all, which is how its
	// absence is detected; v0 already carries everything the overlay reads.
	consumerGroupDescribeKey = 69

	// listOffsetsKey is the ListOffsets API key.
	listOffsetsKey = 2
	// listOffsetsIsolationVersion is the first ListOffsets version with an
	// IsolationLevel field (KIP-98, Kafka 0.11). Below it the field is dropped
	// on downgrade and the broker answers with the high watermark, so a last
	// stable offset would silently equal the end offset.
	listOffsetsIsolationVersion = 2
	// listOffsetsTimestampVersion is the first ListOffsets version whose
	// response carries one Offset plus its Timestamp per partition; v0 answers
	// with OldStyleOffsets and no timestamp at all, which is not a
	// server-measured window. Note this is NOT v7 — v7 adds the -3
	// max-timestamp sentinel (KIP-734), which ListOffsetsAfterMilli never
	// sends.
	listOffsetsTimestampVersion = 1

	// metadataKey is the Metadata API key.
	metadataKey = 3

	// describeLogDirsKey is the DescribeLogDirs API key.
	describeLogDirsKey = 35
	// describeLogDirsTotalBytesVersion is the first DescribeLogDirs version
	// whose response dirs carry TotalBytes/UsableBytes (KIP-827).
	describeLogDirsTotalBytesVersion = 4

	// offsetForLeaderEpochKey is the OffsetForLeaderEpoch API key. v0 is
	// enough: kadm sends only LeaderEpoch, never the v2+ CurrentLeaderEpoch.
	offsetForLeaderEpochKey = 23

	// shareGroupDescribeKey is the ShareGroupDescribe API key (KIP-932,
	// Kafka 4.0). Absent on every broker in existence today outside a 4.x
	// cluster, which is why the phase it gates defaults off.
	shareGroupDescribeKey = 77

	// describeConfigsKey is the DescribeConfigs API key. v0 exists from Kafka
	// 0.11, but v1 (Kafka 1.1) is the floor that matters: it added ConfigSource,
	// and without it every config arrives indistinguishable from a default,
	// which is the whole of drift detection and the change timeline.
	describeConfigsKey = 32

	// listPartitionReassignmentsKey is the ListPartitionReassignments API key
	// (Kafka 2.4). Its max version is still 0, so presence is the capability.
	listPartitionReassignmentsKey = 46
)

// Capability names are wire contract: a backend keys its feature matrix on
// them, so they are append-only and never renamed.
const (
	// CapabilityGroupStatesFilter is ListGroups with a states filter (KIP-518).
	CapabilityGroupStatesFilter = "group_states_filter"
	// CapabilityConsumerGroupDescribe is ConsumerGroupDescribe (KIP-848).
	CapabilityConsumerGroupDescribe = "consumer_group_describe"
	// CapabilityListOffsetsIsolation is ListOffsets honouring an isolation
	// level, which is what makes a last stable offset a last stable offset.
	CapabilityListOffsetsIsolation = "list_offsets_isolation"
	// CapabilityListOffsetsAfterMilli is ListOffsets answering by timestamp
	// with the offset's own timestamp attached.
	CapabilityListOffsetsAfterMilli = "list_offsets_after_milli"
	// CapabilityLogDirTotalBytes is DescribeLogDirs reporting dir capacity
	// (KIP-827), the denominator under log-dir growth.
	CapabilityLogDirTotalBytes = "log_dir_total_bytes"
	// CapabilityOffsetForLeaderEpoch is OffsetForLeaderEpoch, the positive
	// proof of truncation.
	CapabilityOffsetForLeaderEpoch = "offset_for_leader_epoch"
	// CapabilityListPartitionReassignments is ListPartitionReassignments,
	// which separates a planned rebalance from a failing one.
	CapabilityListPartitionReassignments = "list_partition_reassignments"
)

// probedKeys is every API key the fingerprint answers a question about, in
// ascending key order.
var probedKeys = []int16{
	listOffsetsKey,
	metadataKey,
	listGroupsKey,
	offsetForLeaderEpochKey,
	describeLogDirsKey,
	listPartitionReassignmentsKey,
	describeConfigsKey,
	shareGroupDescribeKey,
	consumerGroupDescribeKey,
}

// BrokerSoftware is one broker's guessed Kafka release. Two distinct versions
// across the cluster means a rolling upgrade is in flight, which explains
// otherwise inexplicable per-broker behaviour; it is a free by-product of the
// capability probe.
type BrokerSoftware struct {
	NodeID  int32  `json:"node_id"`
	Version string `json:"version"`
}

// Capabilities is what this cluster can be asked, decided once at startup from
// a single ApiVersions round trip per broker.
//
// Every answer is the MINIMUM across brokers, never the maximum: kadm and kgo
// shard these requests and merge the responses, so one old broker makes the
// merged result wrong while the cluster as a whole still looks modern.
type Capabilities struct {
	// floors is the per-key minimum, keyed by API key.
	floors map[int16]keyFloor
	// brokers is sorted by node ID.
	brokers []BrokerSoftware
	// probed is the raw per-broker answer behind floors, kept so the wire can
	// carry the material a capability was derived from and not only the verdict.
	probed []brokerKeys
}

// keyFloor is the lowest max-version any broker offers for one API key, and
// the broker that offered it. present is false when at least one broker does
// not advertise the key at all, in which case nodeID names that broker.
type keyFloor struct {
	nodeID  int32
	version int16
	present bool
}

// brokerKeys is one broker's probe answer, lifted out of kadm because
// BrokerApiVersions keeps its version map unexported and so cannot be built in
// a test. Lifting it keeps the decision that matters — minimum, not maximum —
// testable without a cluster.
type brokerKeys struct {
	nodeID  int32
	version string
	keys    map[int16]int16
}

// probeBrokers reads the probed keys out of every broker's answer. A broker
// that did not answer is a failure rather than a skip: nothing about its
// version can be shown, and assuming it is fine reintroduces exactly the silent
// degradations the fingerprint exists to catch.
func probeBrokers(versions kadm.BrokersApiVersions) ([]brokerKeys, error) {
	if len(versions) == 0 {
		return nil, errors.New("no broker answered the api versions probe")
	}
	// Sorted() so the broker named in any message is the same on every run,
	// rather than whichever the map yielded first.
	brokers := make([]brokerKeys, 0, len(versions))
	for _, v := range versions.Sorted() {
		if v.Err != nil {
			return nil, fmt.Errorf("broker %d did not answer the api versions probe: %w", v.NodeID, v.Err)
		}
		b := brokerKeys{nodeID: v.NodeID, version: unknownVersion, keys: make(map[int16]int16, len(probedKeys))}
		if v.Raw() != nil {
			b.version = v.VersionGuess()
		}
		for _, key := range probedKeys {
			if max, ok := v.KeyMaxVersion(key); ok {
				b.keys[key] = max
			}
		}
		brokers = append(brokers, b)
	}
	return brokers, nil
}

// foldCapabilities collapses per-broker answers into per-key floors.
func foldCapabilities(brokers []brokerKeys) *Capabilities {
	c := &Capabilities{
		floors:  make(map[int16]keyFloor, len(probedKeys)),
		brokers: make([]BrokerSoftware, 0, len(brokers)),
		probed:  brokers,
	}
	for _, key := range probedKeys {
		// No broker answered means nothing is supported; a zero-broker fold
		// must not report every capability as present by vacuous truth.
		floor := keyFloor{version: math.MaxInt16, present: len(brokers) > 0}
		// Strictly less-than over a node-sorted slice, so a tie deterministically
		// names the lowest broker ID.
		for _, b := range brokers {
			max, ok := b.keys[key]
			if !ok {
				floor = keyFloor{nodeID: b.nodeID, present: false}
				break
			}
			if max < floor.version {
				floor = keyFloor{nodeID: b.nodeID, version: max, present: true}
			}
		}
		c.floors[key] = floor
	}
	for _, b := range brokers {
		c.brokers = append(c.brokers, BrokerSoftware{NodeID: b.nodeID, Version: b.version})
	}
	return c
}

// atLeast reports whether every broker offers key at version or above.
func (c *Capabilities) atLeast(key, version int16) bool {
	f := c.floors[key]
	return f.present && f.version >= version
}

// SupportsGroupStatesFilter reports whether ListGroups can be filtered by group
// state on every broker (KIP-518, Kafka 2.6+).
func (c *Capabilities) SupportsGroupStatesFilter() bool {
	return c.atLeast(listGroupsKey, listGroupsStatesFilterVersion)
}

// SupportsConsumerGroupDescribe reports whether every broker answers
// ConsumerGroupDescribe (KIP-848, Kafka 4.0+).
func (c *Capabilities) SupportsConsumerGroupDescribe() bool {
	return c.atLeast(consumerGroupDescribeKey, 0)
}

// SupportsListOffsetsIsolation reports whether every broker honours the
// ListOffsets isolation level (Kafka 0.11+). Below it the last stable offset
// silently equals the high watermark, so this gates COLLECT_LAST_STABLE_OFFSET.
func (c *Capabilities) SupportsListOffsetsIsolation() bool {
	return c.atLeast(listOffsetsKey, listOffsetsIsolationVersion)
}

// SupportsListOffsetsAfterMilli reports whether every broker can answer
// ListOffsets by timestamp and return the matched offset's own timestamp
// (Kafka 0.10.1+).
func (c *Capabilities) SupportsListOffsetsAfterMilli() bool {
	return c.atLeast(listOffsetsKey, listOffsetsTimestampVersion)
}

// SupportsLogDirTotalBytes reports whether every broker reports log-dir
// capacity and free space in DescribeLogDirs (KIP-827, Kafka 3.3+).
func (c *Capabilities) SupportsLogDirTotalBytes() bool {
	return c.atLeast(describeLogDirsKey, describeLogDirsTotalBytesVersion)
}

// SupportsOffsetForLeaderEpoch reports whether every broker serves
// OffsetForLeaderEpoch (Kafka 0.11+).
func (c *Capabilities) SupportsOffsetForLeaderEpoch() bool {
	return c.atLeast(offsetForLeaderEpochKey, 0)
}

// SupportsListPartitionReassignments reports whether the cluster serves
// ListPartitionReassignments (Kafka 2.4+).
func (c *Capabilities) SupportsListPartitionReassignments() bool {
	return c.atLeast(listPartitionReassignmentsKey, 0)
}

// SupportsMaxTimestampOffsets gates ListOffsets timestamp -3 (KIP-734, Kafka
// 3.0, request v7). Below it the broker has no notion of the sentinel and
// answers as though a real millisecond had been asked for, which would return an
// arbitrary offset with no error -- the failure mode this probe exists to avoid.
func (c *Capabilities) SupportsMaxTimestampOffsets() bool {
	return c.atLeast(listOffsetsKey, 7)
}

// SupportsLocalLogStartOffsets gates ListOffsets timestamp -4 (KIP-405, Kafka
// 3.4, request v8).
func (c *Capabilities) SupportsLocalLogStartOffsets() bool {
	return c.atLeast(listOffsetsKey, 8)
}

// SupportsLatestTieredOffsets gates ListOffsets timestamp -5 (KIP-1005, Kafka
// 3.9, request v9).
func (c *Capabilities) SupportsLatestTieredOffsets() bool {
	return c.atLeast(listOffsetsKey, 9)
}

// SupportsListGroupsTypes gates the GroupType field on the ListGroups response
// (KIP-848, request v5). Below it the field is absent, and an absent type must
// ship empty rather than defaulting to "classic": "we cannot tell" and "it is
// classic" are different answers, and only the first is honest on a 2.x broker.
func (c *Capabilities) SupportsListGroupsTypes() bool {
	return c.atLeast(listGroupsKey, 5)
}

// SupportsShareGroupDescribe gates the share_groups section (KIP-932, Kafka
// 4.0).
func (c *Capabilities) SupportsShareGroupDescribe() bool {
	return c.atLeast(shareGroupDescribeKey, 0)
}

// SupportsDescribeConfigs gates the topic_configs and broker_configs sections
// on v1, not v0. v0 answers without ConfigSource, so every key would ship with
// an empty source and a backend could not tell "an operator set this" from
// "this is the shipped default" -- which is what config drift IS.
func (c *Capabilities) SupportsDescribeConfigs() bool {
	return c.atLeast(describeConfigsKey, 1)
}

// All is the whole fingerprint keyed by capability name, for shipping on the
// wire. Every name is always present, so a false is "probed and unsupported"
// rather than "not asked"; encoding/json sorts map keys, so the output is
// deterministic.
func (c *Capabilities) All() map[string]bool {
	return map[string]bool{
		CapabilityGroupStatesFilter:          c.SupportsGroupStatesFilter(),
		CapabilityConsumerGroupDescribe:      c.SupportsConsumerGroupDescribe(),
		CapabilityListOffsetsIsolation:       c.SupportsListOffsetsIsolation(),
		CapabilityListOffsetsAfterMilli:      c.SupportsListOffsetsAfterMilli(),
		CapabilityLogDirTotalBytes:           c.SupportsLogDirTotalBytes(),
		CapabilityOffsetForLeaderEpoch:       c.SupportsOffsetForLeaderEpoch(),
		CapabilityListPartitionReassignments: c.SupportsListPartitionReassignments(),
	}
}

// wireAPINames maps the probed API keys onto the names
// metrics.BrokerCapability.APIMaxVersions is keyed by, so no backend carries a
// key table.
var wireAPINames = map[int16]string{
	listOffsetsKey:                metrics.APIKeyListOffsets,
	metadataKey:                   metrics.APIKeyMetadata,
	listGroupsKey:                 metrics.APIKeyListGroups,
	offsetForLeaderEpochKey:       metrics.APIKeyOffsetForLeaderEpoch,
	describeLogDirsKey:            metrics.APIKeyDescribeLogDirs,
	listPartitionReassignmentsKey: metrics.APIKeyListPartitionReassignments,
	consumerGroupDescribeKey:      metrics.APIKeyConsumerGroupDescribe,
}

// Wire renders the fingerprint for Batch.Cluster.Capabilities.
//
// Only the capabilities this probe actually asked about are in Features. A name
// the agent does not probe is ABSENT rather than false, which is the documented
// difference between "probed and unsupported" and "not asked" — reporting
// cluster_authorized_operations as false because nothing looked would be a claim
// about the cluster the agent never made.
func (c *Capabilities) Wire() *metrics.ClusterCapabilities {
	if c == nil {
		return nil
	}
	wire := &metrics.ClusterCapabilities{
		Features: map[string]bool{
			metrics.CapabilityLastStableOffset:      c.SupportsListOffsetsIsolation(),
			metrics.CapabilityListOffsetsAfterMilli: c.SupportsListOffsetsAfterMilli(),
			metrics.CapabilityGroupStateFilter:      c.SupportsGroupStatesFilter(),
			metrics.CapabilityConsumerGroupDescribe: c.SupportsConsumerGroupDescribe(),
			metrics.CapabilityLogDirsVolumeBytes:    c.SupportsLogDirTotalBytes(),
			metrics.CapabilityOffsetForLeaderEpoch:  c.SupportsOffsetForLeaderEpoch(),
			metrics.CapabilityReassignments:         c.SupportsListPartitionReassignments(),
		},
		Brokers: make([]metrics.BrokerCapability, 0, len(c.probed)),
	}

	seen := make(map[string]bool, len(c.probed))
	for _, b := range c.probed {
		row := metrics.BrokerCapability{BrokerID: b.nodeID}
		// probeBrokers stores "unknown" when the broker answered without a raw
		// version range to guess from; the wire says the same thing with an empty
		// string.
		if b.version != unknownVersion {
			row.SoftwareVersion = b.version
			if !seen[b.version] {
				seen[b.version] = true
				wire.SoftwareVersions = append(wire.SoftwareVersions, b.version)
			}
		}
		// Non-nil even when empty: null means "this broker did not answer", and
		// probeBrokers fails the whole probe rather than returning such a broker.
		row.APIMaxVersions = make(map[string]int16, len(b.keys))
		for key, max := range b.keys {
			if name, ok := wireAPINames[key]; ok {
				row.APIMaxVersions[name] = max
			}
		}
		wire.Brokers = append(wire.Brokers, row)
	}
	sort.Strings(wire.SoftwareVersions)
	wire.MixedVersions = len(wire.SoftwareVersions) > 1
	return wire
}

// Brokers returns each broker's guessed Kafka release, sorted by node ID.
func (c *Capabilities) Brokers() []BrokerSoftware {
	out := make([]BrokerSoftware, len(c.brokers))
	copy(out, c.brokers)
	return out
}

// MixedVersions reports whether the brokers guessed as more than one Kafka
// release — a rolling upgrade in flight, or one node nobody restarted.
func (c *Capabilities) MixedVersions() bool {
	for _, b := range c.brokers {
		if b.Version != c.brokers[0].Version {
			return true
		}
	}
	return false
}

// checkGroupStateFilter explains, rather than merely reports, an unusable
// states filter: the operator asked for a few hundred groups out of 40k and
// would otherwise get all 40k with no error and no log line.
func (c *Capabilities) checkGroupStateFilter() error {
	f := c.floors[listGroupsKey]
	switch {
	case !f.present && len(c.brokers) == 0:
		return errors.New("no broker answered the api versions probe")
	case !f.present:
		return fmt.Errorf("broker %d does not support ListGroups at all", f.nodeID)
	case f.version < listGroupsStatesFilterVersion:
		return fmt.Errorf("broker %d offers ListGroups v%d, but filtering by group state needs v%d (KIP-518, Kafka %s+); an older broker ignores the filter and returns every group",
			f.nodeID, f.version, listGroupsStatesFilterVersion, listGroupsStatesFilterKafka)
	}
	return nil
}
