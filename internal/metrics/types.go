package metrics

import "time"

// SchemaVersion is the version of the Batch wire format. Bump on any
// backwards-incompatible change to the JSON produced by this package.
//
// ADDING a field never forces a bump: everything added since v1 is OPTIONAL, so
// a v1 consumer that ignores unknown keys stays correct.
//
// REMOVING one depends on what the consumer was already obliged to handle.
// Dropping an `omitempty` or nullable field is safe, because its absence was
// always a state a correct consumer had to tolerate -- that is why the
// authorized_operations fields and the group_states section came out at v1.
// Dropping a field that was ALWAYS PRESENT is not safe by that argument, and
// two have gone: cluster.controller, and the guarantee that `cluster` itself is
// present. Both were left at v1 deliberately, because no released consumer
// existed to break; a bump would have forced every reader to accept a new
// version number for a change none of them could observe. That reasoning
// expires the moment a backend ships. After that, removing an always-present
// field IS a bump.
//
// The one change that forces a bump regardless is a new SectionStatus value:
// see the closed-enum note on that type.
const SchemaVersion = 1

// Batch is one collection cycle, and the unit of export: one POST for HTTP, one
// line for file.
//
// Nullable numeric fields (*int64, *int32) mean "not known this cycle" and MUST
// be distinguished from zero. A partition whose offset lookup failed reports
// null, never 0.
//
// The PRESENCE of Truncation means the batch is incomplete; its absence means
// complete. It is reported at three levels, which answer different questions:
// batch ("can I aggregate cluster-wide from this?"), section ("is topic
// inventory short while groups are fine?"), and entity, where the
// pre-truncation counts (TopicMetrics.PartitionCount, GroupMetrics.MemberCount,
// ConsumerOffset.OffsetCount) stay TRUE, so len(list) < count is
// self-describing.
type Batch struct {
	SchemaVersion   int    `json:"schema_version"`
	AgentVersion    string `json:"agent_version"`
	AgentInstanceID string `json:"agent_instance_id"`
	BatchSeq        uint64 `json:"batch_seq"`

	// CollectedAt is stamped when the cycle starts. Divide rate-derived deltas
	// by Section.SampledAt instead, which is per-section.
	CollectedAt  time.Time `json:"collected_at"`
	CollectionMs int64     `json:"collection_ms"`

	// Cluster is NULL when the metadata request failed, and that is the whole
	// point of it being a pointer. A zero ClusterMetrics serialises as
	// broker_count 0 with a null broker list, and broker_count 0 is a legal
	// value -- a claim that the cluster has no brokers, indistinguishable from
	// an observation. Absent says the only true thing: this cycle could not
	// describe the cluster. sections[cluster].status carries why, and
	// Capabilities rides here too, so a failed cycle asserts nothing about a
	// cluster it never reached. Carry the last non-null forward per cluster.id,
	// exactly as ClusterCapabilities already documents.
	Cluster *ClusterMetrics  `json:"cluster,omitempty"`
	Topics  []TopicMetrics   `json:"topics"`
	Groups  []GroupMetrics   `json:"groups"`
	Offsets []ConsumerOffset `json:"offsets"`

	// LogDirs is per-broker, per-directory replica storage. It is empty both
	// when the section was skipped and when the cluster has nothing to report,
	// so read the section status, never len().
	LogDirs []LogDir `json:"log_dirs,omitempty"`

	// ThroughputWindow is the server-measured window this cycle asked about; the
	// per-partition answers are Partition.Window. Absent when the window phase
	// did not run.
	ThroughputWindow *ThroughputWindow `json:"throughput_window,omitempty"`

	// Reassignments are the partitions the controller reports as moving. Absent
	// means the probe did not run, which is the normal case: it fires only when a
	// URP is observed. Absence is NOT proof that nothing is moving.
	Reassignments []Reassignment `json:"reassignments,omitempty"`

	// EpochProbes are OffsetForLeaderEpoch results. Normally absent entirely: the
	// probe fires only on a committed-vs-current leader-epoch mismatch.
	EpochProbes []EpochProbe `json:"epoch_probes,omitempty"`

	// TopicConfigs and BrokerConfigs are allowlisted DescribeConfigs answers.
	// They are the only section needing an ACL outside the DESCRIBE on CLUSTER,
	// TOPIC and GROUP the rest of the agent lives inside, so they are off by
	// default -- and they are TWO sections rather than one: DESCRIBE_CONFIGS is
	// granted separately on TOPIC and on CLUSTER, and a principal holding one
	// and not the other must see one section ok and the other unauthorized.
	//
	// Both are absent when the phase did not run. Read the section status.
	TopicConfigs  []TopicConfig  `json:"topic_configs,omitempty"`
	BrokerConfigs []BrokerConfig `json:"broker_configs,omitempty"`

	// ShareGroups are KIP-932 share groups (Kafka 4.0+). A separate section from
	// groups[] on purpose: they have no partition ownership and no committed
	// offset, so no consumer-group derivation in this batch applies to them.
	ShareGroups []ShareGroup `json:"share_groups,omitempty"`

	// Principal is the SASL username the agent authenticated as, and the "Z" in
	// "grant X on Y to Z" when a backend renders an authorized_operations gap. It
	// is a username, never a credential, and is empty for anonymous or
	// mTLS-authenticated connections, where the broker's principal is derived
	// from the certificate and the agent cannot know it.
	Principal string `json:"principal,omitempty"`

	Agent    AgentStats        `json:"agent"`
	Sections []Section         `json:"sections"`
	Errors   []CollectionError `json:"errors,omitempty"`

	// Truncation is nil on a complete batch. Non-nil means at least one of its
	// counters is non-zero and the corresponding list is short.
	Truncation *Truncation `json:"truncation,omitempty"`
	// Limits echoes the caps that produced this batch, so a backend can tell
	// "the cluster has 40 topics" from "the agent was told to ship 40". Nil
	// when every cap is unlimited.
	Limits *Limits `json:"limits,omitempty"`
}

// Truncation counts what a cycle collected but did not ship. The counts travel
// even when the entities do not, turning a silent falsehood into a known
// unknown.
type Truncation struct {
	Topics     int `json:"topics,omitempty"`
	Partitions int `json:"partitions,omitempty"`
	Groups     int `json:"groups,omitempty"`
	Offsets    int `json:"offsets,omitempty"`
	// ErrorsCollapsed is occurrences folded into an existing entry's Count by
	// deduplication. Nothing is lost: the total survives in Count.
	ErrorsCollapsed int `json:"errors_collapsed,omitempty"`
	// ErrorsDropped is entries a cap refused outright. These are lost.
	ErrorsDropped int `json:"errors_dropped,omitempty"`
}

// Add accumulates one section's drops into a batch-level total.
func (t *Truncation) Add(o Truncation) {
	t.Topics += o.Topics
	t.Partitions += o.Partitions
	t.Groups += o.Groups
	t.Offsets += o.Offsets
	t.ErrorsCollapsed += o.ErrorsCollapsed
	t.ErrorsDropped += o.ErrorsDropped
}

// Limits is the cap configuration in force for a batch. Zero means unlimited,
// which is the default for every entity cap.
type Limits struct {
	MaxErrors             int `json:"max_errors,omitempty"`
	MaxErrorSamples       int `json:"max_error_samples,omitempty"`
	MaxTopics             int `json:"max_topics,omitempty"`
	MaxPartitionsPerTopic int `json:"max_partitions_per_topic,omitempty"`
	MaxGroups             int `json:"max_groups,omitempty"`
	MaxOffsetsPerGroup    int `json:"max_offsets_per_group,omitempty"`
}

// SectionStatus is a CLOSED enum in schema version 1: a v1 consumer may switch
// on it exhaustively, so adding a value forces a version bump. That is why
// truncation is an orthogonal boolean on Section rather than a status —
// truncation is emission policy, status is collection health. Do not merge them.
type SectionStatus string

// SectionOK and the four statuses below are the complete set. See
// SectionStatus for why nothing may be added to it in schema version 1.
const (
	SectionOK           SectionStatus = "ok"
	SectionPartial      SectionStatus = "partial"
	SectionFailed       SectionStatus = "failed"
	SectionUnauthorized SectionStatus = "unauthorized"
	SectionSkipped      SectionStatus = "skipped"
)

// Section records the health and timing of one collection phase. A consumer
// must treat SectionFailed as "no data", never as "no entities" — the
// difference between a broken coordinator and a deleted group.
type Section struct {
	Name       string        `json:"name"`
	Status     SectionStatus `json:"status"`
	SampledAt  time.Time     `json:"sampled_at"`
	DurationMs int64         `json:"duration_ms"`
	// ErrorCount is how many entries in Batch.Errors carry this section's name.
	// It is NOT an occurrence count — deduplication is always on, so one entry
	// can stand for many identical failures; sum CollectionError.Count (absent
	// meaning 1) for that. Nor is it a count of distinct failure modes, unless
	// MAX_ERROR_SAMPLES is 1 (the default): above that, one failure mode is
	// emitted verbatim up to that many times before folding.
	ErrorCount      int  `json:"error_count,omitempty"`
	ErrorsCollapsed int  `json:"errors_collapsed,omitempty"`
	ErrorsDropped   int  `json:"errors_dropped,omitempty"`
	Truncated       bool `json:"truncated,omitempty"`
}

// CollectionError is an attributed failure. Unlike a bare error string it says
// which API, broker, topic, partition or group produced it.
type CollectionError struct {
	Section   string `json:"section"`
	API       string `json:"api,omitempty"`
	BrokerID  *int32 `json:"broker_id,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Partition *int32 `json:"partition,omitempty"`
	// Dir is the log directory a storage failure belongs to: a
	// KAFKA_STORAGE_ERROR is only actionable once you know which mount died.
	Dir   string `json:"dir,omitempty"`
	Group string `json:"group,omitempty"`
	Code  int16  `json:"kafka_error_code,omitempty"`
	// Kind is a coarse class: "authorization", "coordinator", "transport",
	// "unsupported", "other". Used to route alerts without parsing Message.
	Kind    string `json:"kind,omitempty"`
	Message string `json:"message"`
	// Count is how many identical occurrences this entry represents. Absent
	// means 1. When >1 the topic/partition/group fields are an EXEMPLAR — the
	// first occurrence — not the full affected set, which stays recoverable from
	// the per-entity error_code fields on topics[]/groups[]/offsets[].
	Count int `json:"count,omitempty"`
}

// AgentStats is the agent's self-telemetry. Without it, "agent crashed",
// "export queue dropping", "ingest rejecting" and "cluster is quiet" are
// indistinguishable at the backend.
type AgentStats struct {
	UptimeSec        int64  `json:"uptime_sec"`
	BatchesCollected uint64 `json:"batches_collected"`
	BatchesExported  uint64 `json:"batches_exported"`
	BatchesDropped   uint64 `json:"batches_dropped"`
	BatchesRejected  uint64 `json:"batches_rejected"`
	ExportRetries    uint64 `json:"export_retries"`
	QueueDepth       int    `json:"queue_depth"`
	LastExportError  string `json:"last_export_error,omitempty"`

	// RPC is the agent's view of its own Kafka traffic. Nil when the hooks are
	// disabled; see RPCStats for why it hangs off AgentStats rather than off
	// cluster.brokers[].
	RPC *RPCStats `json:"rpc,omitempty"`
}

// RPCStats is agent-observed broker telemetry, gathered from kgo hooks on
// requests the agent already sends. It belongs under AgentStats and NOT under
// ClusterMetrics because it is a property of one agent's connection to a broker,
// not of the broker: two agents in different network zones legitimately report
// different latencies and different connect failures for the same node, and a
// backend that averaged them as "broker health" would erase the one distinction
// that makes the signal useful — "broker down" versus "unreachable from here".
//
// Counters are per-window deltas, reset every cycle, so a restart cannot produce
// a spike; divide by WindowMs, never by the collection interval.
type RPCStats struct {
	// WindowMs is the time the counters below accumulated over: from the previous
	// reset to this batch's CollectedAt. It is not the collection interval — a
	// slow cycle or a missed tick makes it longer.
	WindowMs int64 `json:"window_ms"`
	// Brokers is sorted by BrokerID. A NEGATIVE ID is a kgo seed-broker
	// placeholder for a connection made before the node identified itself; it must
	// not be joined against cluster.brokers[].
	Brokers []BrokerRPC `json:"brokers"`
}

// BrokerRPC is one broker's RPC health over the window, as this agent saw it.
type BrokerRPC struct {
	BrokerID int32 `json:"broker_id"`
	// Requests is per API key, sorted by API name. Absent APIs were not issued
	// this window, which says nothing about the broker.
	Requests []BrokerAPIRPC `json:"requests"`

	// ConnectAttempts and ConnectFailures count dials that completed the full
	// handshake (ApiVersions plus any SASL flow), so a failure here is
	// "unreachable OR rejected the credentials", not "TCP refused". The two are
	// separated only by LastConnectError.
	ConnectAttempts uint64 `json:"connect_attempts"`
	ConnectFailures uint64 `json:"connect_failures"`
	// ConnectLatency covers successful dials only, so its Count is
	// ConnectAttempts - ConnectFailures.
	ConnectLatency LatencyHistogram `json:"connect_latency"`
	// Disconnects counts closed connections. Idle reaping and LB timeouts produce
	// them steadily on a healthy cluster: a non-zero value is not an incident, a
	// value that tracks ConnectAttempts is.
	Disconnects      uint64 `json:"disconnects"`
	LastConnectError string `json:"last_connect_error,omitempty"`

	// ThrottleEvents and ThrottledMs are broker-imposed quota throttling of THIS
	// agent's principal. They are evidence about the agent's own quota, not about
	// cluster load, and must never be read as a client-facing throttle rate.
	ThrottleEvents uint64 `json:"throttle_events"`
	ThrottledMs    int64  `json:"throttled_ms"`
}

// BrokerAPIRPC is one API key's traffic to one broker over the window. API is
// the kmsg request name lowercased ("metadata", "list_offsets", "offset_fetch"),
// not the numeric key, so a backend never carries a key table.
type BrokerAPIRPC struct {
	API string `json:"api"`
	// E2E is kgo's DurationE2E: TimeToWrite + ReadWait + TimeToRead. It EXCLUDES
	// WriteWait deliberately.
	E2E LatencyHistogram `json:"e2e"`
	// WriteWait is client-side queueing inside the agent. It is a separate series
	// because folding it into E2E would make agent contention read as a broker
	// problem, which is the exact misdiagnosis this section exists to prevent.
	WriteWait LatencyHistogram `json:"write_wait"`
	// Errors counts requests whose write or read failed. A request that returned a
	// Kafka error code is a success here — it completed on the wire.
	Errors uint64 `json:"errors"`
	// BytesWritten and BytesRead exclude TLS overhead. They are the agent's own
	// byte cost against this broker, the answer to "what does the agent cost us".
	BytesWritten int64 `json:"bytes_written"`
	BytesRead    int64 `json:"bytes_read"`
}

// DefaultLatencyBoundsUs is the bucket layout for every LatencyHistogram the
// agent emits, in microseconds. It is fixed rather than configurable so that
// buckets add across agents and across cycles without re-bucketing.
var DefaultLatencyBoundsUs = []int64{500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000}

// LatencyHistogram is counts plus a sum plus explicit bucket bounds, and
// deliberately NOT a percentile. A p99 computed per cycle cannot be aggregated:
// percentiles do not average, so a backend given one p99 per 30s can never
// answer "p99 over the last hour" or "p99 across brokers". Counts and sums add
// exactly, and shipping the bounds makes the backend's interpolation
// reproducible instead of guessed.
type LatencyHistogram struct {
	Count int64 `json:"count"`
	SumUs int64 `json:"sum_us"`
	// MaxUs is the largest single observation, null when Count is 0. The top
	// bucket is open-ended, so this is the only bound on the tail.
	MaxUs *int64 `json:"max_us"`
	// BoundsUs are inclusive upper bounds, ascending. Counts has one MORE entry:
	// the last is the overflow bucket above the final bound. Counts are
	// PER-BUCKET, not cumulative.
	BoundsUs []int64 `json:"bounds_us"`
	Counts   []int64 `json:"counts"`
}

// EnsureBuckets installs the fixed bucket layout on a histogram that has never
// been observed, so bounds_us and counts are arrays rather than null even at
// count 0. That case is the steady state, not an edge: kgo pools connections, so
// a healthy broker's connect_latency sees no dial for hours while its row is
// created every cycle. A consumer that zips bounds with counts, or adds counts
// element-wise across brokers, would otherwise hit a null on nearly every batch.
func (h *LatencyHistogram) EnsureBuckets() {
	if h.Counts == nil {
		h.BoundsUs = DefaultLatencyBoundsUs
		h.Counts = make([]int64, len(DefaultLatencyBoundsUs)+1)
	}
}

// Observe records one duration, initialising the default bucket layout on first
// use so no caller can invent an incompatible one.
func (h *LatencyHistogram) Observe(d time.Duration) {
	h.EnsureBuckets()
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	h.Count++
	h.SumUs += us
	if h.MaxUs == nil || us > *h.MaxUs {
		seen := us
		h.MaxUs = &seen
	}
	i := 0
	for i < len(h.BoundsUs) && us > h.BoundsUs[i] {
		i++
	}
	h.Counts[i]++
}

// ClusterMetrics is the cluster's identity and broker inventory.
type ClusterMetrics struct {
	ID          string   `json:"id"`
	BrokerCount int      `json:"broker_count"`
	Brokers     []Broker `json:"brokers"`

	// Capabilities is the ApiVersions fingerprint. It lives here, not in the
	// envelope, because it describes the CLUSTER: the envelope is what the agent
	// is and what this batch is, and a backend keyed on cluster.id must not have
	// to reach into agent-scoped fields to answer "can this cluster report an
	// LSO". Two agents watching one cluster must agree on it; they cannot be made
	// to agree on anything in the envelope.
	//
	// Nil means the fingerprint was not refreshed this cycle — it runs on its own
	// cadence, like log_dirs — NOT that the cluster has no capabilities. Carry the
	// last non-null forward per cluster.id.
	Capabilities *ClusterCapabilities `json:"capabilities,omitempty"`
}

// Capability names carried in ClusterCapabilities.Features. Each is a wire
// contract shared by the collector that gates on it and the backend that renders
// "absent because the broker is too old" from it, so both sides use the constant.
const (
	// CapabilityLastStableOffset is ListOffsets v2+. Below it, franz-go drops the
	// isolation level on downgrade and a broker answers ListCommittedOffsets with
	// the high watermark, silently: this flag is the ONLY tell.
	CapabilityLastStableOffset = "last_stable_offset"
	// CapabilityGroupStateFilter is ListGroups v4+ (the StatesFilter field).
	CapabilityGroupStateFilter = "group_state_filter"
	// CapabilityConsumerGroupDescribe is the KIP-848 ConsumerGroupDescribe key.
	CapabilityConsumerGroupDescribe = "consumer_group_describe"
	// CapabilityLogDirs is DescribeLogDirs v3+, the first version with a top-level
	// error code; below it an unauthorized principal gets an empty result.
	CapabilityLogDirs = "log_dirs"
	// CapabilityLogDirsVolumeBytes is DescribeLogDirs v4+ (KIP-827), which added
	// TotalBytes and UsableBytes.
	CapabilityLogDirsVolumeBytes = "log_dirs_volume_bytes"
	// CapabilityOffsetForLeaderEpoch is the OffsetForLeaderEpoch key.
	CapabilityOffsetForLeaderEpoch = "offset_for_leader_epoch"
	// CapabilityListOffsetsAfterMilli is ListOffsets v1+, the first version that
	// returns the timestamp of the offset it found.
	CapabilityListOffsetsAfterMilli = "list_offsets_after_milli"
	// CapabilityReassignments is the ListPartitionReassignments key.
	CapabilityReassignments = "list_partition_reassignments"
)

// API names used as keys in BrokerCapability.APIMaxVersions. Only the keys the
// agent can issue are reported; the full ~68-key table is not wire data.
const (
	APIKeyMetadata                   = "metadata"
	APIKeyListOffsets                = "list_offsets"
	APIKeyOffsetFetch                = "offset_fetch"
	APIKeyListGroups                 = "list_groups"
	APIKeyDescribeGroups             = "describe_groups"
	APIKeyConsumerGroupDescribe      = "consumer_group_describe"
	APIKeyDescribeLogDirs            = "describe_log_dirs"
	APIKeyDescribeCluster            = "describe_cluster"
	APIKeyOffsetForLeaderEpoch       = "offset_for_leader_epoch"
	APIKeyListPartitionReassignments = "list_partition_reassignments"
)

// ClusterCapabilities is what the cluster can answer, so a backend can tell
// "this signal is absent because the broker is too old" from "absent because
// collection broke". Without it those two are the same empty field.
type ClusterCapabilities struct {
	// Features maps a Capability* name to whether EVERY reachable broker supports
	// it — the minimum, because a request can land on any broker. A key that is
	// present and false is a probed, unsupported feature; a key that is ABSENT was
	// not probed. Do not read a missing key as false.
	Features map[string]bool `json:"features"`

	// SoftwareVersions is the sorted distinct set of broker version guesses.
	// These are INFERRED from the ApiVersions key ranges, not reported by the
	// broker, so they identify a protocol generation ("3.7") and must not be used
	// as a build identifier or fed to a CVE matcher.
	SoftwareVersions []string `json:"software_versions"`
	// MixedVersions is len(SoftwareVersions) > 1 over the brokers that answered:
	// a rolling upgrade in flight, or one node that missed the last one. It is a
	// fact about this batch, not a trend — the agent computes no trends.
	MixedVersions bool `json:"mixed_versions"`

	// Brokers is one row per broker in the metadata, sorted by BrokerID. A broker
	// whose probe failed still gets a row, with a null APIMaxVersions; compare
	// len() against cluster.broker_count to find brokers missing entirely.
	Brokers []BrokerCapability `json:"brokers"`
}

// BrokerCapability is one broker's software version and the max version it
// accepts for each API the agent uses.
type BrokerCapability struct {
	BrokerID int32 `json:"broker_id"`
	// SoftwareVersion is empty when the probe failed. See
	// ClusterCapabilities.SoftwareVersions for why it is not a build identifier.
	SoftwareVersion string `json:"software_version,omitempty"`
	// APIMaxVersions is the raw material Features is derived from, keyed by the
	// APIKey* names. Null means this broker did not answer — look for a
	// CollectionError carrying its broker_id — and is NOT "supports nothing". A
	// key absent from a non-null map is an API this broker does not implement.
	APIMaxVersions map[string]int16 `json:"api_max_versions"`
}

// Broker is one node. Rack is nil when the broker declares none, which is not
// the same as an empty rack name.
type Broker struct {
	ID   int32   `json:"id"`
	Host string  `json:"host"`
	Port int32   `json:"port"`
	Rack *string `json:"rack,omitempty"`
}

// TopicMetrics is one topic and its partitions.
type TopicMetrics struct {
	Name string `json:"name"`
	// ID is the base64 topic UUID. It distinguishes a recreated topic from the
	// original, the only way to tell a legitimate offset reset from corrupt data.
	ID       string `json:"id,omitempty"`
	Internal bool   `json:"internal"`

	// PartitionCount is the broker's partition count, computed BEFORE any cap.
	// len(Partitions) < PartitionCount means the partition list was truncated.
	PartitionCount int `json:"partition_count"`
	// ReplicationFactor is the minimum replica count across partitions, a
	// summary only; per-partition truth is Partition.Replicas, which diverges
	// during a reassignment.
	ReplicationFactor int         `json:"replication_factor"`
	Partitions        []Partition `json:"partitions"`
	ErrorCode         int16       `json:"error_code,omitempty"`
}

// Partition is one partition's placement and its three offset marks. The
// offsets are nullable because a failed lookup must stay distinguishable from
// an empty partition.
type Partition struct {
	ID          int32 `json:"id"`
	Leader      int32 `json:"leader"`
	LeaderEpoch int32 `json:"leader_epoch"`

	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	OfflineReplicas []int32 `json:"offline_replicas,omitempty"`

	// StartOffset is the log start offset. Nil when the lookup failed.
	StartOffset *int64 `json:"start_offset"`
	// EndOffset is the high watermark, NOT the last stable offset. On
	// transactional topics it counts aborted records and commit markers, so it
	// is the wrong denominator for a read_committed consumer: use
	// LastStableOffset for those, and (EndOffset - LastStableOffset) as the
	// open-transaction backlog.
	EndOffset *int64 `json:"end_offset"`
	// LastStableOffset is the LSO: the first offset of the earliest open
	// transaction, or the high watermark when none is open. A read_committed
	// consumer cannot advance past it, so its lag is (last_stable_offset -
	// committed), never (end_offset - committed).
	//
	// Nil means not known this cycle: the LSO phase is disabled, or the lookup
	// failed. It is sampled between the committed offsets and the high
	// watermarks, so start_offset <= committed <= last_stable_offset <=
	// end_offset holds despite the skew between the three calls.
	//
	// One case has no data-side tell: ListOffsets carries an isolation level
	// only from v2 (Kafka 0.11), and franz-go drops the field when it
	// downgrades, so a pre-0.11 broker silently answers with the high
	// watermark. The topics_lso section status tells "disabled" from "failed";
	// nothing distinguishes "too old" yet.
	LastStableOffset *int64 `json:"last_stable_offset"`

	// MaxTimestamp is the newest record's timestamp and the offset carrying it
	// (ListOffsets timestamp -3, KIP-734). Absent when the phase did not run.
	//
	// It answers "when was the last produce" directly, rather than inferring it
	// from a run of zero end-offset deltas -- an inference that cannot tell a
	// silent topic from a missed cycle.
	//
	// MEASURED TRAP, do not compare Offset to EndOffset naively. This phase is
	// issued AFTER topics_end, so on a live partition Offset is routinely
	// GREATER than EndOffset: records arrived between the two samples. Observed
	// on a real cluster: offset 72182 against an end offset of 72175. A
	// backwards-timestamp check is only sound when the partition did not advance
	// between the two samples -- compare the sections' sampled_at first, and skip
	// the partition when EndOffset moved.
	MaxTimestamp *MaxTimestampOffset `json:"max_timestamp,omitempty"`

	// Tiered is the tiered-storage view (KIP-405 / KIP-1005). Absent when the
	// phase did not run, which is the normal case: it is only worth asking on a
	// cluster with remote storage enabled.
	Tiered *TieredOffsets `json:"tiered,omitempty"`

	// Window is the broker's answer for Batch.ThroughputWindow on this partition.
	// Absent when the window phase did not run; read the section status, not the
	// key's absence.
	Window *WindowOffset `json:"window,omitempty"`

	ErrorCode int16 `json:"error_code,omitempty"`
}

// ThroughputWindow is the server-measured window one cycle asked about, via
// ListOffsetsAfterMilli. The agent ships the window, not a rate: a rate needs
// two samples and the agent keeps none, whereas (end_offset - window.offset)
// over (topics_end.sampled_at - window.timestamp_ms) is complete inside ONE
// batch. That is what makes it correct on the first batch after a restart and
// immune to missed cycles — both endpoints are measured by the broker, so agent
// clock skew and interval jitter cannot enter.
type ThroughputWindow struct {
	// RequestedMs is the epoch-millisecond the agent asked about, on the AGENT's
	// clock. Never use it as the window edge: use WindowOffset.TimestampMs, which
	// is the broker's.
	RequestedMs int64 `json:"requested_ms"`
	// WidthMs is the configured width — how far back RequestedMs was meant to
	// reach. It states intent independently of either clock.
	WidthMs int64 `json:"width_ms"`
}

// WindowOffset is one partition's first offset at or after
// ThroughputWindow.RequestedMs.
//
// A consumer must NOT conclude: that the window is WidthMs wide (records may
// start much later than the request — TimestampMs is the true edge); that offset
// deltas are record counts on a compacted topic; or that the numbers mean
// anything under CreateTime, where the timestamp index reflects PRODUCER clocks
// and a skewed producer moves the edge.
type WindowOffset struct {
	// Offset is the first offset at or after the requested timestamp. When the
	// partition has no records after it, the broker returns the CURRENT END
	// OFFSET, so end - offset is legitimately 0 for a silent partition. Nil means
	// the lookup failed.
	Offset *int64 `json:"offset"`
	// TimestampMs is the record timestamp the broker found, the true window edge
	// and the correct denominator. Nil when the broker returned -1, which is the
	// no-records-after case above: pair it with Offset == end_offset.
	TimestampMs *int64 `json:"timestamp_ms"`
	// LeaderEpoch carries no omitempty; 0 is a real epoch and -1 means unknown. A
	// change between the window offset and the current partition epoch means a
	// leader changed mid-window and the delta may cross a truncation.
	LeaderEpoch int32 `json:"leader_epoch"`
	ErrorCode   int16 `json:"error_code,omitempty"`
}

// Reassignment is one partition the controller reports as moving. It answers
// "is this URP a failure or a planned move" — the largest false-positive source
// in URP alerting.
//
// A consumer must NOT conclude that an absent partition is settled: the probe
// runs only for partitions already observed under-replicated, and a move that
// finished between the two calls vanishes from the list rather than reporting
// completion. Percent-complete comes from joining AddingReplicas against
// log_dirs[].partitions[].size, not from anything here.
type Reassignment struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	// Replicas is the controller's current replica set, which may differ from the
	// metadata view in topics[] — the two calls are not simultaneous.
	Replicas         []int32 `json:"replicas"`
	AddingReplicas   []int32 `json:"adding_replicas"`
	RemovingReplicas []int32 `json:"removing_replicas"`
}

// EpochProbe is one OffsetForLeaderEpoch result: positive proof of truncation,
// as opposed to the high-watermark-went-backwards heuristic.
//
// The proof is CommittedOffset > EndOffset with ErrorCode 0 and a non-null
// ReturnedEpoch: the group committed past the last offset that ever existed in
// the epoch it committed at, so every record in [end_offset, committed_offset)
// was lost AND was consumed.
//
// A consumer must NOT conclude loss from the row's mere presence: the probe
// fires on any committed-vs-current epoch mismatch, which every ordinary leader
// election produces. Nor is absence proof of no loss — the probe needs a
// committed leader epoch, and a group that committed with epoch -1 can never be
// probed.
type EpochProbe struct {
	Topic string `json:"topic"`
	// TopicID is the topic UUID at probe time, so the proof cannot be invalidated
	// later by a delete-and-recreate that reuses the name.
	TopicID   string `json:"topic_id,omitempty"`
	Partition int32  `json:"partition"`
	// GroupID is the group whose commit triggered the probe and is the party
	// proven to have consumed the lost records.
	GroupID string `json:"group_id"`

	// CommittedOffset and CommittedEpoch are what the group had committed, and
	// CommittedEpoch is the epoch that was ASKED about.
	CommittedOffset *int64 `json:"committed_offset"`
	CommittedEpoch  int32  `json:"committed_epoch"`
	// CurrentLeaderEpoch is the partition's epoch at trigger time. Its difference
	// from CommittedEpoch is the trigger, not the finding.
	CurrentLeaderEpoch int32 `json:"current_leader_epoch"`

	// ReturnedEpoch is the epoch the leader answered with. It is LOWER than
	// CommittedEpoch when the requested epoch existed but held no records, and -1
	// when the leader does not know it — in which case EndOffset proves nothing.
	// Null means the probe returned no usable answer.
	ReturnedEpoch *int32 `json:"returned_epoch"`
	// EndOffset is the end offset the leader reports for the requested epoch:
	// the log end offset when the epoch is current, otherwise the first offset of
	// the next epoch. Null when the probe failed.
	EndOffset *int64 `json:"end_offset"`
	// NodeID is the leader that answered. Null when no broker answered.
	NodeID    *int32 `json:"node_id"`
	ErrorCode int16  `json:"error_code,omitempty"`
}

// LogDir is one log directory on one broker. Its rows are per REPLICA, not per
// partition: a partition with replication factor 3 appears in three LogDirs,
// which is what makes this the only view that can attribute bytes to a disk.
type LogDir struct {
	Broker int32  `json:"broker"`
	Dir    string `json:"dir"`

	// TotalBytes and UsableBytes are the KIP-827 volume figures — the
	// denominator for any days-to-full forecast. They are populated when every
	// broker serves DescribeLogDirs v4; null means not known this cycle: a
	// cluster below v4, where the fields are not on the wire and kadm's decoder
	// drops them in any case, or the broker's -1 sentinel, which MUST be
	// translated to null and never shipped as a negative size. Do not substitute
	// sum(Partitions.Size): with a topic filter in force that is a lower bound on
	// the directory's usage, not the usage.
	//
	// They describe the VOLUME, not the directory. Two log dirs on one mount
	// report identical figures, so summing them across dirs double-counts the
	// disk; group by (broker, total_bytes, usable_bytes) before adding, or
	// forecast per dir.
	TotalBytes  *int64 `json:"total_bytes"`
	UsableBytes *int64 `json:"usable_bytes"`

	// ErrorCode is the directory-level error. 56 (KAFKA_STORAGE_ERROR) means
	// this log directory is OFFLINE — a dead disk, reported by the broker that
	// owns it rather than inferred from a peer's offline_replicas. Partitions
	// is nil in that case, which is not the same as an empty directory.
	ErrorCode  int16             `json:"error_code,omitempty"`
	Partitions []LogDirPartition `json:"partitions"`
}

// LogDirPartition is one replica's storage in one log directory.
//
// Size and OffsetLag are plain int64 deliberately: the broker sends no
// per-partition error code and no -1 sentinel here, so a row that exists always
// carries two real values. Unlike TotalBytes/UsableBytes, unknown cannot occur.
type LogDirPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	// Size is the on-disk bytes of this replica's log segments.
	Size int64 `json:"size"`
	// OffsetLag is how far this REPLICA trails: max(high watermark - log end
	// offset, 0) normally, or (log end offset - future log end offset) when
	// IsFuture. Per-replica, which partition-level ISR data cannot express.
	OffsetLag int64 `json:"offset_lag"`
	// IsFuture marks an in-flight intra-broker JBOD move. It explains disk
	// growth that would otherwise read as runaway.
	IsFuture bool `json:"is_future,omitempty"`
}

// GroupMetrics is one consumer group.
type GroupMetrics struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Coordinator int32  `json:"coordinator"`
	// Generation is the highest generation advertised in any member's consumer
	// join metadata, or -1 when no member advertised one. It is NOT the group's
	// authoritative generation: DescribeGroups carries no generation field, so
	// this comes from the sticky assignor's hint, which range and
	// cooperative-sticky leave at -1 — classic-protocol groups report -1 here
	// permanently. Do not build rebalance detection on it; a detector keyed on
	// generation deltas would sit at zero forever and read as a stable cluster.
	// Use GroupEpoch.
	Generation   int32  `json:"generation"`
	Protocol     string `json:"protocol,omitempty"`
	ProtocolType string `json:"protocol_type,omitempty"`
	// GroupType is "classic" or "consumer" (KIP-848), from ListGroups v5+.
	// Empty below v5, which is not the same as "classic": a cluster that cannot
	// report the type must not be read as one where every group is classic.
	//
	// kadm's ListedGroup discards this field, so the agent issues its own raw
	// ListGroups -- the same request it was already broadcasting, not an extra
	// one. Same reason T1.9 reads log-dir volume bytes off a raw request.
	GroupType string `json:"group_type,omitempty"`
	// MemberCount is the coordinator's member count, computed BEFORE any cap.
	// len(Members) < MemberCount means the member list was truncated.
	MemberCount int           `json:"member_count"`
	Members     []GroupMember `json:"members"`
	ErrorCode   int16         `json:"error_code,omitempty"`

	// The fields below come from ConsumerGroupDescribe (KIP-848) and are null on
	// a classic-protocol group, on a broker older than Kafka 4.0, and when
	// COLLECT_CONSUMER_GROUPS is off. Null means "not applicable or not known",
	// never zero — group epochs legitimately start at 0.

	// GroupEpoch is the AUTHORITATIVE group epoch, unlike Generation. The
	// coordinator bumps it on every reconciliation, so a delta is a real
	// rebalance count and a rising epoch with a static AssignmentEpoch is a
	// stalled reconciliation. Build rebalance alerting on this.
	GroupEpoch *int32 `json:"group_epoch,omitempty"`
	// AssignmentEpoch is the epoch the current assignment was computed at. It
	// trails GroupEpoch while a rebalance is in flight; a persistent gap is a
	// group that cannot converge.
	AssignmentEpoch *int32 `json:"assignment_epoch,omitempty"`
	// Assignor is the server-side assignor the group selected. Under KIP-848
	// assignment moved to the coordinator, so unlike Protocol this is chosen by
	// the broker rather than negotiated between members.
	Assignor string `json:"assignor,omitempty"`
}

// GroupMember is one member of a consumer group.
type GroupMember struct {
	MemberID string `json:"member_id"`
	// InstanceID is the KIP-345 static membership ID. When set, it is the
	// stable identity across rejoins; MemberID is not.
	InstanceID *string `json:"instance_id,omitempty"`
	ClientID   string  `json:"client_id"`
	Host       string  `json:"host"`
	Rack       *string `json:"rack,omitempty"`

	// SubscribedTopics is what the member asked for at join.
	SubscribedTopics []string `json:"subscribed_topics,omitempty"`
	// Assignment is what the leader gave it.
	Assignment []TopicPartition `json:"assignment"`
	// Owned is what the member claims to still own (KIP-429). A persistent
	// difference from Assignment is a wedged cooperative rebalance.
	Owned []TopicPartition `json:"owned,omitempty"`

	// The fields below come from ConsumerGroupDescribe (KIP-848); see
	// GroupMetrics for when they are null.

	// MemberEpoch is the epoch this member has acknowledged. A member whose
	// epoch trails GroupEpoch while every other member has caught up is the
	// single member blocking the group's reconciliation.
	MemberEpoch *int32 `json:"member_epoch,omitempty"`
	// TargetAssignment is what the coordinator intends this member to own;
	// Assignment is what it currently owns. Under KIP-848 reconciliation is
	// incremental, so a lasting difference is the new-protocol equivalent of a
	// wedged cooperative rebalance.
	TargetAssignment []TopicPartition `json:"target_assignment,omitempty"`
	// SubscribedTopicRegex is set when the member subscribed by pattern rather
	// than by name (new protocol only).
	SubscribedTopicRegex *string `json:"subscribed_topic_regex,omitempty"`
}

// TopicPartition names one partition of one topic.
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// ConsumerOffset is one group's committed offsets.
type ConsumerOffset struct {
	GroupID string `json:"group_id"`
	// ErrorCode is non-zero when the group's offsets could not be fetched at
	// all, and Offsets is nil. The entry is still emitted so "could not fetch"
	// stays distinguishable from "has committed nothing" without cross-checking
	// the section status.
	ErrorCode int16             `json:"error_code,omitempty"`
	Offsets   []PartitionOffset `json:"offsets"`
	// OffsetCount is how many offsets survived filtering, BEFORE any cap. It
	// carries no omitempty for the same reason PartitionCount and MemberCount
	// do not: len(Offsets) < OffsetCount is the only per-entity truncation
	// signal, and it must be readable without cross-checking the batch.
	OffsetCount int `json:"offset_count"`
}

// PartitionOffset is one group's commit for one partition.
type PartitionOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	// Offset is the committed offset. Nil means the group has never committed
	// for this partition — it must not be reported as lag against the whole
	// backlog.
	Offset *int64 `json:"offset"`
	// LeaderEpoch carries no omitempty: a genuine epoch of 0 is common and must
	// not look like an absent field, because epoch comparison is how offset
	// regressions are told apart from corruption. -1 means unknown.
	LeaderEpoch int32  `json:"leader_epoch"`
	Metadata    string `json:"metadata,omitempty"`
	ErrorCode   int16  `json:"error_code,omitempty"`
}

// ConfigEntry is one allowlisted configuration key.
//
// Value is a pointer because a config genuinely having no value is distinct
// from having an empty one, and because a SENSITIVE config arrives with its
// value stripped by the broker. Nil therefore means "not disclosed", never "".
//
// Source is the wire enum rendered as a name (DYNAMIC_TOPIC_CONFIG,
// STATIC_BROKER_CONFIG, DEFAULT_CONFIG, ...). It is what separates "somebody set
// this" from "this is the shipped default", which is the whole of config-drift
// detection and of a config-change timeline.
type ConfigEntry struct {
	Key   string  `json:"key"`
	Value *string `json:"value"`
	// Sensitive is echoed so a backend can render "redacted by the broker"
	// rather than "unset". The agent never ships a sensitive value even when a
	// broker sends one.
	Sensitive bool   `json:"sensitive,omitempty"`
	Source    string `json:"source,omitempty"`
}

// TopicConfig is the allowlisted config of one topic.
//
// The two keys this section exists for:
//
//   - cleanup.policy. On a compacted topic the offset range is not a record
//     count: compaction removes records and leaves the offsets consumed, so
//     end_offset - committed_offset counts gaps that hold nothing. Consumer lag
//     on a compacted topic is overstated by an unknowable amount, and without
//     this key a backend cannot even mark the topic as unreliable. That is a
//     correction; everything else here is a feature.
//   - min.insync.replicas. len(isr) < len(replicas) means redundancy is
//     reduced. len(isr) < min.insync.replicas means every acks=all produce is
//     failing right now. Identical wire data, opposite severity, and the second
//     is unknowable without this key.
type TopicConfig struct {
	Topic     string        `json:"topic"`
	ErrorCode int16         `json:"error_code,omitempty"`
	Configs   []ConfigEntry `json:"configs"`
}

// BrokerConfig is the allowlisted config of one broker.
//
// Per-broker rather than cluster-wide on purpose: DescribeBrokerConfigs with no
// broker IDs answers with cluster-level DYNAMIC config only, which cannot show
// drift and would arrive labelled with whichever broker served it. "Broker 3
// has a different min.insync.replicas" is silently fatal and invisible to every
// other tool, and it is only visible if each broker is asked about itself.
//
// offsets.retention.minutes is the key that turns consumer overrun from a
// post-mortem into a prediction: it is how long the coordinator keeps a group's
// committed offsets after the group empties.
type BrokerConfig struct {
	Broker    int32         `json:"broker"`
	ErrorCode int16         `json:"error_code,omitempty"`
	Configs   []ConfigEntry `json:"configs"`
}

// MaxTimestampOffset is ListOffsets at timestamp -3: the largest record
// timestamp in the partition, and the offset carrying it.
//
// Both fields are nullable for the usual reason -- a broker that could not
// answer must not look like one that answered zero -- and additionally because
// a partition holding no records has no max timestamp at all.
type MaxTimestampOffset struct {
	Offset      *int64 `json:"offset"`
	TimestampMs *int64 `json:"timestamp_ms"`
	LeaderEpoch int32  `json:"leader_epoch"`
	ErrorCode   int16  `json:"error_code,omitempty"`
}

// TieredOffsets is the pair of tiered-storage boundaries.
//
// On a cluster with remote storage, StartOffset (the global earliest) can sit far
// below what is actually on the broker's disk. A consumer reading between
// LocalStartOffset and StartOffset still succeeds -- and every fetch it issues
// goes to object storage, at object-storage latency. Nothing else in the batch
// tells that apart from a healthy consumer, which is the gap this closes.
//
// The two fields have different version floors (ListOffsets v8 and v9), so one
// may be null while the other is set. That is a real state, not an error.
type TieredOffsets struct {
	// LocalStartOffset is the earliest offset on the broker's own disk
	// (timestamp -4, KIP-405). Null below ListOffsets v8.
	LocalStartOffset *int64 `json:"local_start_offset"`
	// RemoteEndOffset is the latest offset in remote storage (timestamp -5,
	// KIP-1005). Null below ListOffsets v9. StartOffset minus this is how far
	// behind the archival tier is running.
	RemoteEndOffset *int64 `json:"remote_end_offset"`
}

// ShareGroup is one KIP-932 share group. Kafka 4.0+.
//
// Share groups are NOT consumer groups under another name, and none of the
// consumer-group derivations apply to them. There is no partition ownership --
// members share partitions and acknowledge individual records -- so there is no
// per-partition committed offset, no member assignment to diff, and no lag in
// the committed-versus-end sense. They travel in their own section for exactly
// that reason: folding them into groups[] would let every existing consumer
// signal silently describe something it does not model.
//
// StartOffsets is the closest analogue to a committed offset: the share-partition
// start offset, before which records are no longer deliverable.
type ShareGroup struct {
	ID              string `json:"id"`
	State           string `json:"state"`
	Coordinator     int32  `json:"coordinator"`
	GroupEpoch      int32  `json:"group_epoch"`
	AssignmentEpoch int32  `json:"assignment_epoch"`
	Assignor        string `json:"assignor,omitempty"`
	ErrorCode       int16  `json:"error_code,omitempty"`

	MemberCount int                `json:"member_count"`
	Members     []ShareGroupMember `json:"members"`

	// StartOffsets is the share-partition start offset per topic-partition.
	// Empty both when the offsets phase did not run and when the group holds
	// none -- read the share_groups section status, never len().
	StartOffsets []ShareGroupOffset `json:"start_offsets,omitempty"`
}

// ShareGroupMember is one member of a share group. Assignment here is which
// partitions the member may fetch from, NOT which it exclusively owns.
type ShareGroupMember struct {
	MemberID         string           `json:"member_id"`
	ClientID         string           `json:"client_id"`
	Host             string           `json:"host"`
	Rack             *string          `json:"rack,omitempty"`
	MemberEpoch      int32            `json:"member_epoch"`
	SubscribedTopics []string         `json:"subscribed_topics,omitempty"`
	Assignment       []TopicPartition `json:"assignment,omitempty"`
}

// ShareGroupOffset is one share-partition start offset.
type ShareGroupOffset struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	StartOffset *int64 `json:"start_offset"`
	LeaderEpoch int32  `json:"leader_epoch"`
	ErrorCode   int16  `json:"error_code,omitempty"`
}
