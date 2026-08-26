package metrics

import "time"

// SchemaVersion is the version of the Batch wire format. Bump on any
// backwards-incompatible change to the JSON produced by this package.
//
// Everything added since v1 is an OPTIONAL field, so a v1 consumer that ignores
// unknown keys stays correct. The one change that forces a bump is a new
// SectionStatus value: see the closed-enum note on that type.
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

	Cluster ClusterMetrics   `json:"cluster"`
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

	// GroupStates is the fast-poll group-state observation window. It is attached
	// to the next full batch rather than shipped as its own batch, so "one batch
	// per collection interval" still holds.
	GroupStates *GroupStateWatch `json:"group_states,omitempty"`

	// EpochProbes are OffsetForLeaderEpoch results. Normally absent entirely: the
	// probe fires only on a committed-vs-current leader-epoch mismatch.
	EpochProbes []EpochProbe `json:"epoch_probes,omitempty"`

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
	Members    int `json:"members,omitempty"`
	Offsets    int `json:"offsets,omitempty"`
	// GroupStateTransitions is fast-poll transitions dropped by a cap. The
	// per-state counts in GroupStateWindow.States stay true, so a dwell
	// distribution is short but a rebalance count is not.
	GroupStateTransitions int `json:"group_state_transitions,omitempty"`
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
	t.Members += o.Members
	t.Offsets += o.Offsets
	t.GroupStateTransitions += o.GroupStateTransitions
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
	MaxMembersPerGroup    int `json:"max_members_per_group,omitempty"`
	MaxOffsetsPerGroup    int `json:"max_offsets_per_group,omitempty"`
	// MaxTransitionsPerGroup caps the fast poll's per-group transition list. A
	// rebalance storm is exactly when that list is longest and exactly when the
	// signal matters, so a cap here trades dwell samples for a bounded batch.
	MaxTransitionsPerGroup int `json:"max_transitions_per_group,omitempty"`
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
	Controller  int32    `json:"controller"`
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

	// AuthorizedOperations is the principal's permissions on the CLUSTER
	// resource. Nil and empty mean different things: see AuthorizedOps.
	AuthorizedOperations *AuthorizedOps `json:"authorized_operations,omitempty"`
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
	// CapabilityGroupState is ListGroups v1+, where the response first carried
	// State. Below it every fast-poll observation has an empty state.
	CapabilityGroupState = "group_state"
	// CapabilityConsumerGroupDescribe is the KIP-848 ConsumerGroupDescribe key.
	CapabilityConsumerGroupDescribe = "consumer_group_describe"
	// CapabilityLogDirs is DescribeLogDirs v3+, the first version with a top-level
	// error code; below it an unauthorized principal gets an empty result.
	CapabilityLogDirs = "log_dirs"
	// CapabilityLogDirsVolumeBytes is DescribeLogDirs v4+ (KIP-827), which added
	// TotalBytes and UsableBytes.
	CapabilityLogDirsVolumeBytes = "log_dirs_volume_bytes"
	// CapabilityTopicAuthorizedOps is Metadata v8+.
	CapabilityTopicAuthorizedOps = "topic_authorized_operations"
	// CapabilityClusterAuthorizedOps is Metadata v8-v10 or DescribeCluster: the
	// cluster bitfield was REMOVED from Metadata in v11, so on a modern broker
	// this is false unless DescribeCluster is reachable.
	CapabilityClusterAuthorizedOps = "cluster_authorized_operations"
	// CapabilityGroupAuthorizedOps is DescribeGroups v3+.
	CapabilityGroupAuthorizedOps = "group_authorized_operations"
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

// AuthorizedOps is the broker's answer to "what may this principal do to this
// resource", the input to "grant X on Y to principal Z".
//
// The nil/empty distinction is the whole point and must survive: a nil
// *AuthorizedOps means the broker did not report the bitfield (too old, or the
// field was omitted), while a non-nil value with an empty Operations means the
// broker reported that NOTHING is permitted. A consumer must not render an
// absent bitfield as a missing grant, and must not render an empty one as
// "unknown".
//
// Note that kadm's DecodeACLOperations collapses both cases to a nil slice, so
// the collector decides emission from the capability flag, not from the decoded
// length.
type AuthorizedOps struct {
	// Bitfield is the raw int32 the broker sent. The broker's "omitted" sentinel
	// (math.MinInt32) must be shipped as a nil *AuthorizedOps, never as a number.
	// Null here means the value reached the agent already decoded and the raw
	// bitfield was lost; Operations is still authoritative.
	Bitfield *int32 `json:"bitfield"`
	// Operations are Kafka ACL operation names ("READ", "DESCRIBE",
	// "DESCRIBE_CONFIGS"), sorted. This is deliberately NOT an enum on the wire:
	// Kafka adds operations, and an unknown name must be forwarded, not dropped.
	// Bits that decode to no known operation are omitted, so Operations can be
	// shorter than Bitfield's population count.
	Operations []string `json:"operations"`
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

	// AuthorizedOperations is the principal's permissions on this topic. A topic
	// visible in metadata with DESCRIBE but nothing else is exactly the case a
	// backend renders as "grant DESCRIBE_CONFIGS on topic X to Y".
	AuthorizedOperations *AuthorizedOps `json:"authorized_operations,omitempty"`
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

// GroupStateWatch is one window of fast-poll group-state observations, attached
// to the next full batch.
//
// It ships RAW OBSERVATIONS, not a percentile. The roadmap asks for
// "time-in-PreparingRebalance p99", and the agent deliberately does not compute
// it: percentiles do not aggregate across windows or agents, a per-window p99
// over a handful of rebalances is meaningless, and the agent's whole stance is
// that derivation belongs at the backend where the history lives. Transitions
// plus per-state counts are strictly more information than any statistic derived
// from them.
type GroupStateWatch struct {
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	// PollIntervalMs is the configured fast-poll cadence. It is the resolution
	// floor: no dwell shorter than this is observable, so a zero-length
	// PreparingRebalance means "shorter than the poll", never "instant".
	PollIntervalMs int64 `json:"poll_interval_ms"`
	// Polls is how many polls completed, MissedPolls how many were skipped or
	// failed. Dwell totals are understated by exactly the missed span, so a window
	// with MissedPolls > 0 must not be aggregated as if it were continuous.
	Polls       int `json:"polls"`
	MissedPolls int `json:"missed_polls"`
	// Groups is sorted by GroupID and contains only groups that were observed at
	// least once in the window.
	Groups []GroupStateWindow `json:"groups"`
}

// GroupStateWindow is one group's observed state over the watch window.
type GroupStateWindow struct {
	GroupID     string `json:"group_id"`
	Coordinator int32  `json:"coordinator"`
	// StateAtStart is the state at the first poll of the window and StateAtEnd at
	// the last; both are empty on a broker below Kafka 2.6, which does not
	// populate State in ListGroups. Empty is "not reported", never a state name.
	StateAtStart string `json:"state_at_start"`
	StateAtEnd   string `json:"state_at_end"`

	// Transitions are observed state changes in time order. Timestamps are when
	// the AGENT SAW the change, so each is late by up to PollIntervalMs and a
	// transition pair that both fell between two polls is invisible.
	Transitions []GroupStateTransition `json:"transitions"`
	// TransitionCount is the pre-cap count and stays TRUE when the list is
	// truncated, so len(Transitions) < TransitionCount is self-describing.
	TransitionCount int `json:"transition_count"`

	// States is per-state occupancy over the window, sorted by state name. It
	// survives truncation of Transitions.
	States []GroupStateDwell `json:"states"`
}

// GroupStateTransition is one observed state change. From is empty when the
// group was first seen in this window, which is not a transition from nothing —
// the group may have held that state for hours.
type GroupStateTransition struct {
	At   time.Time `json:"at"`
	From string    `json:"from"`
	To   string    `json:"to"`
}

// GroupStateDwell is how long one group spent in one state during the window.
type GroupStateDwell struct {
	State string `json:"state"`
	// Entries is how many times the state was entered, Completed how many of
	// those also ended inside the window. Only completed intervals have a true
	// duration; the backend builds its dwell distribution from those.
	Entries   int `json:"entries"`
	Completed int `json:"completed"`
	// ObservedMs is total time seen in this state, INCLUDING any interval still
	// open at the window edge. That part is right-censored: it is a lower bound on
	// a dwell, and treating it as a completed sample biases every percentile down.
	ObservedMs int64 `json:"observed_ms"`
	// CompletedMs is the duration of each completed interval, in observation
	// order — the raw samples a percentile is computed from. Empty when Completed
	// is 0.
	CompletedMs []int64 `json:"completed_ms"`
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
	// MemberCount is the coordinator's member count, computed BEFORE any cap.
	// len(Members) < MemberCount means the member list was truncated.
	MemberCount int           `json:"member_count"`
	Members     []GroupMember `json:"members"`
	ErrorCode   int16         `json:"error_code,omitempty"`

	// AuthorizedOperations is the principal's permissions on this group, from
	// DescribeGroups v3+. Nil and empty differ: see AuthorizedOps.
	AuthorizedOperations *AuthorizedOps `json:"authorized_operations,omitempty"`

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
