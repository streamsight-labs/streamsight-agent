package metrics

import "time"

// SchemaVersion is the version of the Batch wire format. Bump on any
// backwards-incompatible change to the JSON produced by this package.
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
}

// ClusterMetrics is the cluster's identity and broker inventory.
type ClusterMetrics struct {
	ID          string   `json:"id"`
	Controller  int32    `json:"controller"`
	BrokerCount int      `json:"broker_count"`
	Brokers     []Broker `json:"brokers"`
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

	ErrorCode int16 `json:"error_code,omitempty"`
}

// LogDir is one log directory on one broker. Its rows are per REPLICA, not per
// partition: a partition with replication factor 3 appears in three LogDirs,
// which is what makes this the only view that can attribute bytes to a disk.
type LogDir struct {
	Broker int32  `json:"broker"`
	Dir    string `json:"dir"`

	// TotalBytes and UsableBytes are the KIP-827 volume figures — the
	// denominator for any days-to-full forecast. They are ALWAYS null today,
	// because kadm's DescribeLogDirs decoder copies only Size, OffsetLag and
	// IsFuture; they are declared now so filling them in later is a collector
	// change, not a schema change. Do not substitute sum(Partitions.Size): with
	// a topic filter in force that is a lower bound on the directory's usage,
	// not the usage.
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
