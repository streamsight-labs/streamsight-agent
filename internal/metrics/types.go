package metrics

import "time"

// SchemaVersion is the version of the Batch wire format. Bump on any
// backwards-incompatible change to the JSON produced by this package.
const SchemaVersion = 1

// Batch is one collection cycle. It is the unit of export: the HTTP exporter
// POSTs one Batch per request, the file exporter writes one Batch per line.
//
// Nullable numeric fields (*int64, *int32) mean "not known this cycle" and MUST
// be distinguished from zero by consumers. A partition whose offset lookup
// failed reports null, never 0.
type Batch struct {
	SchemaVersion   int    `json:"schema_version"`
	AgentVersion    string `json:"agent_version"`
	AgentInstanceID string `json:"agent_instance_id"`
	BatchSeq        uint64 `json:"batch_seq"`

	// CollectedAt is stamped when the cycle starts. Per-section timing lives in
	// Sections; prefer Section.SampledAt for anything rate-derived.
	CollectedAt  time.Time `json:"collected_at"`
	CollectionMs int64     `json:"collection_ms"`

	Cluster ClusterMetrics   `json:"cluster"`
	Topics  []TopicMetrics   `json:"topics"`
	Groups  []GroupMetrics   `json:"groups"`
	Offsets []ConsumerOffset `json:"offsets"`

	Agent    AgentStats        `json:"agent"`
	Sections []Section         `json:"sections"`
	Errors   []CollectionError `json:"errors,omitempty"`
}

type SectionStatus string

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
	ErrorCount int           `json:"error_count,omitempty"`
}

// CollectionError is an attributed failure. Unlike a bare error string it says
// which API, broker, topic, partition or group produced it.
type CollectionError struct {
	Section   string `json:"section"`
	API       string `json:"api,omitempty"`
	BrokerID  *int32 `json:"broker_id,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Partition *int32 `json:"partition,omitempty"`
	Group     string `json:"group,omitempty"`
	Code      int16  `json:"kafka_error_code,omitempty"`
	// Kind is a coarse class: "authorization", "coordinator", "transport",
	// "unsupported", "other". Used to route alerts without parsing Message.
	Kind    string `json:"kind,omitempty"`
	Message string `json:"message"`
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

type ClusterMetrics struct {
	ID          string   `json:"id"`
	Controller  int32    `json:"controller"`
	BrokerCount int      `json:"broker_count"`
	Brokers     []Broker `json:"brokers"`
}

type Broker struct {
	ID   int32   `json:"id"`
	Host string  `json:"host"`
	Port int32   `json:"port"`
	Rack *string `json:"rack,omitempty"`
}

type TopicMetrics struct {
	Name string `json:"name"`
	// ID is the base64 topic UUID. It distinguishes a recreated topic from the
	// original, which is the only way to tell a legitimate offset reset from
	// corrupt data.
	ID       string `json:"id,omitempty"`
	Internal bool   `json:"internal"`

	PartitionCount int `json:"partition_count"`
	// ReplicationFactor is the minimum replica count across partitions. It is a
	// summary only; per-partition truth is Partition.Replicas, which diverges
	// during a reassignment.
	ReplicationFactor int         `json:"replication_factor"`
	Partitions        []Partition `json:"partitions"`
	ErrorCode         int16       `json:"error_code,omitempty"`
}

type Partition struct {
	ID          int32 `json:"id"`
	Leader      int32 `json:"leader"`
	LeaderEpoch int32 `json:"leader_epoch"`

	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	OfflineReplicas []int32 `json:"offline_replicas,omitempty"`

	// StartOffset is the log start offset. Nil when the lookup failed.
	StartOffset *int64 `json:"start_offset"`
	// EndOffset is the high watermark, not the last stable offset. On
	// transactional topics it counts aborted records and commit markers, so
	// read_committed consumers show residual lag by construction.
	EndOffset *int64 `json:"end_offset"`

	ErrorCode int16 `json:"error_code,omitempty"`
}

type GroupMetrics struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Coordinator int32  `json:"coordinator"`
	// Generation is the group's join generation, taken from member metadata
	// (v2+). It is the exact rebalance counter; -1 when unavailable. Prefer it
	// over member-ID churn, which over-reports for static members.
	Generation   int32         `json:"generation"`
	Protocol     string        `json:"protocol,omitempty"`
	ProtocolType string        `json:"protocol_type,omitempty"`
	MemberCount  int           `json:"member_count"`
	Members      []GroupMember `json:"members"`
	ErrorCode    int16         `json:"error_code,omitempty"`
}

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
}

type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

type ConsumerOffset struct {
	GroupID string `json:"group_id"`
	// ErrorCode is non-zero when the group's offsets could not be fetched at
	// all. The entry is still emitted so that "could not fetch" is
	// distinguishable from "has committed nothing" without cross-checking the
	// section status; Offsets is nil in that case.
	ErrorCode int16             `json:"error_code,omitempty"`
	Offsets   []PartitionOffset `json:"offsets"`
}

type PartitionOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	// Offset is the committed offset. Nil means the group has never committed
	// for this partition — it must not be reported as lag against the whole
	// backlog.
	Offset *int64 `json:"offset"`
	// LeaderEpoch carries no omitempty: a genuine epoch of 0 is common and must
	// not be indistinguishable from an absent field, because epoch comparison
	// is how offset regressions are told apart from corruption. -1 means
	// unknown.
	LeaderEpoch int32  `json:"leader_epoch"`
	Metadata    string `json:"metadata,omitempty"`
	ErrorCode   int16  `json:"error_code,omitempty"`
}
