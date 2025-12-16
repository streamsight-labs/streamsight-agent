package metrics

import "time"

// Batch is what we collect each cycle and print to console
type Batch struct {
	CollectedAt  time.Time        `json:"collected_at"`
	CollectionMs int64            `json:"collection_ms"`
	Cluster      ClusterMetrics   `json:"cluster"`
	Topics       []TopicMetrics   `json:"topics"`
	Groups       []GroupMetrics   `json:"groups"`
	Offsets      []ConsumerOffset `json:"offsets"`
	Errors       []string         `json:"errors,omitempty"`
}

type ClusterMetrics struct {
	ID          string   `json:"id"`
	Controller  int32    `json:"controller"`
	BrokerCount int      `json:"broker_count"`
	Brokers     []Broker `json:"brokers"`
}

type Broker struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
}

type TopicMetrics struct {
	Name              string      `json:"name"`
	PartitionCount    int         `json:"partition_count"`
	ReplicationFactor int         `json:"replication_factor"`
	Partitions        []Partition `json:"partitions"`
}

type Partition struct {
	ID          int32   `json:"id"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	StartOffset int64   `json:"start_offset"`
	EndOffset   int64   `json:"end_offset"`
}

type GroupMetrics struct {
	ID          string        `json:"id"`
	State       string        `json:"state"`
	Coordinator int32         `json:"coordinator"`
	MemberCount int           `json:"member_count"`
	Members     []GroupMember `json:"members"`
}

type GroupMember struct {
	MemberID   string           `json:"member_id"`
	ClientID   string           `json:"client_id"`
	Host       string           `json:"host"`
	Assignment []TopicPartition `json:"assignment"`
}

type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// ConsumerOffset is the committed offset for a consumer group
type ConsumerOffset struct {
	GroupID   string            `json:"group_id"`
	Offsets   []PartitionOffset `json:"offsets"`
}

type PartitionOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}
