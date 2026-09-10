package collector

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/streamsight-labs/streamsight-agent/internal/kafka"
	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// clusterClient is everything this package asks of a Kafka cluster: the eighteen
// kadm calls the phases issue, the raw-kmsg sharded request two of them fall back
// to for fields kadm decodes and drops (ListGroups v5's GroupType,
// DescribeLogDirs v4's TotalBytes/UsableBytes), the cached ApiVersions probe, and
// the RPC accumulator the post-pass drains.
//
// It is declared HERE, at the consumer, and it is exactly as wide as the phases'
// actual use rather than the ninety-odd methods *kadm.Client carries. Both
// properties are load-bearing. Declaring it here is what keeps internal/kafka
// free of any knowledge of the collector -- the interface belongs to the code
// that depends on it, not to the code that satisfies it, and putting it next to
// *kadm.Client would have made internal/kafka import internal/collector's
// vocabulary to describe its own field. Keeping it narrow is what makes adding a
// request to a phase a compile error until the fake in client_test.go grows an
// answer for it, so no phase can quietly start issuing something no test ever
// sees.
//
// The seam exists for one thing above all. Collect's phase order is a correctness
// constraint -- start <= committed <= LSO <= high watermark, see the diagram on
// Collect, the table in README.md and docs/ARCHITECTURE.md's "The wire order" --
// and it is realised by channel closes between eleven goroutines. Before this
// interface there was no way to observe the order the requests actually came out
// in, so the constraint was held by comments and by review; the only end-to-end
// Collect test that existed ran against a refused port, where every phase fails
// identically and order means nothing.
//
// *kafka.Client satisfies this because kafka.Client embeds *kadm.Client. The
// assertion below is what turns a signature drift on either side into a build
// failure instead of a surprise at startup.
type clusterClient interface {
	Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error)

	ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListCommittedOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListMaxTimestampOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListLocalLogStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListLatestRemoteOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListOffsetsAfterMilli(ctx context.Context, millisecond int64, topics ...string) (kadm.ListedOffsets, error)

	FetchManyOffsets(ctx context.Context, groups ...string) kadm.FetchOffsetsResponses
	DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error)
	DescribeConsumerGroups(ctx context.Context, groups ...string) (kadm.DescribedConsumerGroups, error)
	DescribeShareGroups(ctx context.Context, groups ...string) (kadm.DescribedShareGroups, error)
	DescribeShareGroupOffsets(ctx context.Context, groups ...string) (kadm.DescribedShareGroupsOffsets, error)

	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	DescribeBrokerConfigs(ctx context.Context, brokers ...int32) (kadm.ResourceConfigs, error)
	DescribeAllLogDirs(ctx context.Context, s kadm.TopicsSet) (kadm.DescribedAllLogDirs, error)
	ListPartitionReassignments(ctx context.Context, s kadm.TopicsSet) (kadm.ListPartitionReassignmentsResponses, error)
	OffsetForLeaderEpoch(ctx context.Context, r kadm.OffsetForLeaderEpochRequest) (kadm.OffsetsForLeaderEpochs, error)

	RequestSharded(ctx context.Context, req kmsg.Request) []kgo.ResponseShard
	Probe(ctx context.Context) (*kafka.Capabilities, error)
	RPCSnapshot() *metrics.RPCStats
}

var _ clusterClient = (*kafka.Client)(nil)
