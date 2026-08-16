package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/kafka"
	"kafka-metrics-agent/internal/metrics"
)

// Options configures a Collector. The zero value is usable: no filtering, no
// per-call deadline, internal topics excluded.
type Options struct {
	// Timeout bounds one Collect call end to end. Zero means the collector
	// relies solely on the caller's context.
	Timeout time.Duration

	// IncludeInternalTopics includes topics the broker flags as internal
	// (__consumer_offsets, __transaction_state). Their health is usually more
	// interesting than user topics, since their failure stalls every group.
	IncludeInternalTopics bool

	// Topic/Group regexes are optional. An empty include matches everything;
	// exclude wins over include.
	TopicIncludeRegex string
	TopicExcludeRegex string
	GroupIncludeRegex string
	GroupExcludeRegex string

	// GroupStates optionally restricts ListGroups to these states (Kafka 2.6+).
	// Empty lists all groups.
	GroupStates []string

	Logger *slog.Logger
}

// Collector turns one Kafka admin cycle into one metrics.Batch. It never
// returns an error: a cycle that fails in part still ships the parts that
// worked, and reports what failed in Batch.Sections and Batch.Errors.
type Collector struct {
	client *kafka.Client
	opts   Options
	topics *filter
	groups *filter
	log    *slog.Logger
}

// New builds a Collector. It fails only on a malformed filter regex, which is a
// config error worth surfacing at startup rather than every cycle.
func New(client *kafka.Client, opts Options) (*Collector, error) {
	topics, err := newFilter(opts.TopicIncludeRegex, opts.TopicExcludeRegex)
	if err != nil {
		return nil, err
	}
	groups, err := newFilter(opts.GroupIncludeRegex, opts.GroupExcludeRegex)
	if err != nil {
		return nil, err
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Collector{
		client: client,
		opts:   opts,
		topics: topics,
		groups: groups,
		log:    log,
	}, nil
}

// Collect runs one cycle.
//
// Phase order is a correctness constraint, not an optimisation. Committed
// offsets are sampled BEFORE end offsets so that the unavoidable skew between
// the two makes lag look slightly large rather than slightly negative: a
// caught-up consumer must never report -3.
//
//	cluster  ─┬─────────────────────────────► topics (start offsets)
//	          │                                  │
//	list groups ─┬─► groups (describe)           │ waits
//	             └─► offsets (committed) ────────┴─► topics_end (end offsets)
//
// End offsets get their own section because they are sampled measurably later
// than the rest of the topics phase; a rate derived from them must be divided
// by topics_end.sampled_at, not topics.sampled_at.
//
// The caller owns SchemaVersion's siblings: AgentVersion, AgentInstanceID,
// BatchSeq and Agent are left zero for the agent loop to fill in.
func (c *Collector) Collect(ctx context.Context) *metrics.Batch {
	start := time.Now()

	if c.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.opts.Timeout)
		defer cancel()
	}

	var (
		metaDone      = make(chan struct{})
		groupsListed  = make(chan struct{})
		committedDone = make(chan struct{})

		cluster      metrics.ClusterMetrics
		topicDetails kadm.TopicDetails

		groupIDs  []string
		listErr   error
		listStart time.Time

		topics  []metrics.TopicMetrics
		groups  []metrics.GroupMetrics
		offsets []metrics.ConsumerOffset

		clusterSec, groupsSec, offsetsSec *section
		topicsSecs                        []*section

		wg sync.WaitGroup
	)

	wg.Add(4)

	// Cluster metadata. Everything topic-shaped depends on it.
	go func() {
		defer wg.Done()
		defer close(metaDone)
		cluster, topicDetails, clusterSec = c.collectCluster(ctx)
	}()

	// One ListGroups broadcast per cycle, shared by both group phases.
	go func() {
		defer close(groupsListed)
		listStart = time.Now()
		groupIDs, listErr = c.listGroups(ctx)
	}()

	go func() {
		defer wg.Done()
		<-groupsListed
		groups, groupsSec = c.collectGroups(ctx, groupIDs, listErr, listStart)
	}()

	// Committed offsets. Must complete before end offsets are requested.
	go func() {
		defer wg.Done()
		defer close(committedDone)
		<-metaDone
		<-groupsListed
		offsets, offsetsSec = c.collectOffsets(ctx, groupIDs, listErr, internalTopics(topicDetails))
	}()

	go func() {
		defer wg.Done()
		<-metaDone
		topics, topicsSecs = c.collectTopics(ctx, topicDetails, committedDone)
	}()

	wg.Wait()

	batch := &metrics.Batch{
		SchemaVersion: metrics.SchemaVersion,
		CollectedAt:   start,
		CollectionMs:  time.Since(start).Milliseconds(),
		Cluster:       cluster,
		Topics:        topics,
		Groups:        groups,
		Offsets:       offsets,
	}

	secs := append([]*section{clusterSec}, topicsSecs...)
	secs = append(secs, groupsSec, offsetsSec)
	for _, s := range secs {
		batch.Sections = append(batch.Sections, s.finish())
		batch.Errors = append(batch.Errors, s.errs...)
	}

	c.log.Debug("collected batch",
		"duration_ms", batch.CollectionMs,
		"topics", len(batch.Topics),
		"groups", len(batch.Groups),
		"offsets", len(batch.Offsets),
		"errors", len(batch.Errors),
	)

	return batch
}
