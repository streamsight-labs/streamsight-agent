package collector

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/kafka"
	"kafka-metrics-agent/internal/metrics"
)

// Options configures a Collector. The zero value is usable: no filtering, no
// per-call deadline, internal topics excluded.
type Options struct {
	// Timeout bounds one Collect call end to end; zero relies on the caller's
	// context alone.
	Timeout time.Duration

	// IncludeInternalTopics includes topics the broker flags as internal.
	IncludeInternalTopics bool

	// An empty include matches everything; exclude wins over include.
	TopicIncludeRegex string
	TopicExcludeRegex string
	GroupIncludeRegex string
	GroupExcludeRegex string

	// GroupStates optionally restricts ListGroups to these states, in the
	// broker's own capitalisation. Empty lists all groups. The broker applies it
	// before building the response, so it reports no truncation.
	//
	// It needs ListGroups v4 (KIP-518, Kafka 2.6+); below that the field is not
	// serialised and every group comes back silently, so agent.New probes
	// ApiVersions at startup and refuses to start.
	GroupStates []string

	// CollectConsumerGroups issues ConsumerGroupDescribe (KIP-848) for the epochs
	// the classic describe cannot see for a new-protocol group. agent.New turns
	// it off when the cluster is too old to answer it.
	CollectConsumerGroups bool

	// CollectLastStableOffset issues ListCommittedOffsets between the committed
	// and end-offset samples, filling Partition.LastStableOffset. Without it the
	// only ceiling on the wire is the high watermark, so every read_committed
	// consumer on a transactional topic reports permanent false lag. config.Load
	// defaults it ON.
	CollectLastStableOffset bool
	// CollectLogDirs issues DescribeAllLogDirs. It needs no new ACL but its
	// response is O(replicas), the largest payload the agent emits, so it
	// defaults off.
	CollectLogDirs bool
	// LogDirsEvery runs the log-dirs phase on every Nth cycle; below 1 means
	// every cycle. Disks fill over hours, so COLLECTION_INTERVAL is the wrong
	// cadence for an O(replicas) response.
	LogDirsEvery int

	// Limits caps what one batch may contain. The zero value is unlimited.
	Limits Limits

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
	limits Limits
	log    *slog.Logger

	// cycle drives the log-dirs cadence. Atomic because nothing in this package's
	// contract requires Collect to be called serially.
	cycle atomic.Uint64
}

// New builds a Collector. It fails only on a malformed filter regex.
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
		limits: opts.Limits,
		log:    log,
	}, nil
}

// Collect runs one cycle.
//
// Phase order is a correctness constraint, not an optimisation. The offset
// chain is sampled in the order start ≤ committed ≤ LSO ≤ high watermark, so
// that the unavoidable skew between the calls makes every derived difference
// look slightly large rather than slightly negative: a caught-up consumer must
// never report -3, and a hung transaction must never report a negative backlog
// and read as healthy.
//
//	cluster  ─┬─────────────────────────────► topics (start offsets)
//	          │                                  │
//	list groups ─┬─► groups (describe)           │ waits
//	             └─► offsets (committed) ────────┤
//	                                             ▼
//	                            topics_lso (last stable offsets)
//	                                             │
//	                                             ▼
//	                            topics_end (end offsets)
//	                                             │
//	                                             ▼
//	                            log_dirs (every Nth cycle)
//
// Each offset flavour gets its own section because each is sampled measurably
// later than the last; see the section name constants.
//
// log_dirs is the one phase whose order does not matter for correctness. It
// waits on the end-offset sample anyway, so its O(replicas) response cannot
// inflate the very sample latency topics_end exists to pin down.
//
// AgentVersion, AgentInstanceID, BatchSeq and Agent are left zero for the agent
// loop to fill in.
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
		endDone       = make(chan struct{})

		cluster      metrics.ClusterMetrics
		topicDetails kadm.TopicDetails

		groupIDs      []string
		groupsDropped int
		listErr       error
		listStart     time.Time

		topics  []metrics.TopicMetrics
		groups  []metrics.GroupMetrics
		offsets []metrics.ConsumerOffset
		logDirs []metrics.LogDir

		clusterSec, groupsSec, offsetsSec, logDirsSec *section
		topicsSecs                                    []*section

		wg sync.WaitGroup
	)

	wg.Add(5)

	// Cluster metadata. Everything topic-shaped depends on it.
	go func() {
		defer wg.Done()
		defer close(metaDone)
		cluster, topicDetails, clusterSec = c.collectCluster(ctx)
	}()

	// One ListGroups broadcast per cycle, shared by both group phases;
	// close(groupsListed) publishes its results.
	go func() {
		defer close(groupsListed)
		listStart = time.Now()
		groupIDs, groupsDropped, listErr = c.listGroups(ctx)
	}()

	go func() {
		defer wg.Done()
		<-groupsListed
		groups, groupsSec = c.collectGroups(ctx, groupIDs, groupsDropped, listErr, listStart)
	}()

	// Committed offsets. Must complete before either ceiling — the last stable
	// offset or the high watermark — is requested, or both lag figures can come
	// out negative.
	go func() {
		defer wg.Done()
		defer close(committedDone)
		<-metaDone
		<-groupsListed
		offsets, offsetsSec = c.collectOffsets(ctx, groupIDs, groupsDropped, listErr, internalTopics(topicDetails))
	}()

	go func() {
		defer wg.Done()
		defer close(endDone)
		<-metaDone
		topics, topicsSecs = c.collectTopics(ctx, topicDetails, committedDone)
	}()

	// Log directories, on their own cadence. The counter is 0-based so a freshly
	// started agent samples on its first cycle rather than N intervals in.
	n := c.cycle.Add(1) - 1
	runDirs := c.opts.CollectLogDirs && n%logDirsEvery(c.opts.LogDirsEvery) == 0
	go func() {
		defer wg.Done()
		<-metaDone
		<-endDone
		logDirs, logDirsSec = c.collectLogDirs(ctx, cluster, topicDetails, runDirs)
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
		LogDirs:       logDirs,
	}

	secs := append([]*section{clusterSec}, topicsSecs...)
	secs = append(secs, groupsSec, offsetsSec, logDirsSec)
	c.finalize(batch, secs, groupsDropped)

	c.log.Debug("collected batch",
		"duration_ms", batch.CollectionMs,
		"topics", len(batch.Topics),
		"groups", len(batch.Groups),
		"offsets", len(batch.Offsets),
		"log_dirs", len(batch.LogDirs),
		"errors", len(batch.Errors),
		"errors_collapsed", truncationOf(batch).ErrorsCollapsed,
		"errors_dropped", truncationOf(batch).ErrorsDropped,
	)

	return batch
}

// finalize turns the finished sections into the batch's errors, sections,
// truncation and limits blocks. Step order is load-bearing: mergeErrors decides
// each section's ErrorsDropped and post-cap ErrorCount, so finish() cannot run
// before it.
//
// secs must be in canonical wire order — cluster, topics, topics_lso,
// topics_end, groups, offsets, log_dirs — because that is the order errors and
// sections are emitted in. Every one is present on every cycle, skipped or not:
// a missing section and an empty one mean different things.
func (c *Collector) finalize(batch *metrics.Batch, secs []*section, groupsDropped int) {
	batch.Errors, _ = mergeErrors(secs, c.limits.MaxErrors)

	// Counted once, here: MaxGroups is enforced once in listGroups and shortens
	// the groups and offsets sections by the same set, so counting it per
	// section would double it.
	trunc := metrics.Truncation{Groups: groupsDropped}
	for _, s := range secs {
		batch.Sections = append(batch.Sections, s.finish())
		if s != nil {
			trunc.Add(s.dropped)
		}
	}
	// Presence is the signal: a nil Truncation promises a complete batch.
	if trunc != (metrics.Truncation{}) {
		batch.Truncation = &trunc
	}
	if c.limits.any() {
		batch.Limits = c.limits.wire()
	}
}

// logDirsEvery clamps the cadence to at least one; zero would panic the modulo.
// config.Load already rejects both, so this only guards a hand-built Options.
func logDirsEvery(every int) uint64 {
	if every < 1 {
		return 1
	}
	return uint64(every)
}

// truncationOf is the nil-safe read of a batch's truncation block.
func truncationOf(b *metrics.Batch) metrics.Truncation {
	if b.Truncation == nil {
		return metrics.Truncation{}
	}
	return *b.Truncation
}
