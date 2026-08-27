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

	// CollectThroughputWindow issues ListOffsetsAfterMilli, the only rate input
	// that survives an agent restart. It adds a ListOffsets fan-out — two on a
	// mostly-silent cluster — to every cycle it runs on, so it defaults off.
	// agent.New turns it off when the cluster cannot serve it.
	CollectThroughputWindow bool
	// ThroughputWindowWidth is how far back the window reaches. It earns its cost
	// only when it is wider than the collection interval: inside one interval the
	// backend can already difference two batches.
	ThroughputWindowWidth time.Duration
	// ThroughputWindowEvery runs the window phase on every Nth cycle; below 1
	// means every cycle.
	ThroughputWindowEvery int

	// CollectMaxTimestamp adds partitions[].max_timestamp via ListOffsets at
	// timestamp -3 (KIP-734, Kafka 3.0+). One extra ListOffsets fan-out, no new
	// ACL: it is the same API key the four offset phases already use, and the
	// broker authorizes before it reads the timestamp field.
	//
	// It defaults on because it replaces an inference with a measurement --
	// "when was the last produce" is currently guessed from a run of zero
	// end-offset deltas, which cannot tell a silent topic from a missed cycle.
	CollectMaxTimestamp bool

	// CollectTieredOffsets adds partitions[].tiered.local_start_offset via
	// ListOffsets at timestamp -4 (KIP-405, Kafka 3.4+). Defaults OFF: on a
	// cluster without remote storage it is a round trip per cycle that returns
	// the same answer as the start offsets already collected.
	CollectTieredOffsets bool
	// CollectLatestTiered additionally asks for the remote end offset at
	// timestamp -5 (KIP-1005, Kafka 3.9+). Gated apart from the local start
	// because KIP-1005 landed five releases later, so 3.4-3.8 serves one and not
	// the other. Ignored when CollectTieredOffsets is false.
	CollectLatestTiered bool

	// CollectShareGroups adds the share_groups section (KIP-932, Kafka 4.0+).
	// Defaults off: no cluster below 4.0 can answer, and a share group is a
	// different data model from a consumer group rather than a variant of one.
	CollectShareGroups bool

	// CollectConfigs adds the topic_configs and broker_configs sections via
	// DescribeConfigs. It is the ONE phase that needs an ACL outside the
	// DESCRIBE on CLUSTER, TOPIC and GROUP the rest of the agent lives inside --
	// DESCRIBE_CONFIGS on TOPIC and on CLUSTER respectively -- and it defaults
	// ON regardless, because what it collects is a correction, not a feature.
	//
	// cleanup.policy is the only way to know a topic is compacted, and on a
	// compacted topic consumer lag is overstated by an unknowable amount.
	// Defaulting this off would ship that wrong number by default.
	// min.insync.replicas is the only way to tell "redundancy is reduced" from
	// "every acks=all produce is failing right now".
	//
	// A principal without the grant loses these two sections to `unauthorized`
	// and nothing else: no cycle fails, no other section degrades.
	CollectConfigs bool
	// ConfigsEvery runs the config phases on every Nth cycle; below 1 means
	// every cycle. Configs change on human timescales and the response is
	// O(topics + brokers) over an allowlist, so this wants to be the slowest
	// cadence in the agent.
	ConfigsEvery int

	// CollectReassignments asks the controller which under-replicated partitions
	// are moving on purpose. It costs nothing in steady state — the request is
	// issued only when a URP is observed — so it defaults on.
	CollectReassignments bool

	// CollectEpochProbes turns a committed-vs-current leader-epoch mismatch into
	// positive proof of truncation. It issues nothing until a mismatch appears,
	// so it defaults on.
	CollectEpochProbes bool

	// CollectRPCStats ships the counters the kgo hooks accumulate on traffic the
	// agent already sends. Zero extra requests, so it defaults on.
	CollectRPCStats bool

	// GroupStateWatch is the fast group-state poll, or nil when the watch is off
	// or the cluster is too old to populate ListedGroup.State. The agent owns its
	// goroutine; the collector only drains it. It is a live object rather than a
	// bool because the poll has to start before the first cycle to have a
	// baseline to transition from.
	GroupStateWatch *GroupStateWatcher

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

	// stateWatch is the agent's fast group-state poll, nil when it is not
	// running. The collector never starts or stops it.
	stateWatch *GroupStateWatcher

	// cycle drives the cadence of every phase that samples on every Nth cycle.
	// Atomic because nothing in this package's contract requires Collect to be
	// called serially.
	cycle atomic.Uint64

	// probed suppresses re-asking a leader-epoch question already answered; see
	// probedEpochs.
	probed probedEpochs
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
		client:     client,
		opts:       opts,
		topics:     topics,
		groups:     groups,
		limits:     opts.Limits,
		log:        log,
		stateWatch: opts.GroupStateWatch,
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
//	cluster  ─┬──────────────────────────────► topics (start offsets)
//	          │                                   │
//	          └─► topics_window (every Nth) ──────┤ waits
//	                                              │
//	list groups ─┬─► groups (describe)            │ waits
//	             └─► offsets (committed) ─────────┤
//	                                              ▼
//	                             topics_lso (last stable offsets)
//	                                              │
//	                                              ▼
//	                             topics_end (end offsets)
//	                                              │
//	                    ┌─────────────────────────┼─────────────────────────┐
//	                    ▼                         ▼                         ▼
//	          log_dirs (every Nth)   reassignments (on a URP)   epoch_probes (on a
//	                                                            committed-vs-current
//	                                                            epoch mismatch)
//
// Each offset flavour gets its own section because each is sampled measurably
// later than the last; see the section name constants.
//
// topics_window is in the chain for the same reason the others are: its offset
// is the earlier edge of a record count whose later edge is the high watermark,
// so issuing it after ListEndOffsets would make that count negative on a busy
// partition. It hangs off the metadata rather than off the start offsets because
// nothing orders it against them.
//
// log_dirs, reassignments and epoch_probes are the phases whose order does not
// matter for correctness. All three wait on the end-offset sample anyway, so
// their responses — O(replicas) for the first, triggered fan-outs for the other
// two — cannot inflate the very sample latency topics_end exists to pin down.
// The two triggered phases read the batch's OWN topics[] and offsets[] rather
// than raw metadata, so everything they emit has a join partner in the same
// batch.
//
// group_states and broker_rpc are outside the graph entirely: both drain
// accumulators and issue no request, so they run after wg.Wait(), which is what
// makes the RPC window cover this cycle's own traffic.
//
// AgentVersion, AgentInstanceID, BatchSeq, Principal and the envelope half of
// Agent are left zero for the agent loop to fill in.
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
		windowDone    = make(chan struct{})
		committedDone = make(chan struct{})
		endDone       = make(chan struct{})

		cluster      metrics.ClusterMetrics
		topicDetails kadm.TopicDetails

		groupIDs      []string
		groupTypes    map[string]string
		groupsDropped int
		listErr       error
		listStart     time.Time

		topics  []metrics.TopicMetrics
		groups  []metrics.GroupMetrics
		offsets []metrics.ConsumerOffset
		logDirs []metrics.LogDir

		window        *metrics.ThroughputWindow
		windowOffsets *windowSample
		reassignments []metrics.Reassignment
		probes        []metrics.EpochProbe
		topicConfigs  []metrics.TopicConfig
		brokerConfigs []metrics.BrokerConfig
		shareGroups   []metrics.ShareGroup

		clusterSec, windowSec, groupsSec, offsetsSec *section
		logDirsSec, reassignSec, epochSec            *section
		topicCfgSec, brokerCfgSec, shareSec          *section
		topicSecs                                    topicSections

		wg sync.WaitGroup
	)

	// The cycle counter is 0-based, so a freshly started agent samples every
	// cadenced phase on its first cycle rather than N intervals in.
	n := c.cycle.Add(1) - 1
	var (
		runWindow = runsThisCycle(c.opts.CollectThroughputWindow, c.opts.ThroughputWindowEvery, n)
		runDirs   = runsThisCycle(c.opts.CollectLogDirs, c.opts.LogDirsEvery, n)
		runCfg    = runsThisCycle(c.opts.CollectConfigs, c.opts.ConfigsEvery, n)
	)

	wg.Add(11)

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
		groupIDs, groupTypes, groupsDropped, listErr = c.listGroups(ctx)
	}()

	go func() {
		defer wg.Done()
		<-groupsListed
		groups, groupsSec = c.collectGroups(ctx, groupIDs, groupTypes, groupsDropped, listErr, listStart)
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

	// The server-measured window. It runs in parallel with the start offsets,
	// and closing windowDone is what guarantees its request precedes the high
	// watermarks rather than merely tending to.
	go func() {
		defer wg.Done()
		defer close(windowDone)
		<-metaDone
		window, windowOffsets, windowSec = c.collectThroughputWindow(ctx, topicDetails, c.opts.ThroughputWindowWidth, runWindow)
	}()

	go func() {
		defer wg.Done()
		defer close(endDone)
		<-metaDone
		topics, topicSecs = c.collectTopics(ctx, topicDetails, committedDone, windowDone)
	}()

	// Log directories, on their own cadence.
	go func() {
		defer wg.Done()
		<-metaDone
		<-endDone
		logDirs, logDirsSec = c.collectLogDirs(ctx, cluster, topicDetails, runDirs)
	}()

	// Share groups. Needs the cycle's group listing for the GroupType filter,
	// not the described groups: a share group is never in DescribeGroups.
	go func() {
		defer wg.Done()
		<-groupsListed
		shareGroups, shareSec = c.collectShareGroups(ctx, groupTypes, c.opts.CollectShareGroups)
	}()

	// Topic configs, on their own cadence. Needs the topics phase for the name
	// list, and waits on the end-offset sample for the same reason log_dirs
	// does: an O(topics) response must not inflate the latency topics_end
	// exists to pin down.
	go func() {
		defer wg.Done()
		<-endDone
		topicConfigs, topicCfgSec = c.collectTopicConfigs(ctx, topics, runCfg)
	}()

	// Broker configs, same cadence, different ACL. Needs only cluster metadata,
	// but waits on the end sample for the same latency reason.
	go func() {
		defer wg.Done()
		<-metaDone
		<-endDone
		brokerConfigs, brokerCfgSec = c.collectBrokerConfigs(ctx, cluster, runCfg)
	}()

	// Reassignments, triggered by a URP in the topics this cycle is shipping.
	go func() {
		defer wg.Done()
		<-endDone
		reassignments, reassignSec = c.collectReassignments(ctx, topics, c.opts.CollectReassignments)
	}()

	// Leader-epoch probes, triggered by a committed epoch that disagrees with the
	// partition's current one — which needs both finished phases.
	go func() {
		defer wg.Done()
		<-committedDone
		<-endDone
		probes, epochSec = c.collectEpochProbes(ctx, c.opts.CollectEpochProbes, topics, offsets)
	}()

	wg.Wait()

	// After the topics phase built the partitions, before finalize collects the
	// errors this records.
	windowOffsets.attach(topics)

	batch := &metrics.Batch{
		SchemaVersion:    metrics.SchemaVersion,
		CollectedAt:      start,
		Cluster:          cluster,
		Topics:           topics,
		Groups:           groups,
		Offsets:          offsets,
		LogDirs:          logDirs,
		ThroughputWindow: window,
		Reassignments:    reassignments,
		EpochProbes:      probes,
		TopicConfigs:     topicConfigs,
		BrokerConfigs:    brokerConfigs,
		ShareGroups:      shareGroups,
	}

	// Post-passes over the finished batch. Both drain accumulators and issue no
	// request of their own.
	var statesSec, rpcSec *section
	batch.GroupStates, statesSec = c.collectGroupStates(c.stateWatch)
	batch.Agent.RPC, rpcSec = c.collectRPC(c.opts.CollectRPCStats)

	// Stamped after the post-passes so collection_ms covers the whole cycle.
	batch.CollectionMs = time.Since(start).Milliseconds()

	secs := []*section{
		clusterSec,
		topicSecs.starts, windowSec, topicSecs.lso, topicSecs.end,
		topicSecs.maxTS, topicSecs.local, topicSecs.remote,
		groupsSec, offsetsSec,
		statesSec, epochSec,
		logDirsSec, reassignSec, topicCfgSec, brokerCfgSec, shareSec, rpcSec,
	}
	c.finalize(batch, secs, groupsDropped)

	c.log.Debug("collected batch",
		"duration_ms", batch.CollectionMs,
		"topics", len(batch.Topics),
		"groups", len(batch.Groups),
		"offsets", len(batch.Offsets),
		"log_dirs", len(batch.LogDirs),
		"reassignments", len(batch.Reassignments),
		"epoch_probes", len(batch.EpochProbes),
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
// secs must be in canonical wire order — the order of the section name block in
// errors.go — because that is the order errors and sections are emitted in.
// Every one is present on every cycle, skipped or not: a missing section and an
// empty one mean different things.
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

// runsThisCycle reports whether a cadenced phase samples on cycle n, which is
// 0-based.
func runsThisCycle(enabled bool, every int, n uint64) bool {
	return enabled && n%everyNth(every) == 0
}

// everyNth clamps a cadence to at least one; zero would panic the modulo.
// config.Load already rejects both, so this only guards a hand-built Options.
func everyNth(every int) uint64 {
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
