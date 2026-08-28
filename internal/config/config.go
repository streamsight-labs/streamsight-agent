// Package config loads and validates the agent's entire environment surface at
// startup, so a typo fails loudly instead of silently falling back to a default
// (or panicking on the first tick).
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Export modes. These duplicate export.ModeFile / ModeHTTP / ModeStdout; config
// does not import export so the dependency stays one-way.
const (
	ExportModeFile   = "file"
	ExportModeHTTP   = "http"
	ExportModeStdout = "stdout"
)

// Defaults for every optional setting.
const (
	DefaultExportFile       = "./metrics.jsonl"
	DefaultExportFileMaxMB  = 100
	DefaultExportMaxBackups = 3
	// DefaultExportFileSync is off: a flush already survives the process dying,
	// and only power loss or a kernel panic needs a device round trip.
	DefaultExportFileSync = false
	// DefaultExportQueueSize is 20 batches = 100 seconds at the 5s interval.
	// The queue holds ENCODED bodies and evicts the OLDEST on overflow, so it is
	// a stall absorber rather than an outage archive: 100s covers the worst-case
	// retry chain for one batch, and metrics older than that have been overtaken
	// by fresher ones. Sizing it in batches means its wall-clock depth moves with
	// COLLECTION_INTERVAL.
	DefaultExportQueueSize  = 20
	DefaultExportMaxRetries = 3
	DefaultExportBaseDelay  = time.Second
	DefaultExportTimeout    = 10 * time.Second
	// DefaultExportEndpoint is the hosted ingest path. Self-hosted and on-prem
	// deployments override it; the agent POSTs to this URL verbatim and appends
	// no path of its own, so the route is a decision on the receiving side.
	DefaultExportEndpoint = "https://ingestion.streamsight.cloud/v1/batches"

	// DefaultInterval is 5s because that is the polling cadence the product
	// sells on every plan. It used to be 30s, and every _EVERY default below was
	// sized against that: each is a CYCLE COUNT, so its wall-clock meaning moves
	// when this does. They were all re-derived when this changed.
	DefaultInterval = 5 * time.Second
	DefaultLogLevel = "info"

	// DefaultCollectLSO is ON because it is a correctness fix, not a feature:
	// end_offset is the high watermark, so without the last stable offset every
	// read_committed consumer on a transactional topic reports permanent false
	// lag, with no failure anywhere to explain it. It costs one extra
	// ListOffsets fan-out per cycle and needs no new ACL.
	DefaultCollectLSO = true
	// DefaultCollectConsumerGroups is ON: it is the only way to see a
	// new-protocol group at all, since the classic describe returns one with
	// empty join metadata and NO error, i.e. a healthy-looking group with no
	// members. Startup probes the cluster and disables it when unsupported, so
	// an older cluster pays one ApiVersions round trip rather than a failing
	// request every cycle.
	DefaultCollectConsumerGroups = true
	// DefaultCollectLogDirs is ON. It is the only source of per-replica disk
	// bytes, and every storage question the product answers rests on it:
	// days-to-full per log dir, per-topic storage chargeback, follower lag in
	// bytes without JMX, JBOD imbalance, and the average record size that turns
	// every record rate in the product into a byte rate.
	//
	// The response is O(replicas) and is the largest section the agent emits,
	// which is what DefaultLogDirsEvery is for rather than a reason to leave the
	// data uncollected: at that cadence the section is absent from 96% of
	// batches, and it costs ~4 KB gzipped on the cycles it RUNS, which is ~166 B
	// per cycle amortised at LOG_DIRS_EVERY=24, measured on a 3-broker cluster at
	// 390 partitions RF 3.
	// It needs no ACL beyond the DESCRIBE on CLUSTER the agent already requires.
	//
	// Two things to know before sizing a large cluster. The figures describe the
	// VOLUME, not the directory, so two log dirs on one mount report identical
	// totals -- group by (broker, total_bytes, usable_bytes) before summing. And
	// MAX_PARTITIONS_PER_TOPIC does shrink this request, unlike the offset
	// listings, because the log-dir request names partitions explicitly.
	DefaultCollectLogDirs = true
	// DefaultLogDirsEvery samples once every two minutes at the 5s interval.
	//
	// MEASURED at 1,170 replicas (390 partitions RF 3): the section is 77.0 B per
	// replica, so it is ~90 KB against a ~174 KB steady batch -- it adds ~52% to
	// the payload on the cycles it runs. Disks fill over hours, so 720 samples a day is ample
	// for a days-to-full forecast, and it keeps the largest section off 96% of
	// batches. Offline-disk detection does not depend on this cadence: a dead
	// replica also shows in partitions[].offline_replicas every cycle.
	DefaultLogDirsEvery = 24

	// DefaultCollectThroughputWindow is OFF: the phase adds a ListOffsets
	// fan-out — two on a mostly-silent cluster, because the broker re-lists every
	// partition that answered -1 — to every cycle it runs on. It is the only rate
	// input that survives an agent restart, but a backend that never misses a
	// cycle can difference two batches instead.
	DefaultCollectThroughputWindow = false
	// DefaultThroughputWindow is sixty times the default interval. It was ten
	// times when the interval was 30s and it deliberately did not move when the
	// interval did: unlike every _EVERY knob this is a WALL-CLOCK width, not a
	// cycle count, so the sampling cadence does not change what it means. The
	// phase earns its cost only when the window is WIDER than COLLECTION_INTERVAL
	// — inside one interval the backend can already difference two batches — and
	// five minutes is the right denominator for a burn-down ETA however often the
	// agent samples.
	DefaultThroughputWindow = 5 * time.Minute
	// DefaultThroughputWindowEvery is every cycle: an operator who opted in wants
	// a rate on every batch. The knob exists for wide clusters.
	DefaultThroughputWindowEvery = 1

	// DefaultCollectConfigs is ON, and it is the only collector that needs an
	// ACL outside DESCRIBE on CLUSTER/TOPIC/GROUP. It defaults on anyway,
	// because what it collects is a CORRECTION rather than a feature.
	//
	// Without cleanup.policy nothing can tell that a topic is compacted, and on
	// a compacted topic the offset range is not a record count: compaction
	// removes records and leaves the offsets consumed, so end_offset minus
	// committed_offset counts gaps that hold nothing. Consumer lag -- the
	// headline number this agent exists to produce -- is then overstated by an
	// unknowable amount, and cannot even be flagged as unreliable. Defaulting
	// this off would mean the default build ships that wrong number.
	//
	// Missing the grant is not fatal and never blocks a cycle: the two sections
	// report `unauthorized` and every other section is unaffected. The grant to
	// add is DESCRIBE_CONFIGS on TOPIC and on CLUSTER respectively -- the two
	// sections are separate precisely so a principal holding one and not the
	// other sees which.
	DefaultCollectConfigs = true
	// DefaultConfigsEvery samples once every thirty minutes at the 5s interval,
	// the slowest cadence in the agent. Configs change when a human changes them.
	DefaultConfigsEvery = 360

	// DefaultCollectMaxTimestamp is ON. One extra ListOffsets fan-out, no new
	// ACL, and it replaces an inference with a measurement: topic liveness is
	// otherwise guessed from a run of zero end-offset deltas, which cannot tell
	// a silent topic from a missed cycle.
	DefaultCollectMaxTimestamp = true
	// DefaultMaxTimestampEvery samples once a minute at the 5s interval.
	//
	// It is the only offset flavour that is NOT O(1) on the broker. -1 and -2
	// read logEndOffset and logStartOffset, numbers already in memory; -3
	// (MAX_TIMESTAMP, KIP-734) makes UnifiedLog.fetchOffsetByTimestamp walk every
	// local segment comparing cached maxTimestampSoFar, then on Kafka 3.8+ do an
	// index lookup and scan the winning batch -- work that can reach page cache.
	// At 390 partitions, every cycle is 6.7M segment walks a day; this is 560k.
	//
	// MEASURED at 390 partitions: partitions[].max_timestamp
	// is 77.7 B per partition raw and 18.91% of the gzipped steady batch, the
	// largest single field the agent adds and one gzip cannot fold -- each value
	// is a distinct wide integer.
	//
	// A minute is the right cadence because of what the field ANSWERS: "when was
	// the last record written". Nobody alerts on a topic that went quiet five
	// seconds ago -- that is indistinguishable from ordinary traffic. They alert
	// on ten minutes. The cost of the cadence is that the answer is up to 60s
	// stale, which is well inside the resolution of the question.
	DefaultMaxTimestampEvery = 12
	// DefaultCollectTieredOffsets is OFF. On a cluster without remote storage
	// the local log start is always equal to the log start already collected, so
	// it is a round trip per cycle for a duplicate answer.
	DefaultCollectTieredOffsets = false
	// DefaultCollectShareGroups is OFF. KIP-932 needs Kafka 4.0, and the phase
	// has never been exercised against a broker that can answer it.
	DefaultCollectShareGroups = false

	// DefaultMaxErrors is the one cap that defaults ON. Behind deduplication
	// errors[] is already bounded by the number of distinct failure modes, so
	// this backstop is for a pathological cluster, not a policy.
	DefaultMaxErrors = 1000
	// DefaultMaxErrorSamples emits one verbatim occurrence per deduplication
	// key. Raising it hands back raw exemplars without a separate "disable
	// dedup" switch.
	DefaultMaxErrorSamples = 1

	// Every ENTITY cap defaults to 0 = unlimited. Any non-zero default would be
	// an untested guess that silently shortens the customer's core data on first
	// deploy, and TOPIC_INCLUDE_REGEX / GROUP_INCLUDE_REGEX already exist as the
	// intentional selection tool.
	DefaultMaxEntities = 0

	// collectionTimeoutRatio derives COLLECTION_TIMEOUT from
	// COLLECTION_INTERVAL when it is not set explicitly. A cycle that runs
	// longer than this eats into the next tick, which time.Ticker silently
	// coalesces.
	collectionTimeoutRatio = 80
)

// groupStates are the states a classic consumer group can be in. The broker
// compares the ListGroups filter to its own state strings byte for byte, so
// this list is both the validation set and the canonical capitalisation an
// operator's input is folded onto.
//
// The KIP-848 states (Assigning, Reconciling, NotReady) are deliberately absent:
// the agent describes groups over the classic DescribeGroups path, so a filter
// naming a new-protocol-only state would list groups it cannot describe.
var groupStates = []string{
	"Unknown",
	"PreparingRebalance",
	"CompletingRebalance",
	"Stable",
	"Dead",
	"Empty",
}

// canonicalGroupState folds an operator's spelling onto the broker's. Case is
// the only leniency: "stable" is plausible, "Stabel" is a typo that must fail.
func canonicalGroupState(s string) (string, bool) {
	for _, state := range groupStates {
		if strings.EqualFold(s, state) {
			return state, true
		}
	}
	return "", false
}

// Config is the agent's environment surface, already validated: nothing
// downstream needs to re-check a value or invent a fallback.
type Config struct {
	KafkaBrokers  []string
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	TLSEnabled    bool

	// ExportMode is always one of the ExportMode* constants after a successful
	// Load.
	ExportMode           string
	ExportEndpoint       string
	APIKey               string
	ExportFile           string
	ExportFileMaxMB      int
	ExportFileMaxBackups int
	ExportFileSync       bool
	ExportQueueSize      int
	ExportMaxRetries     int
	ExportBaseDelay      time.Duration
	ExportTimeout        time.Duration

	CollectionInterval    time.Duration
	CollectionTimeout     time.Duration
	IncludeInternalTopics bool
	TopicIncludeRegex     string
	TopicExcludeRegex     string
	GroupIncludeRegex     string
	GroupExcludeRegex     string
	// GroupStates restricts the ListGroups broadcast to these states, in the
	// broker's own capitalisation; empty means every state. It is the only
	// cardinality control the broker applies before building the response, so
	// the groups it removes never cross the wire — but it needs ListGroups v4
	// (KIP-518, Kafka 2.6+), and an older broker ignores the field in silence.
	// agent.New probes for that and refuses to start.
	GroupStates []string

	// Optional collectors. None needs an ACL beyond the DESCRIBE on
	// CLUSTER/TOPIC/GROUP the agent already requires, so these are payload and
	// latency switches, not permission switches.
	CollectLastStableOffset bool
	CollectConsumerGroups   bool
	CollectLogDirs          bool
	LogDirsEvery            int
	CollectThroughputWindow bool
	ThroughputWindow        time.Duration
	ThroughputWindowEvery   int
	CollectConfigs          bool
	ConfigsEvery            int
	CollectMaxTimestamp     bool
	MaxTimestampEvery       int
	CollectTieredOffsets    bool
	CollectShareGroups      bool

	// Cardinality caps. Zero means unlimited for every entity cap; MaxErrors and
	// MaxErrorSamples always have a positive default. They cap the batch, not
	// the cycle, which is why they carry no COLLECTION_ prefix.
	MaxErrors             int
	MaxErrorSamples       int
	MaxTopics             int
	MaxPartitionsPerTopic int
	MaxGroups             int
	MaxOffsetsPerGroup    int

	LogLevel        string
	AgentInstanceID string
}

// Load reads the environment. It reports every problem it finds rather than
// only the first, so a misconfigured deployment needs one restart, not five.
func Load() (*Config, error) {
	p := &parser{}
	c := &Config{}

	brokers := os.Getenv("KAFKA_BROKERS")
	c.KafkaBrokers = splitList(brokers)
	if len(c.KafkaBrokers) == 0 {
		p.errf("KAFKA_BROKERS is required (comma separated host:port list)")
	}

	c.SASLMechanism = os.Getenv("KAFKA_SASL_MECHANISM")
	c.SASLUsername = os.Getenv("KAFKA_SASL_USERNAME")
	c.SASLPassword = os.Getenv("KAFKA_SASL_PASSWORD")
	switch c.SASLMechanism {
	case "", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		p.errf("KAFKA_SASL_MECHANISM %q is not one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", c.SASLMechanism)
	}
	if c.SASLMechanism != "" && (c.SASLUsername == "" || c.SASLPassword == "") {
		p.errf("KAFKA_SASL_MECHANISM is set, so KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required")
	}
	c.TLSEnabled = p.boolean("KAFKA_TLS_ENABLED", false)

	// Explicit mode wins, otherwise http iff an endpoint is present. File mode
	// is the fallback so the agent runs with no network and no credentials.
	c.ExportEndpoint = strings.TrimSpace(os.Getenv("EXPORT_ENDPOINT"))
	c.APIKey = os.Getenv("API_KEY")
	c.ExportMode = strings.ToLower(strings.TrimSpace(os.Getenv("EXPORT_MODE")))
	if c.ExportMode == "" {
		if c.ExportEndpoint != "" {
			c.ExportMode = ExportModeHTTP
		} else {
			c.ExportMode = ExportModeFile
		}
	}
	switch c.ExportMode {
	case ExportModeFile, ExportModeStdout:
	case ExportModeHTTP:
		// Defaulted here rather than at the read above, and the placement is the
		// whole point: EXPORT_MODE is inferred as http IFF an endpoint is set, so
		// a default assigned earlier would flip every unconfigured agent from
		// file mode into posting at production. Inside this case the operator has
		// already chosen http, either explicitly or by setting an endpoint.
		if c.ExportEndpoint == "" {
			c.ExportEndpoint = DefaultExportEndpoint
		}
		if c.APIKey == "" {
			p.errf("API_KEY is required when EXPORT_MODE=http")
		}
	default:
		p.errf("EXPORT_MODE %q is not one of file, http, stdout", c.ExportMode)
	}

	c.ExportFile = p.str("EXPORT_FILE", DefaultExportFile)
	// A negative EXPORT_FILE_MAX_MB deliberately disables rotation; a negative
	// backup count has no meaning, so it is rejected below.
	c.ExportFileMaxMB = p.integer("EXPORT_FILE_MAX_MB", DefaultExportFileMaxMB)
	c.ExportFileMaxBackups = p.integer("EXPORT_FILE_MAX_BACKUPS", DefaultExportMaxBackups)
	c.ExportFileSync = p.boolean("EXPORT_FILE_FSYNC", DefaultExportFileSync)
	if c.ExportFileMaxBackups < 0 {
		p.errf("EXPORT_FILE_MAX_BACKUPS must be >= 0, got %d", c.ExportFileMaxBackups)
	}
	c.ExportQueueSize = p.integer("EXPORT_QUEUE_SIZE", DefaultExportQueueSize)
	if c.ExportQueueSize <= 0 {
		p.errf("EXPORT_QUEUE_SIZE must be > 0, got %d", c.ExportQueueSize)
	}
	c.ExportMaxRetries = p.integer("EXPORT_MAX_RETRIES", DefaultExportMaxRetries)
	if c.ExportMaxRetries < 0 {
		p.errf("EXPORT_MAX_RETRIES must be >= 0, got %d", c.ExportMaxRetries)
	}
	c.ExportBaseDelay = p.duration("EXPORT_BASE_DELAY", DefaultExportBaseDelay)
	if c.ExportBaseDelay <= 0 {
		p.errf("EXPORT_BASE_DELAY must be > 0, got %s", c.ExportBaseDelay)
	}
	c.ExportTimeout = p.duration("EXPORT_TIMEOUT", DefaultExportTimeout)
	if c.ExportTimeout <= 0 {
		p.errf("EXPORT_TIMEOUT must be > 0, got %s", c.ExportTimeout)
	}

	c.CollectionInterval = p.duration("COLLECTION_INTERVAL", DefaultInterval)
	if c.CollectionInterval <= 0 {
		// time.NewTicker panics on a non-positive duration. Catch it here.
		p.errf("COLLECTION_INTERVAL must be > 0, got %s", c.CollectionInterval)
	}
	defaultTimeout := c.CollectionInterval * collectionTimeoutRatio / 100
	c.CollectionTimeout = p.duration("COLLECTION_TIMEOUT", defaultTimeout)
	if c.CollectionTimeout <= 0 {
		p.errf("COLLECTION_TIMEOUT must be > 0, got %s", c.CollectionTimeout)
	}

	c.IncludeInternalTopics = p.boolean("INCLUDE_INTERNAL_TOPICS", false)
	c.TopicIncludeRegex = p.regex("TOPIC_INCLUDE_REGEX")
	c.TopicExcludeRegex = p.regex("TOPIC_EXCLUDE_REGEX")
	c.GroupIncludeRegex = p.regex("GROUP_INCLUDE_REGEX")
	c.GroupExcludeRegex = p.regex("GROUP_EXCLUDE_REGEX")
	// A misspelt state is not a no-op: the broker matches literally, so
	// GROUP_STATES=Stabel lists nothing and the batch reports a cluster with no
	// consumer groups.
	for _, s := range splitList(os.Getenv("GROUP_STATES")) {
		state, ok := canonicalGroupState(s)
		if !ok {
			p.errf("GROUP_STATES entry %q is not one of %s", s, strings.Join(groupStates, ", "))
			continue
		}
		c.GroupStates = append(c.GroupStates, state)
	}

	c.CollectLastStableOffset = p.boolean("COLLECT_LAST_STABLE_OFFSET", DefaultCollectLSO)
	c.CollectConsumerGroups = p.boolean("COLLECT_CONSUMER_GROUPS", DefaultCollectConsumerGroups)
	c.CollectLogDirs = p.boolean("COLLECT_LOG_DIRS", DefaultCollectLogDirs)
	// The value is a modulus: reading 0 as "every cycle" would turn a typo into
	// the heaviest possible cadence, so it is rejected instead.
	c.LogDirsEvery = p.integer("LOG_DIRS_EVERY", DefaultLogDirsEvery)
	if c.LogDirsEvery < 1 {
		p.errf("LOG_DIRS_EVERY must be >= 1 (1 = every cycle), got %d", c.LogDirsEvery)
	}

	c.CollectThroughputWindow = p.boolean("COLLECT_THROUGHPUT_WINDOW", DefaultCollectThroughputWindow)
	// A zero width asks about "now": every partition answers -1, kadm re-lists
	// every one of them as an end offset, and the cycle pays two fan-outs for a
	// window of no width. A negative one asks about the future.
	c.ThroughputWindow = p.duration("THROUGHPUT_WINDOW", DefaultThroughputWindow)
	if c.ThroughputWindow <= 0 {
		p.errf("THROUGHPUT_WINDOW must be > 0, got %s", c.ThroughputWindow)
	}
	c.ThroughputWindowEvery = p.integer("THROUGHPUT_WINDOW_EVERY", DefaultThroughputWindowEvery)
	if c.ThroughputWindowEvery < 1 {
		p.errf("THROUGHPUT_WINDOW_EVERY must be >= 1 (1 = every cycle), got %d", c.ThroughputWindowEvery)
	}

	c.CollectMaxTimestamp = p.boolean("COLLECT_MAX_TIMESTAMP", DefaultCollectMaxTimestamp)
	c.MaxTimestampEvery = p.integer("MAX_TIMESTAMP_EVERY", DefaultMaxTimestampEvery)
	if c.MaxTimestampEvery < 1 {
		p.errf("MAX_TIMESTAMP_EVERY must be >= 1 (1 = every cycle), got %d", c.MaxTimestampEvery)
	}
	c.CollectTieredOffsets = p.boolean("COLLECT_TIERED_OFFSETS", DefaultCollectTieredOffsets)
	c.CollectShareGroups = p.boolean("COLLECT_SHARE_GROUPS", DefaultCollectShareGroups)
	c.CollectConfigs = p.boolean("COLLECT_CONFIGS", DefaultCollectConfigs)
	c.ConfigsEvery = p.integer("CONFIGS_EVERY", DefaultConfigsEvery)
	if c.ConfigsEvery < 1 {
		p.errf("CONFIGS_EVERY must be >= 1 (1 = every cycle), got %d", c.ConfigsEvery)
	}

	// A negative cap has no meaning, unlike EXPORT_FILE_MAX_MB where it disables
	// rotation.
	c.MaxErrors = p.integer("MAX_ERRORS", DefaultMaxErrors)
	if c.MaxErrors < 0 {
		p.errf("MAX_ERRORS must be >= 0 (0 = unlimited), got %d", c.MaxErrors)
	}
	// The one asymmetric bound: zero samples would emit no exemplar, leaving a
	// count with nothing to attach to.
	c.MaxErrorSamples = p.integer("MAX_ERROR_SAMPLES", DefaultMaxErrorSamples)
	if c.MaxErrorSamples < 1 {
		p.errf("MAX_ERROR_SAMPLES must be >= 1, got %d", c.MaxErrorSamples)
	}
	for _, lim := range []struct {
		key string
		dst *int
	}{
		{"MAX_TOPICS", &c.MaxTopics},
		{"MAX_PARTITIONS_PER_TOPIC", &c.MaxPartitionsPerTopic},
		{"MAX_GROUPS", &c.MaxGroups},
		{"MAX_OFFSETS_PER_GROUP", &c.MaxOffsetsPerGroup},
	} {
		*lim.dst = p.integer(lim.key, DefaultMaxEntities)
		if *lim.dst < 0 {
			p.errf("%s must be >= 0 (0 = unlimited), got %d", lim.key, *lim.dst)
		}
	}

	c.LogLevel = strings.ToLower(strings.TrimSpace(p.str("LOG_LEVEL", DefaultLogLevel)))
	if _, err := parseLevel(c.LogLevel); err != nil {
		p.err(err)
	}

	c.AgentInstanceID = strings.TrimSpace(os.Getenv("AGENT_INSTANCE_ID"))
	if c.AgentInstanceID == "" {
		c.AgentInstanceID = defaultInstanceID()
	}

	if err := p.result(); err != nil {
		return nil, err
	}
	return c, nil
}

// SlogLevel maps LOG_LEVEL onto a slog level. Load has already validated it,
// so the fallback here is unreachable in practice.
func (c *Config) SlogLevel() slog.Level {
	lvl, err := parseLevel(c.LogLevel)
	if err != nil {
		return slog.LevelInfo
	}
	return lvl
}

// Warnings lists settings that are legal but likely a mistake. They are logged
// at startup rather than rejected, because the operator may mean them.
func (c *Config) Warnings() []string {
	var w []string
	if c.CollectionTimeout > c.CollectionInterval {
		w = append(w, fmt.Sprintf("COLLECTION_TIMEOUT (%s) exceeds COLLECTION_INTERVAL (%s): slow cycles will delay ticks",
			c.CollectionTimeout, c.CollectionInterval))
	}
	if c.ExportMode != ExportModeHTTP && c.ExportEndpoint != "" {
		w = append(w, fmt.Sprintf("EXPORT_ENDPOINT is set but EXPORT_MODE=%s, so it is ignored", c.ExportMode))
	}
	if c.ExportMode == ExportModeFile && c.ExportFileMaxMB < 0 {
		w = append(w, "EXPORT_FILE_MAX_MB is negative: file rotation is disabled and the file will grow without bound")
	}

	// Every entity cap silently shortens the customer's own inventory, so warn
	// once per cap that is set.
	for _, lim := range []struct {
		key string
		val int
	}{
		{"MAX_TOPICS", c.MaxTopics},
		{"MAX_PARTITIONS_PER_TOPIC", c.MaxPartitionsPerTopic},
		{"MAX_GROUPS", c.MaxGroups},
		{"MAX_OFFSETS_PER_GROUP", c.MaxOffsetsPerGroup},
	} {
		if lim.val != 0 {
			w = append(w, fmt.Sprintf("%s=%d truncates the inventory; prefer TOPIC_INCLUDE_REGEX/GROUP_INCLUDE_REGEX for intentional selection", lim.key, lim.val))
		}
	}
	if c.MaxGroups != 0 {
		// The name says groups, but it is enforced on the listing that both
		// sections share.
		w = append(w, "MAX_GROUPS also truncates the offsets section, not just groups")
	}
	if c.MaxErrors == 0 {
		w = append(w, "MAX_ERRORS=0 leaves errors[] unbounded")
	}
	if c.MaxErrorSamples > 50 {
		w = append(w, fmt.Sprintf("MAX_ERROR_SAMPLES=%d largely defeats error deduplication", c.MaxErrorSamples))
	}

	if len(c.GroupStates) > 0 {
		// Unlike a cap, nothing is counted as truncated: the filtered groups
		// never reach the agent.
		w = append(w, fmt.Sprintf("GROUP_STATES=%s filters the listing broker-side: groups in other states are absent from both groups[] and offsets[], and a group leaving the set looks deleted",
			strings.Join(c.GroupStates, ",")))
		if !slices.Contains(c.GroupStates, "Empty") {
			w = append(w, "GROUP_STATES excludes Empty: a group whose consumers have all died is an Empty group, so that outage will not appear in the batch")
		}
	}

	if !c.CollectLastStableOffset {
		w = append(w, "COLLECT_LAST_STABLE_OFFSET=false: end_offset is the high watermark, so read_committed consumers on transactional topics will report permanent false lag")
	}
	if c.CollectLogDirs && c.LogDirsEvery == 1 {
		w = append(w, fmt.Sprintf("COLLECT_LOG_DIRS=true with LOG_DIRS_EVERY=1 requests an O(replicas) response from every broker on every %s cycle", c.CollectionInterval))
	}

	if c.CollectThroughputWindow && c.ThroughputWindow < c.CollectionInterval {
		// Not an error: the numbers stay true, they just stop being worth an
		// extra fan-out.
		w = append(w, fmt.Sprintf("THROUGHPUT_WINDOW (%s) is narrower than COLLECTION_INTERVAL (%s): a backend can already difference two batches over that span, so the extra ListOffsets fan-out buys nothing",
			c.ThroughputWindow, c.CollectionInterval))
	}
	return w
}

// HasSASL reports whether a complete SASL credential set was supplied. Load
// rejects a partial one, so this is never true for a half-configured agent.
func (c *Config) HasSASL() bool {
	return c.SASLMechanism != "" && c.SASLUsername != "" && c.SASLPassword != ""
}

// ExportTarget is a human-readable description of where batches go, for the
// startup log line.
func (c *Config) ExportTarget() string {
	switch c.ExportMode {
	case ExportModeHTTP:
		return c.ExportEndpoint
	case ExportModeFile:
		return c.ExportFile
	case ExportModeStdout:
		return "stdout"
	default:
		return ""
	}
}

// Redacted renders the config for logging. It never includes API_KEY or
// KAFKA_SASL_PASSWORD; both are reduced to a set/unset marker.
func (c *Config) Redacted() string {
	var b strings.Builder
	fmt.Fprintf(&b, "brokers=%s", strings.Join(c.KafkaBrokers, ","))
	fmt.Fprintf(&b, " tls=%t sasl_mechanism=%s sasl_username=%s sasl_password=%s",
		c.TLSEnabled, orNone(c.SASLMechanism), orNone(c.SASLUsername), secret(c.SASLPassword))
	fmt.Fprintf(&b, " export_mode=%s export_target=%s api_key=%s",
		c.ExportMode, orNone(c.ExportTarget()), secret(c.APIKey))
	if c.ExportMode == ExportModeFile {
		fmt.Fprintf(&b, " export_file_max_mb=%d export_file_max_backups=%d",
			c.ExportFileMaxMB, c.ExportFileMaxBackups)
		fmt.Fprintf(&b, " export_file_fsync=%t", c.ExportFileSync)
	}
	if c.ExportMode == ExportModeHTTP {
		fmt.Fprintf(&b, " export_queue_size=%d export_max_retries=%d export_base_delay=%s export_timeout=%s",
			c.ExportQueueSize, c.ExportMaxRetries, c.ExportBaseDelay, c.ExportTimeout)
	}
	fmt.Fprintf(&b, " collection_interval=%s collection_timeout=%s include_internal_topics=%t",
		c.CollectionInterval, c.CollectionTimeout, c.IncludeInternalTopics)
	fmt.Fprintf(&b, " topic_include=%s topic_exclude=%s group_include=%s group_exclude=%s",
		orNone(c.TopicIncludeRegex), orNone(c.TopicExcludeRegex), orNone(c.GroupIncludeRegex), orNone(c.GroupExcludeRegex))
	fmt.Fprintf(&b, " group_states=%s", orNone(strings.Join(c.GroupStates, ",")))
	// Every collector knob is printed, including the ones that default on. This
	// line is the only record of what an agent was actually running with, and
	// applyCapabilityGates can switch several of these off after startup against
	// an older cluster — the INFO line it logs is only readable against a
	// baseline, which is this.
	fmt.Fprintf(&b, " collect_last_stable_offset=%t collect_consumer_groups=%t collect_log_dirs=%t",
		c.CollectLastStableOffset, c.CollectConsumerGroups, c.CollectLogDirs)
	// Printing the cadence for a phase that never runs invites reading it as
	// "log dirs are being collected". Same rule for every optional phase below.
	if c.CollectLogDirs {
		fmt.Fprintf(&b, " log_dirs_every=%d", c.LogDirsEvery)
	}
	fmt.Fprintf(&b, " collect_throughput_window=%t", c.CollectThroughputWindow)
	if c.CollectThroughputWindow {
		fmt.Fprintf(&b, " throughput_window=%s throughput_window_every=%d", c.ThroughputWindow, c.ThroughputWindowEvery)
	}
	fmt.Fprintf(&b, " collect_max_timestamp=%t", c.CollectMaxTimestamp)
	if c.CollectMaxTimestamp {
		fmt.Fprintf(&b, " max_timestamp_every=%d", c.MaxTimestampEvery)
	}
	fmt.Fprintf(&b, " collect_tiered_offsets=%t", c.CollectTieredOffsets)
	fmt.Fprintf(&b, " collect_share_groups=%t collect_configs=%t", c.CollectShareGroups, c.CollectConfigs)
	if c.CollectConfigs {
		fmt.Fprintf(&b, " configs_every=%d", c.ConfigsEvery)
	}
	fmt.Fprintf(&b, " max_errors=%d max_error_samples=%d", c.MaxErrors, c.MaxErrorSamples)
	// Only the caps that are set: five "=0" pairs on every startup line would
	// bury the settings that matter.
	for _, lim := range []struct {
		key string
		val int
	}{
		{"max_topics", c.MaxTopics},
		{"max_partitions_per_topic", c.MaxPartitionsPerTopic},
		{"max_groups", c.MaxGroups},
		{"max_offsets_per_group", c.MaxOffsetsPerGroup},
	} {
		if lim.val != 0 {
			fmt.Fprintf(&b, " %s=%d", lim.key, lim.val)
		}
	}
	fmt.Fprintf(&b, " log_level=%s agent_instance_id=%s", c.LogLevel, c.AgentInstanceID)
	return b.String()
}

// String is Redacted, so that an accidental %v or %s of the config cannot leak
// a credential.
func (c *Config) String() string { return c.Redacted() }

func secret(v string) string {
	if v == "" {
		return "unset"
	}
	return "***"
}

func orNone(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func parseLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("LOG_LEVEL %q is not one of debug, info, warn, error", s)
	}
}

func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// defaultInstanceID is hostname + a short random suffix, so two agents on the
// same host (or two pods from one image) do not collide in the backend.
func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "agent"
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", host, time.Now().UnixNano()&0xffffffff)
	}
	return host + "-" + hex.EncodeToString(b[:])
}

// parser accumulates parse and validation failures so Load can report them all
// at once.
type parser struct {
	errs []error
}

func (p *parser) err(err error)                        { p.errs = append(p.errs, err) }
func (p *parser) errf(format string, a ...interface{}) { p.err(fmt.Errorf(format, a...)) }

func (p *parser) result() error {
	if len(p.errs) == 0 {
		return nil
	}
	return errors.Join(p.errs...)
}

func (p *parser) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.errf("%s %q is not a valid duration (e.g. 30s, 2m): %v", key, v, err)
		return def
	}
	return d
}

func (p *parser) integer(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.errf("%s %q is not a valid integer", key, v)
		return def
	}
	return n
}

func (p *parser) boolean(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.errf("%s %q is not a valid boolean (true/false)", key, v)
		return def
	}
	return b
}

// regex validates the pattern at startup and discards the compiled value; the
// collector owns compilation. A bad pattern must not wait for the first cycle
// to be noticed.
func (p *parser) regex(key string) string {
	v := os.Getenv(key)
	if v == "" {
		return ""
	}
	if _, err := regexp.Compile(v); err != nil {
		p.errf("%s is not a valid regular expression: %v", key, err)
	}
	return v
}
