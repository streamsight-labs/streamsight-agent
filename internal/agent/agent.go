// Package agent owns the collection loop: it wires a Kafka client, a collector
// and an exporter together, stamps the batch envelope the collector cannot
// know about, and shuts all three down cleanly.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"kafka-metrics-agent/internal/collector"
	"kafka-metrics-agent/internal/config"
	"kafka-metrics-agent/internal/export"
	"kafka-metrics-agent/internal/kafka"
	"kafka-metrics-agent/internal/metrics"
)

// batchCollector is the agent's view of the collector, so the loop can be
// tested without a broker.
type batchCollector interface {
	Collect(ctx context.Context) *metrics.Batch
}

// Agent is the collection loop: one Kafka client, one collector and one
// exporter, plus the envelope fields the collector deliberately leaves zero.
type Agent struct {
	cfg     *config.Config
	logger  *slog.Logger
	version string

	client    *kafka.Client
	collector batchCollector
	exporter  export.Exporter

	// caps is the startup fingerprint, stamped onto every batch. It is decided
	// once because Probe caches it for the client's lifetime, so re-deriving it
	// per cycle would ship the same bytes for another map allocation.
	caps *metrics.ClusterCapabilities

	startedAt time.Time
	// batchSeq is monotonic from 1 per boot, and only ever touched from the
	// single collection goroutine.
	batchSeq uint64
}

// New connects to Kafka and builds the pipeline. Every failure path after the
// client is created must close it -- a returned error leaves the caller no
// handle to close it with.
func New(cfg *config.Config, version string) (*Agent, error) {
	logger := setupLogger(cfg.SlogLevel())
	logger.Info("starting streamsight-agent", "version", version, "instance_id", cfg.AgentInstanceID)
	logger.Debug("configuration", "config", cfg.Redacted())
	for _, w := range cfg.Warnings() {
		logger.Warn(w)
	}

	client, err := kafka.NewClient(cfg, version)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.CollectionTimeout)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect to kafka: %w", err)
	}
	logger.Info("connected to kafka", "brokers", cfg.KafkaBrokers)

	// A broker below Kafka 2.6 ignores the filter without erroring, which does
	// not degrade the setting but inverts it: the operator asked for a few
	// hundred groups and gets every one of them, on the cluster least able to
	// serve that. Hence a startup failure rather than warn-and-continue. The
	// probe runs once, here, on the ping's context.
	if len(cfg.GroupStates) > 0 {
		if err := client.CheckGroupStateFilter(pingCtx); err != nil {
			client.Close()
			return nil, fmt.Errorf("GROUP_STATES=%s cannot be honoured: %w", strings.Join(cfg.GroupStates, ","), err)
		}
		logger.Info("group state filter active", "states", cfg.GroupStates)
	}

	// The same probe answers every remaining startup question; it is cached for
	// the client's lifetime, so the gates below and the fingerprint on the wire
	// cost one ApiVersions round trip per broker in total.
	caps, err := client.Probe(pingCtx)
	if err != nil {
		// A failed probe says nothing about the cluster's capabilities, so every
		// phase stays as configured: a spurious disable would hide data a
		// perfectly capable cluster can serve. The phases that are silently wrong
		// on an old broker are the reason this is logged loudly.
		logger.Warn("could not probe broker api versions, leaving every optional phase as configured", "error", err)
		caps = nil
	}

	opts := collectorOptions(cfg, logger)
	applyCapabilityGates(&opts, caps, logger)

	coll, err := collector.New(client, opts)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("create collector: %w", err)
	}

	exporter, err := export.New(export.Config{
		Mode:       cfg.ExportMode,
		Endpoint:   cfg.ExportEndpoint,
		APIKey:     cfg.APIKey,
		QueueSize:  cfg.ExportQueueSize,
		MaxRetries: cfg.ExportMaxRetries,
		BaseDelay:  cfg.ExportBaseDelay,
		Timeout:    cfg.ExportTimeout,
		Gzip:       cfg.ExportGzip,
		Path:       cfg.ExportFile,
		MaxMB:      cfg.ExportFileMaxMB,
		MaxBackups: cfg.ExportFileMaxBackups,
		Sync:       cfg.ExportFileSync,
	})
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("create exporter: %w", err)
	}
	logger.Info("export configured", "mode", cfg.ExportMode, "target", cfg.ExportTarget())

	return &Agent{
		cfg:       cfg,
		logger:    logger,
		version:   version,
		client:    client,
		collector: coll,
		exporter:  exporter,
		caps:      caps.Wire(),
		startedAt: time.Now(),
	}, nil
}

// applyCapabilityGates switches off the phases this cluster cannot serve.
//
// Every one of them degrades rather than refusing to start, unlike GROUP_STATES:
// each is on by default or opt-in, so failing closed would refuse to start
// against an older cluster the agent can still describe perfectly well. The
// point of disabling rather than letting the request fail is that a skipped
// section says "your broker is too old" once, where a failing one says "your
// cluster is broken" every cycle forever — and for the two silent degradations
// (the isolation level and the by-timestamp lookup) an enabled phase would ship
// numbers that are wrong with nothing on the wire to say so.
func applyCapabilityGates(opts *collector.Options, caps *kafka.Capabilities, logger *slog.Logger) {
	if caps == nil {
		return
	}
	for _, gate := range []struct {
		on        *bool
		supported bool
		message   string
	}{
		{&opts.CollectConsumerGroups, caps.SupportsConsumerGroupDescribe(),
			"ConsumerGroupDescribe unsupported by at least one broker, disabling it (KIP-848 needs Kafka 4.0+); classic group describe is unaffected"},
		{&opts.CollectLastStableOffset, caps.SupportsListOffsetsIsolation(),
			"ListOffsets carries no isolation level below v2 (Kafka 0.11+), disabling the last stable offset: the broker would answer with the high watermark and nothing on the wire would say so"},
		{&opts.CollectThroughputWindow, caps.SupportsListOffsetsAfterMilli(),
			"ListOffsets answers by timestamp only from v1 (Kafka 0.10.1+), disabling the throughput window: below it the broker returns old-style offsets and no timestamp, with no error"},
		{&opts.CollectReassignments, caps.SupportsListPartitionReassignments(),
			"cluster does not serve ListPartitionReassignments (Kafka 2.4+), disabling it: a URP cannot be attributed to a planned move here"},
		{&opts.CollectMaxTimestamp, caps.SupportsMaxTimestampOffsets(),
			"ListOffsets serves the max-timestamp sentinel only from v7 (Kafka 3.0+), disabling it: below that the broker reads -3 as a real millisecond and answers with an arbitrary offset and no error"},
		{&opts.CollectTieredOffsets, caps.SupportsLocalLogStartOffsets(),
			"ListOffsets serves the local-log-start sentinel only from v8 (Kafka 3.4+), disabling the tiered-storage phase"},
		{&opts.CollectLatestTiered, caps.SupportsLatestTieredOffsets(),
			"ListOffsets serves the latest-tiered sentinel only from v9 (Kafka 3.9+), disabling the remote end offset; the local log start is unaffected"},
		{&opts.CollectShareGroups, caps.SupportsShareGroupDescribe(),
			"cluster does not serve ShareGroupDescribe (KIP-932, Kafka 4.0+), disabling the share_groups section"},
		{&opts.CollectConfigs, caps.SupportsDescribeConfigs(),
			"DescribeConfigs carries no config source below v1 (Kafka 1.1+), disabling the config sections rather than shipping every key as an indistinguishable default"},
		{&opts.CollectEpochProbes, caps.SupportsOffsetForLeaderEpoch(),
			"cluster does not serve OffsetForLeaderEpoch (Kafka 0.11+), disabling truncation proof"},
	} {
		if *gate.on && !gate.supported {
			*gate.on = false
			logger.Info(gate.message)
		}
	}
}

// collectorOptions translates the validated config into collector options. It
// is a named function rather than a literal inside New so a test can prove
// every setting actually reaches the collector: GroupStates was once plumbed
// into ListGroups but missing from this mapping, which made GROUP_STATES
// silently dead — a gap no collector test could have caught.
func collectorOptions(cfg *config.Config, logger *slog.Logger) collector.Options {
	return collector.Options{
		Timeout:               cfg.CollectionTimeout,
		IncludeInternalTopics: cfg.IncludeInternalTopics,
		TopicIncludeRegex:     cfg.TopicIncludeRegex,
		TopicExcludeRegex:     cfg.TopicExcludeRegex,
		GroupIncludeRegex:     cfg.GroupIncludeRegex,
		GroupExcludeRegex:     cfg.GroupExcludeRegex,
		GroupStates:           cfg.GroupStates,

		CollectLastStableOffset: cfg.CollectLastStableOffset,
		CollectConsumerGroups:   cfg.CollectConsumerGroups,
		CollectLogDirs:          cfg.CollectLogDirs,
		LogDirsEvery:            cfg.LogDirsEvery,

		CollectThroughputWindow: cfg.CollectThroughputWindow,
		ThroughputWindowWidth:   cfg.ThroughputWindow,
		ThroughputWindowEvery:   cfg.ThroughputWindowEvery,
		CollectConfigs:          cfg.CollectConfigs,
		ConfigsEvery:            cfg.ConfigsEvery,
		CollectMaxTimestamp:     cfg.CollectMaxTimestamp,
		MaxTimestampEvery:       cfg.MaxTimestampEvery,
		CollectTieredOffsets:    cfg.CollectTieredOffsets,
		CollectLatestTiered:     cfg.CollectLatestTiered,
		CollectShareGroups:      cfg.CollectShareGroups,
		CollectReassignments:    cfg.CollectReassignments,
		CollectEpochProbes:      cfg.CollectEpochProbes,
		CollectRPCStats:         cfg.CollectRPCStats,

		Limits: collector.Limits{
			MaxErrors:             cfg.MaxErrors,
			MaxErrorSamples:       cfg.MaxErrorSamples,
			MaxTopics:             cfg.MaxTopics,
			MaxPartitionsPerTopic: cfg.MaxPartitionsPerTopic,
			MaxGroups:             cfg.MaxGroups,
			MaxMembersPerGroup:    cfg.MaxMembersPerGroup,
			MaxOffsetsPerGroup:    cfg.MaxOffsetsPerGroup,
		},
		Logger: logger,
	}
}

// Run collects on a fixed interval until SIGINT/SIGTERM. The first cycle runs
// immediately so a misconfiguration surfaces at once rather than one interval
// later.
func (a *Agent) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(a.cfg.CollectionInterval)
	defer ticker.Stop()

	a.runCycle(ctx)

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("shutting down", "batches_collected", a.batchSeq)
			return nil
		case <-ticker.C:
			a.runCycle(ctx)
		}
	}
}

// runCycle is one collect-and-export. Collection is bounded by
// COLLECTION_TIMEOUT so a hung broker cannot eat the tick interval; export is
// detached from the signal context, because a cycle that used its whole budget
// still has a batch worth shipping even once shutdown has begun.
func (a *Agent) runCycle(parent context.Context) {
	collectCtx, cancel := context.WithTimeout(parent, a.cfg.CollectionTimeout)
	batch := a.collector.Collect(collectCtx)
	cancel()

	if batch == nil {
		a.logger.Error("collector returned no batch")
		return
	}

	// A cycle cut short by SIGTERM produces a batch whose every section failed
	// with a transport error — a description of the agent stopping, not of the
	// cluster breaking. Discard it and consume no sequence number: a hole in
	// batch_seq means lost data at the backend, so shipping one on every
	// rollout would make routine restarts look like an outage.
	if parent.Err() != nil {
		a.logger.Debug("cycle interrupted by shutdown, batch discarded")
		return
	}

	a.batchSeq++
	a.stamp(batch)

	// Bounded so a wedged ingest cannot hold shutdown past the container's
	// termination grace period. Config.Load guarantees a positive timeout; the
	// guard keeps a hand-constructed zero from meaning "already expired".
	exportCtx := context.WithoutCancel(parent)
	if a.cfg.ExportTimeout > 0 {
		var exportCancel context.CancelFunc
		exportCtx, exportCancel = context.WithTimeout(exportCtx, a.cfg.ExportTimeout)
		defer exportCancel()
	}

	if err := a.exporter.Export(exportCtx, batch); err != nil {
		a.logger.Error("export failed", "error", err, "batch_seq", batch.BatchSeq)
		return
	}
	a.logger.Debug("batch exported",
		"batch_seq", batch.BatchSeq,
		"topics", len(batch.Topics),
		"groups", len(batch.Groups),
		"collection_ms", batch.CollectionMs,
		"errors", len(batch.Errors),
	)
}

// stamp fills the envelope the collector deliberately leaves zero: identity,
// sequence and the agent's self-telemetry.
func (a *Agent) stamp(batch *metrics.Batch) {
	batch.SchemaVersion = metrics.SchemaVersion
	batch.AgentVersion = a.version
	batch.AgentInstanceID = a.cfg.AgentInstanceID
	batch.BatchSeq = a.batchSeq
	// The "Y" in "grant DESCRIBE_CONFIGS on topic X to principal Y". Empty for
	// mTLS and anonymous connections, where the broker derives the principal from
	// the certificate and the agent cannot know it.
	batch.Principal = a.cfg.SASLUsername
	// Constant for the client's lifetime; the collector leaves it alone because
	// the probe is a startup fact, not a per-cycle sample. Skipped when the
	// cluster phase failed: Cluster is nil then, and a batch that could not
	// describe the cluster must not describe its capabilities either.
	if batch.Cluster != nil {
		batch.Cluster.Capabilities = a.caps
	}

	// The collector owns the RPC window: it drains the hooks and emits the
	// broker_rpc section from the same toggle, so the wholesale assignment below
	// must not drop what it wrote.
	rpc := batch.Agent.RPC
	stats := a.exporter.Stats()
	batch.Agent = metrics.AgentStats{
		UptimeSec:        int64(time.Since(a.startedAt).Seconds()),
		BatchesCollected: a.batchSeq,
		BatchesExported:  stats.BatchesExported,
		BatchesDropped:   stats.BatchesDropped,
		BatchesRejected:  stats.BatchesRejected,
		ExportRetries:    stats.ExportRetries,
		QueueDepth:       stats.QueueDepth,
		LastExportError:  stats.LastError,
		RPC:              rpc,
	}
}

// Close flushes the exporter before dropping the Kafka connection. It is
// nil-safe so a partially built agent can still be closed.
func (a *Agent) Close() error {
	var err error
	if a.exporter != nil {
		if cerr := a.exporter.Close(); cerr != nil {
			err = fmt.Errorf("close exporter: %w", cerr)
		}
	}
	if a.client != nil {
		a.client.Close()
	}
	return err
}

func setupLogger(level slog.Level) *slog.Logger {
	// stderr, always: stdout is a valid export destination and must stay
	// machine-parseable JSONL.
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
