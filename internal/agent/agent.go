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

	// Same probe, opposite policy: degrade instead of refusing to start. A
	// cluster below Kafka 4.0 has no new-protocol groups to describe, but the
	// phase must be switched off explicitly or it is a rejected request every
	// cycle for the life of that cluster.
	opts := collectorOptions(cfg, logger)
	if opts.CollectConsumerGroups {
		supported, err := client.SupportsConsumerGroupDescribe(pingCtx)
		switch {
		case err != nil:
			// A failed probe says nothing about the cluster's capability, so
			// leave the phase on: a spurious disable would hide new-protocol
			// groups on a cluster that can serve them.
			logger.Warn("could not probe ConsumerGroupDescribe support, leaving consumer group describe enabled", "error", err)
		case !supported:
			opts.CollectConsumerGroups = false
			logger.Info("ConsumerGroupDescribe unsupported by at least one broker, disabling it (KIP-848 needs Kafka 4.0+); classic group describe is unaffected")
		default:
			logger.Debug("ConsumerGroupDescribe supported")
		}
	}

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
		startedAt: time.Now(),
	}, nil
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
