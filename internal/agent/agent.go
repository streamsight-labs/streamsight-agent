// Package agent owns the collection loop: it wires a Kafka client, a
// collector and an exporter together, stamps the batch envelope the collector
// cannot know about, and shuts all three down cleanly.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-metrics-agent/internal/collector"
	"kafka-metrics-agent/internal/config"
	"kafka-metrics-agent/internal/export"
	"kafka-metrics-agent/internal/kafka"
	"kafka-metrics-agent/internal/metrics"
)

// batchCollector is the agent's view of the collector. It exists so the loop
// can be tested without a broker.
type batchCollector interface {
	Collect(ctx context.Context) *metrics.Batch
}

type Agent struct {
	cfg     *config.Config
	logger  *slog.Logger
	version string

	client    *kafka.Client
	collector batchCollector
	exporter  export.Exporter

	startedAt time.Time
	// batchSeq is monotonic from 1 per boot. It is only ever touched from the
	// single collection goroutine.
	batchSeq uint64
}

// New connects to Kafka and builds the pipeline. Every failure path after the
// client is created must close it -- a returned error means the caller has no
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

	coll, err := collector.New(client, collector.Options{
		Timeout:               cfg.CollectionTimeout,
		IncludeInternalTopics: cfg.IncludeInternalTopics,
		TopicIncludeRegex:     cfg.TopicIncludeRegex,
		TopicExcludeRegex:     cfg.TopicExcludeRegex,
		GroupIncludeRegex:     cfg.GroupIncludeRegex,
		GroupExcludeRegex:     cfg.GroupExcludeRegex,
		Logger:                logger,
	})
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
// COLLECTION_TIMEOUT so a hung broker cannot eat the tick interval; export
// deliberately uses the parent context instead, because a cycle that used its
// whole budget still has a batch worth shipping.
func (a *Agent) runCycle(parent context.Context) {
	collectCtx, cancel := context.WithTimeout(parent, a.cfg.CollectionTimeout)
	batch := a.collector.Collect(collectCtx)
	cancel()

	if batch == nil {
		a.logger.Error("collector returned no batch")
		return
	}

	a.batchSeq++
	a.stamp(batch)

	if err := a.exporter.Export(parent, batch); err != nil {
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

// Close flushes the exporter before dropping the Kafka connection. Both are
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
