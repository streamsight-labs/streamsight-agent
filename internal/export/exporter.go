// Package export ships collected metric batches somewhere durable: a local
// JSONL file (the default, works with no network and no API key), the global
// HTTP ingest endpoint, or stdout for piping into another process.
package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"kafka-metrics-agent/internal/metrics"
)

// Mode selects an exporter implementation. It is an alias for string so a
// plain config field (cfg.ExportMode) can be assigned without a conversion.
type Mode = string

const (
	ModeFile   Mode = "file"
	ModeHTTP   Mode = "http"
	ModeStdout Mode = "stdout"
)

var (
	// ErrQueueFull means the batch was dropped because the export queue was
	// saturated. Collection must never block on export, so this is a drop, not
	// backpressure.
	ErrQueueFull = errors.New("export queue full")
	// ErrClosed means Export was called after Close.
	ErrClosed = errors.New("exporter is closed")
)

// Stats is the exporter's self-telemetry. The agent copies it into
// metrics.AgentStats on every collection cycle so the backend can tell
// "nothing to report" apart from "the pipe is broken".
type Stats struct {
	// BatchesExported is batches the destination accepted.
	BatchesExported uint64
	// BatchesDropped is batches lost: queue overflow, write failure, or
	// retries exhausted.
	BatchesDropped uint64
	// BatchesRejected is batches the destination refused terminally (a
	// non-retryable 4xx). These are a configuration or schema problem, not a
	// transient one.
	BatchesRejected uint64
	// ExportRetries counts retry attempts, not retried batches.
	ExportRetries uint64
	// QueueDepth is the number of batches waiting to be sent (always 0 for
	// the synchronous file and stdout exporters).
	QueueDepth int
	// LastError is the most recent export failure, empty if there has never
	// been one. It is never cleared by a subsequent success.
	LastError string
}

// Exporter is the agent's only view of the export pipeline. All three
// implementations satisfy it, so the agent can be tested against a fake.
type Exporter interface {
	// Export hands a batch to the exporter. It must not block on network or
	// disk latency for longer than the collection interval; implementations
	// either write synchronously (file, stdout) or enqueue (http).
	Export(ctx context.Context, batch *metrics.Batch) error
	// Stats returns a snapshot of the counters. Safe for concurrent use.
	Stats() Stats
	// Close flushes anything buffered and releases resources.
	Close() error
}

// Config is the union of every exporter's settings. Only the fields relevant
// to Mode are read; New validates the ones that mode requires.
type Config struct {
	Mode Mode

	// HTTP.
	Endpoint   string
	APIKey     string
	QueueSize  int
	MaxRetries int
	BaseDelay  time.Duration
	Timeout    time.Duration
	Gzip       bool

	// File.
	Path       string
	MaxMB      int
	MaxBackups int
}

// New builds the exporter named by cfg.Mode.
func New(cfg Config) (Exporter, error) {
	switch cfg.Mode {
	case ModeHTTP:
		if cfg.Endpoint == "" {
			return nil, errors.New(`export mode "http" requires EXPORT_ENDPOINT`)
		}
		if cfg.APIKey == "" {
			return nil, errors.New(`export mode "http" requires API_KEY`)
		}
		return NewHTTPExporter(HTTPExporterConfig{
			Endpoint:   cfg.Endpoint,
			APIKey:     cfg.APIKey,
			QueueSize:  cfg.QueueSize,
			MaxRetries: cfg.MaxRetries,
			BaseDelay:  cfg.BaseDelay,
			Timeout:    cfg.Timeout,
			Gzip:       cfg.Gzip,
		}), nil

	case ModeFile:
		if cfg.Path == "" {
			return nil, errors.New(`export mode "file" requires EXPORT_FILE`)
		}
		return NewFileExporter(FileExporterConfig{
			Path:       cfg.Path,
			MaxMB:      cfg.MaxMB,
			MaxBackups: cfg.MaxBackups,
		})

	case ModeStdout:
		return NewStdoutExporter(), nil

	case "":
		return nil, errors.New(`export mode is empty (want "file", "http" or "stdout")`)

	default:
		return nil, fmt.Errorf("unknown export mode %q (want \"file\", \"http\" or \"stdout\")", cfg.Mode)
	}
}

// encodeLine renders one batch as a single JSONL record. json.Marshal escapes
// newlines inside strings, so the result is guaranteed to be exactly one line.
func encodeLine(batch *metrics.Batch) ([]byte, error) {
	if batch == nil {
		return nil, errors.New("nil batch")
	}
	data, err := json.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("marshal batch: %w", err)
	}
	return append(data, '\n'), nil
}

// counters is the atomic state behind Stats, embedded by every exporter.
type counters struct {
	exported atomic.Uint64
	dropped  atomic.Uint64
	rejected atomic.Uint64
	retries  atomic.Uint64
	lastErr  atomic.Pointer[string]
}

func (c *counters) recordError(err error) {
	if err == nil {
		return
	}
	s := err.Error()
	c.lastErr.Store(&s)
}

func (c *counters) snapshot() Stats {
	s := Stats{
		BatchesExported: c.exported.Load(),
		BatchesDropped:  c.dropped.Load(),
		BatchesRejected: c.rejected.Load(),
		ExportRetries:   c.retries.Load(),
	}
	if p := c.lastErr.Load(); p != nil {
		s.LastError = *p
	}
	return s
}
