package export

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"kafka-metrics-agent/internal/metrics"
)

// StdoutExporter writes the same JSONL framing as FileExporter to stdout, for
// piping into jq or another process. Nothing else may be written to stdout:
// slog goes to stderr, so the two never interleave and stdout stays
// machine-parseable.
type StdoutExporter struct {
	counters

	mu sync.Mutex
	w  io.Writer
}

func NewStdoutExporter() *StdoutExporter {
	return newStreamExporter(os.Stdout)
}

// newStreamExporter is the injectable form used by tests.
func newStreamExporter(w io.Writer) *StdoutExporter {
	return &StdoutExporter{w: w}
}

func (e *StdoutExporter) Export(ctx context.Context, batch *metrics.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	line, err := encodeLine(batch)
	if err != nil {
		e.dropped.Add(1)
		e.recordError(err)
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// One Write per batch keeps each record atomic against concurrent callers.
	if _, err := e.w.Write(line); err != nil {
		e.dropped.Add(1)
		err = fmt.Errorf("write stdout: %w", err)
		e.recordError(err)
		return err
	}

	e.exported.Add(1)
	return nil
}

func (e *StdoutExporter) Stats() Stats {
	return e.snapshot()
}

// Close is a no-op: os.Stdout is unbuffered here and is not ours to close.
func (e *StdoutExporter) Close() error {
	return nil
}
