package export

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"kafka-metrics-agent/internal/metrics"
)

const (
	defaultFileMaxMB      = 100
	defaultFileMaxBackups = 3
)

// FileExporter appends one JSON-encoded Batch per line to a local file. It is
// the default exporter: local mode needs no network, no API key and no
// credentials of any kind.
type FileExporter struct {
	counters

	path       string
	maxBytes   int64
	maxBackups int
	sync       bool

	mu   sync.Mutex
	file *os.File
	w    *bufio.Writer
	size int64
	// needsReopen means a rotation completed its destructive backup shift but
	// could not reopen the live file; only the reopen is retried.
	needsReopen bool
	closed      bool
}

// FileExporterConfig configures the JSONL file exporter.
type FileExporterConfig struct {
	// Path is the JSONL file. Parent directories are created as needed.
	Path string
	// MaxMB is the size at which the file is rotated. Zero uses the default;
	// negative disables rotation.
	MaxMB int
	// MaxBackups is how many rotated files to keep (<name>.1 … <name>.N).
	// Negative uses the default; zero keeps none.
	MaxBackups int
	// Sync fsyncs after every batch. Off by default: a flush already survives
	// process death, and only power loss or a kernel panic needs more.
	Sync bool
}

// NewFileExporter opens the target file, creating parent directories as
// needed. It fails at startup rather than on the first batch, so a bad path is
// a refusal to start instead of a silent hole in the data.
func NewFileExporter(cfg FileExporterConfig) (*FileExporter, error) {
	if cfg.Path == "" {
		return nil, errors.New("file exporter: path is required")
	}
	if cfg.MaxMB == 0 {
		cfg.MaxMB = defaultFileMaxMB
	}
	if cfg.MaxBackups < 0 {
		cfg.MaxBackups = defaultFileMaxBackups
	}

	e := &FileExporter{
		path:       cfg.Path,
		maxBackups: cfg.MaxBackups,
		sync:       cfg.Sync,
	}
	if cfg.MaxMB > 0 {
		e.maxBytes = int64(cfg.MaxMB) * 1024 * 1024
	}

	if err := e.open(); err != nil {
		return nil, err
	}
	return e, nil
}

// Export writes the batch and flushes the buffer, so `tail -f` is live and a
// SIGKILL loses at most the batch being written.
func (e *FileExporter) Export(ctx context.Context, batch *metrics.Batch) error {
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

	if e.closed {
		e.dropped.Add(1)
		e.recordError(ErrClosed)
		return ErrClosed
	}

	// Finish a rotation that shifted the backups but could not reopen. Replaying
	// the shift would consume one more backup generation every cycle and destroy
	// all of them within maxBackups cycles, without writing a single batch.
	if e.needsReopen {
		if err := e.open(); err != nil {
			e.dropped.Add(1)
			e.recordError(err)
			return err
		}
		e.needsReopen = false
	}

	// Rotate before writing so a batch is never split across two files. Never
	// rotate an empty file: a single batch larger than maxBytes would other-
	// wise rotate forever and still not fit.
	if e.maxBytes > 0 && e.size > 0 && e.size+int64(len(line)) > e.maxBytes {
		if err := e.rotate(); err != nil {
			e.dropped.Add(1)
			e.recordError(err)
			return err
		}
	}

	n, err := e.w.Write(line)
	e.size += int64(n)
	if err != nil {
		e.dropped.Add(1)
		err = fmt.Errorf("write %s: %w", e.path, err)
		e.recordError(err)
		return err
	}
	if err := e.w.Flush(); err != nil {
		e.dropped.Add(1)
		err = fmt.Errorf("flush %s: %w", e.path, err)
		e.recordError(err)
		return err
	}
	// A flush reaches the page cache, which survives the process dying but not
	// the machine dying. fsync is opt-in because it costs a device round trip
	// under the lock, and collection is synchronous: on a network volume a
	// stalled fsync stalls collection itself.
	if e.sync && e.file != nil {
		if err := e.file.Sync(); err != nil {
			e.dropped.Add(1)
			err = fmt.Errorf("fsync %s: %w", e.path, err)
			e.recordError(err)
			return err
		}
	}

	e.exported.Add(1)
	return nil
}

// Stats returns a snapshot of the exporter's counters.
func (e *FileExporter) Stats() Stats {
	return e.snapshot()
}

// Close flushes and closes the file. It is safe to call twice.
func (e *FileExporter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	var firstErr error
	if e.w != nil {
		if err := e.w.Flush(); err != nil {
			firstErr = fmt.Errorf("flush %s: %w", e.path, err)
		}
	}
	if e.file != nil {
		if err := e.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", e.path, err)
		}
	}
	e.w = nil
	e.file = nil
	e.recordError(firstErr)
	return firstErr
}

// open creates the parent directory and opens the file for append. Caller
// holds the lock (or has not published the exporter yet).
func (e *FileExporter) open() error {
	if dir := filepath.Dir(e.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create dir %s: %w", dir, err)
		}
	}

	f, err := os.OpenFile(e.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", e.path, err)
	}

	var size int64
	if fi, err := f.Stat(); err == nil {
		size = fi.Size()
	}

	e.file = f
	e.size = size
	if e.w == nil {
		e.w = bufio.NewWriter(f)
	} else {
		e.w.Reset(f)
	}
	return nil
}

// rotate shifts <name>.N-1 to <name>.N, moves the live file to <name>.1 and
// reopens. Caller holds the lock.
func (e *FileExporter) rotate() error {
	if e.w != nil {
		if err := e.w.Flush(); err != nil {
			return fmt.Errorf("flush %s before rotate: %w", e.path, err)
		}
	}
	if e.file != nil {
		if err := e.file.Close(); err != nil {
			return fmt.Errorf("close %s before rotate: %w", e.path, err)
		}
		e.file = nil
	}

	if e.maxBackups <= 0 {
		if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", e.path, err)
		}
		return e.reopen()
	}

	oldest := backupPath(e.path, e.maxBackups)
	if err := os.Remove(oldest); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", oldest, err)
	}
	for i := e.maxBackups - 1; i >= 1; i-- {
		from := backupPath(e.path, i)
		if _, err := os.Stat(from); err != nil {
			continue
		}
		if err := os.Rename(from, backupPath(e.path, i+1)); err != nil {
			return fmt.Errorf("rotate %s: %w", from, err)
		}
	}
	if err := os.Rename(e.path, backupPath(e.path, 1)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rotate %s: %w", e.path, err)
	}

	return e.reopen()
}

// reopen opens the live file after the backup shift has already happened. It
// marks needsReopen first so a failure here (a full or read-only volume) is
// retried as a bare reopen next cycle rather than replaying the shift.
func (e *FileExporter) reopen() error {
	e.needsReopen = true
	if err := e.open(); err != nil {
		return err
	}
	e.needsReopen = false
	return nil
}

func backupPath(path string, n int) string {
	return fmt.Sprintf("%s.%d", path, n)
}
