package export

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kafka-metrics-agent/internal/metrics"
)

func readLines(t *testing.T, path string) []string {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return lines
}

func TestFileExporter_JSONLRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")

	e, err := NewFileExporter(FileExporterConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}

	start := int64(10)
	end := int64(99)
	committed := int64(42)
	want := &metrics.Batch{
		SchemaVersion:   metrics.SchemaVersion,
		AgentVersion:    "1.2.3",
		AgentInstanceID: "host-a1b2",
		BatchSeq:        1,
		CollectedAt:     time.Now().UTC().Truncate(time.Millisecond),
		CollectionMs:    17,
		Topics: []metrics.TopicMetrics{{
			Name: "orders",
			// A newline inside a string must not break JSONL framing.
			ID:             "id\nwith\nnewlines",
			PartitionCount: 1,
			Partitions: []metrics.Partition{{
				ID:          0,
				StartOffset: &start,
				EndOffset:   &end,
			}, {
				ID:          1,
				StartOffset: nil, // lookup failed: must stay null, never 0
				EndOffset:   nil,
			}},
		}},
		Offsets: []metrics.ConsumerOffset{{
			GroupID: "g1",
			Offsets: []metrics.PartitionOffset{
				{Topic: "orders", Partition: 0, Offset: &committed},
				{Topic: "orders", Partition: 1, Offset: nil}, // never committed
			},
		}},
		Errors: []metrics.CollectionError{{Section: "offsets", Kind: "authorization", Message: "denied"}},
	}

	if err := e.Export(context.Background(), want); err != nil {
		t.Fatalf("Export: %v", err)
	}
	// Flushed per batch, so the line is readable before Close.
	if lines := readLines(t, path); len(lines) != 1 {
		t.Fatalf("expected 1 line before Close, got %d", len(lines))
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	lines := readLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}

	var got metrics.Batch
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("unmarshal line: %v", err)
	}
	if got.AgentInstanceID != want.AgentInstanceID || got.BatchSeq != want.BatchSeq {
		t.Errorf("identity mismatch: %+v", got)
	}
	p := got.Topics[0].Partitions
	if p[0].StartOffset == nil || *p[0].StartOffset != 10 || *p[0].EndOffset != 99 {
		t.Errorf("offsets mangled: %+v", p[0])
	}
	if p[1].StartOffset != nil || p[1].EndOffset != nil {
		t.Errorf("unknown offsets must round-trip as null, got %+v", p[1])
	}
	if got.Offsets[0].Offsets[1].Offset != nil {
		t.Error("never-committed offset must round-trip as null")
	}
	if got.Topics[0].ID != "id\nwith\nnewlines" {
		t.Errorf("embedded newlines mangled: %q", got.Topics[0].ID)
	}

	if s := e.Stats(); s.BatchesExported != 1 || s.BatchesDropped != 0 {
		t.Errorf("unexpected stats: %+v", s)
	}
}

func TestFileExporter_AppendsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "dir", "metrics.jsonl")

	for i := 0; i < 3; i++ {
		e, err := NewFileExporter(FileExporterConfig{Path: path})
		if err != nil {
			t.Fatalf("NewFileExporter: %v", err)
		}
		if err := e.Export(context.Background(), &metrics.Batch{BatchSeq: uint64(i)}); err != nil {
			t.Fatalf("Export: %v", err)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	lines := readLines(t, path)
	if len(lines) != 3 {
		t.Fatalf("expected 3 appended lines, got %d", len(lines))
	}
	for i, line := range lines {
		var b metrics.Batch
		if err := json.Unmarshal([]byte(line), &b); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if b.BatchSeq != uint64(i) {
			t.Errorf("line %d: batch_seq %d, want %d", i, b.BatchSeq, i)
		}
	}
}

func TestFileExporter_CreatesParentDirs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "c", "metrics.jsonl")

	e, err := NewFileExporter(FileExporterConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	defer e.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o644 {
		t.Errorf("expected mode 0644, got %o", perm)
	}
}

func TestFileExporter_Rotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.jsonl")

	e, err := NewFileExporter(FileExporterConfig{
		Path:       path,
		MaxMB:      1,
		MaxBackups: 2,
	})
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	// Shrink the threshold so the test does not have to write megabytes.
	// One encoded batch below is ~460 bytes, so this holds three per file.
	e.maxBytes = 1500

	// Each batch fits comfortably; a few of them together overflow.
	for i := 0; i < 40; i++ {
		batch := &metrics.Batch{BatchSeq: uint64(i), AgentInstanceID: strings.Repeat("x", 60)}
		if err := e.Export(context.Background(), batch); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if fi, err := os.Stat(path); err != nil {
		t.Fatalf("live file missing: %v", err)
	} else if fi.Size() > 1500 {
		t.Errorf("live file %d bytes, expected rotation to keep it under 1500", fi.Size())
	}

	for _, n := range []int{1, 2} {
		if _, err := os.Stat(backupPath(path, n)); err != nil {
			t.Errorf("expected backup %s: %v", backupPath(path, n), err)
		}
	}
	if _, err := os.Stat(backupPath(path, 3)); !os.IsNotExist(err) {
		t.Errorf("expected %s to be pruned (MaxBackups=2)", backupPath(path, 3))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 3 {
		names := make([]string, 0, len(entries))
		for _, en := range entries {
			names = append(names, en.Name())
		}
		t.Errorf("expected 3 files (live + 2 backups), got %v", names)
	}

	// .1 is newer than .2: the highest sequence number lives in the live file,
	// then .1, then .2.
	seqOf := func(p string) uint64 {
		lines := readLines(t, p)
		if len(lines) == 0 {
			t.Fatalf("%s is empty", p)
		}
		var b metrics.Batch
		if err := json.Unmarshal([]byte(lines[0]), &b); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		return b.BatchSeq
	}
	if seqOf(backupPath(path, 2)) >= seqOf(backupPath(path, 1)) {
		t.Error("expected .2 to hold older batches than .1")
	}
	if seqOf(backupPath(path, 1)) >= seqOf(path) {
		t.Error("expected .1 to hold older batches than the live file")
	}
}

func TestFileExporter_RotationDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")

	e, err := NewFileExporter(FileExporterConfig{Path: path, MaxMB: -1})
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := e.Export(context.Background(), &metrics.Batch{AgentInstanceID: strings.Repeat("y", 100)}); err != nil {
			t.Fatalf("Export: %v", err)
		}
	}
	e.Close()

	if _, err := os.Stat(backupPath(path, 1)); !os.IsNotExist(err) {
		t.Error("expected no rotation when MaxMB is negative")
	}
	if lines := readLines(t, path); len(lines) != 50 {
		t.Errorf("expected 50 lines, got %d", len(lines))
	}
}

// A single batch larger than the rotation threshold must still be written,
// not spin the rotator forever.
func TestFileExporter_OversizedBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")

	e, err := NewFileExporter(FileExporterConfig{Path: path, MaxMB: 1, MaxBackups: 1})
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	e.maxBytes = 50

	for i := 0; i < 3; i++ {
		if err := e.Export(context.Background(), &metrics.Batch{AgentInstanceID: strings.Repeat("z", 500)}); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}
	e.Close()

	if lines := readLines(t, path); len(lines) != 1 {
		t.Errorf("expected the last oversized batch alone in the live file, got %d lines", len(lines))
	}
	if s := e.Stats(); s.BatchesExported != 3 {
		t.Errorf("expected 3 exported, got %d", s.BatchesExported)
	}
}

func TestFileExporter_ExportAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")

	e, err := NewFileExporter(FileExporterConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Errorf("second Close should be a no-op, got %v", err)
	}

	if err := e.Export(context.Background(), &metrics.Batch{}); err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
	s := e.Stats()
	if s.BatchesDropped != 1 {
		t.Errorf("expected 1 dropped, got %d", s.BatchesDropped)
	}
	if s.LastError != ErrClosed.Error() {
		t.Errorf("expected last error %q, got %q", ErrClosed, s.LastError)
	}
}

func TestFileExporter_ErrorsSurface(t *testing.T) {
	dir := t.TempDir()
	// A directory cannot be opened for writing.
	if _, err := NewFileExporter(FileExporterConfig{Path: dir}); err == nil {
		t.Fatal("expected an error opening a directory as the log file")
	}
}

func TestFileExporter_CancelledContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")

	e, err := NewFileExporter(FileExporterConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	defer e.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := e.Export(ctx, &metrics.Batch{}); err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if lines := readLines(t, path); len(lines) != 0 {
		t.Errorf("expected nothing written, got %d lines", len(lines))
	}
}
