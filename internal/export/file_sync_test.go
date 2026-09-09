package export

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

func seqIn(t *testing.T, path string) []uint64 {
	t.Helper()
	var out []uint64
	for _, line := range readLines(t, path) {
		var b metrics.Batch
		if err := json.Unmarshal([]byte(line), &b); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		out = append(out, b.BatchSeq)
	}
	return out
}

// TestFileExporter_ReopenRetryDoesNotReplayTheBackupShift covers the retry that
// needsReopen exists for. A rotation is two halves: a destructive backup shift,
// then a reopen. When the reopen fails -- a full or read-only volume -- only the
// reopen may be retried. Replaying the shift would consume one backup generation
// every cycle and destroy all of them within MaxBackups cycles without writing a
// single batch, which is the worst possible response to a disk that is merely
// full.
//
// The failed reopen is set up rather than provoked, because the only way to make
// the shift succeed and the create fail is a read-only parent directory, and
// root walks straight through that. needsReopen is precisely the state such a
// failure leaves behind, and setting it drives the same branch; this package's
// existing rotation tests reach into e.maxBytes the same way.
func TestFileExporter_ReopenRetryDoesNotReplayTheBackupShift(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.jsonl")

	e, err := NewFileExporter(FileExporterConfig{Path: path, MaxMB: 1, MaxBackups: 3})
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	e.maxBytes = 300

	pad := strings.Repeat("x", 200)
	for i := 0; i < 2; i++ {
		if err := e.Export(context.Background(), &metrics.Batch{BatchSeq: uint64(i), AgentInstanceID: pad}); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}
	if got := seqIn(t, backupPath(path, 1)); len(got) != 1 || got[0] != 0 {
		t.Fatalf("%s holds %v, want the first batch", backupPath(path, 1), got)
	}

	// Rotation is now out of reach on size, so a .2 appearing below can only
	// have come from the retry replaying the destructive half.
	e.maxBytes = 1 << 20
	e.needsReopen = true

	// A path whose parent is a regular file cannot be created, on any user and
	// any platform: MkdirAll fails with ENOTDIR before OpenFile is reached, so
	// this is a reopen failure even when the tests run as root.
	blocked := filepath.Join(dir, "blocked")
	if err := os.WriteFile(blocked, nil, 0o644); err != nil {
		t.Fatalf("write %s: %v", blocked, err)
	}
	e.path = filepath.Join(blocked, "metrics.jsonl")
	if err := e.Export(context.Background(), &metrics.Batch{BatchSeq: 8}); err == nil {
		t.Fatal("a reopen onto an uncreatable path must fail the export")
	}
	if !e.needsReopen {
		t.Error("a failed retry must leave the rotation unfinished, or the next cycle replays the shift")
	}
	if s := e.Stats(); s.BatchesDropped != 1 || s.BatchesExported != 2 {
		t.Errorf("stats = %+v, want the failed retry counted as a drop", s)
	}

	e.path = path
	if err := e.Export(context.Background(), &metrics.Batch{BatchSeq: 9}); err != nil {
		t.Fatalf("retry after the path came back: %v", err)
	}
	if e.needsReopen {
		t.Error("a successful retry must clear needsReopen")
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(backupPath(path, 2)); !os.IsNotExist(err) {
		t.Error("the retry consumed a backup generation: it replayed the shift instead of reopening")
	}
	if got := seqIn(t, backupPath(path, 1)); len(got) != 1 || got[0] != 0 {
		t.Errorf("%s holds %v, want the untouched first batch", backupPath(path, 1), got)
	}
	if got := seqIn(t, path); len(got) != 2 || got[0] != 1 || got[1] != 9 {
		t.Errorf("live file holds %v, want the rotated-into batch plus the retried one", got)
	}
}

// TestFileExporter_FsyncIsIssuedOnlyWhenConfigured pins EXPORT_FILE_FSYNC to the
// call it pays for. The fsync costs a device round trip under the exporter's
// lock and collection is synchronous, so the flag has to be exactly what decides
// whether it happens: an inverted or dropped guard would either stall collection
// on a network volume with nothing on the wire to say why, or quietly lose the
// durability an operator paid for.
//
// Observing a successful fsync is not possible portably, so the observation is
// inverted. e.file is pointed at a closed descriptor while e.w keeps writing to
// the live file, so the write and the flush still succeed and the Sync is the
// only call that can fail. With the flag on that failure must surface; with it
// off it cannot, because the call is never made.
func TestFileExporter_FsyncIsIssuedOnlyWhenConfigured(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sync    bool
		wantErr bool
	}{
		{"fsync on", true, true},
		{"fsync off", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "metrics.jsonl")

			e, err := NewFileExporter(FileExporterConfig{Path: path, Sync: tc.sync})
			if err != nil {
				t.Fatalf("NewFileExporter: %v", err)
			}
			defer e.Close()

			dead, err := os.CreateTemp(dir, "closed")
			if err != nil {
				t.Fatalf("CreateTemp: %v", err)
			}
			dead.Close()
			e.file = dead

			err = e.Export(context.Background(), &metrics.Batch{BatchSeq: 1})
			if tc.wantErr {
				if err == nil {
					t.Fatal("EXPORT_FILE_FSYNC is set and the fsync could not have succeeded, yet Export reported none")
				}
				if !strings.Contains(err.Error(), "fsync") {
					t.Errorf("error = %v, want it to name the fsync", err)
				}
				if s := e.Stats(); s.BatchesExported != 0 || s.BatchesDropped != 1 {
					t.Errorf("stats = %+v, want the batch counted as dropped", s)
				}
				return
			}
			if err != nil {
				t.Fatalf("Export with fsync off: %v", err)
			}
			if s := e.Stats(); s.BatchesExported != 1 {
				t.Errorf("stats = %+v, want the batch exported", s)
			}
		})
	}
}

// TestFileExporter_ConfigCarriesTheFsyncSetting checks the flag survives the
// factory too. A durability switch honoured by the exporter and dropped by
// export.New is dead in exactly the way GROUP_STATES once was: wired at both
// ends and connected in the middle by nothing.
func TestFileExporter_ConfigCarriesTheFsyncSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	e, err := New(Config{Mode: ModeFile, Path: path, Sync: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer e.Close()

	fe, ok := e.(*FileExporter)
	if !ok {
		t.Fatalf("export.New returned %T, want *FileExporter", e)
	}
	if !fe.sync {
		t.Error("EXPORT_FILE_FSYNC did not reach the file exporter")
	}
}
