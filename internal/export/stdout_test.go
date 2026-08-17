package export

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"kafka-metrics-agent/internal/metrics"
)

func TestStdoutExporter_JSONL(t *testing.T) {
	var buf bytes.Buffer
	e := newStreamExporter(&buf)

	for i := 0; i < 3; i++ {
		if err := e.Export(context.Background(), &metrics.Batch{BatchSeq: uint64(i)}); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Error("output must be newline terminated")
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	for i, line := range lines {
		var b metrics.Batch
		if err := json.Unmarshal([]byte(line), &b); err != nil {
			t.Fatalf("line %d is not JSON: %v", i, err)
		}
		if b.BatchSeq != uint64(i) {
			t.Errorf("line %d: batch_seq %d, want %d", i, b.BatchSeq, i)
		}
	}

	if s := e.Stats(); s.BatchesExported != 3 {
		t.Errorf("expected 3 exported, got %d", s.BatchesExported)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

func TestStdoutExporter_ErrorSurfaces(t *testing.T) {
	e := newStreamExporter(failWriter{})

	if err := e.Export(context.Background(), &metrics.Batch{}); err == nil {
		t.Fatal("expected the write error to surface")
	}
	s := e.Stats()
	if s.BatchesDropped != 1 {
		t.Errorf("expected 1 dropped, got %d", s.BatchesDropped)
	}
	if !strings.Contains(s.LastError, "boom") {
		t.Errorf("expected last error to mention the cause, got %q", s.LastError)
	}
}

func TestStdoutExporter_CancelledContext(t *testing.T) {
	var buf bytes.Buffer
	e := newStreamExporter(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := e.Export(ctx, &metrics.Batch{}); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if buf.Len() != 0 {
		t.Error("expected nothing written")
	}
}
