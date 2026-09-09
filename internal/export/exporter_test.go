package export

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// All three exporters must satisfy the interface the agent depends on.
var (
	_ Exporter = (*HTTPExporter)(nil)
	_ Exporter = (*FileExporter)(nil)
	_ Exporter = (*StdoutExporter)(nil)
)

func TestNew_File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "metrics.jsonl")

	e, err := New(Config{Mode: ModeFile, Path: path, MaxMB: 10, MaxBackups: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer e.Close()

	if _, ok := e.(*FileExporter); !ok {
		t.Fatalf("expected *FileExporter, got %T", e)
	}
	// Local mode must need neither an endpoint nor an API key.
	if err := e.Export(context.Background(), &metrics.Batch{BatchSeq: 1}); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

func TestNew_FileRequiresPath(t *testing.T) {
	if _, err := New(Config{Mode: ModeFile}); err == nil {
		t.Fatal("expected an error when EXPORT_FILE is empty")
	} else if !strings.Contains(err.Error(), "EXPORT_FILE") {
		t.Errorf("error should name EXPORT_FILE, got %q", err)
	}
}

func TestNew_HTTP(t *testing.T) {
	e, err := New(Config{Mode: ModeHTTP, Endpoint: "http://127.0.0.1:1/ingest", APIKey: "k"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer e.Close()

	if _, ok := e.(*HTTPExporter); !ok {
		t.Fatalf("expected *HTTPExporter, got %T", e)
	}
}

func TestNew_HTTPValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"no endpoint", Config{Mode: ModeHTTP, APIKey: "k"}, "EXPORT_ENDPOINT"},
		{"no api key", Config{Mode: ModeHTTP, Endpoint: "http://x/ingest"}, "API_KEY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := New(tc.cfg)
			if err == nil {
				e.Close()
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should name %s, got %q", tc.want, err)
			}
		})
	}
}

func TestNew_Stdout(t *testing.T) {
	e, err := New(Config{Mode: ModeStdout})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer e.Close()

	if _, ok := e.(*StdoutExporter); !ok {
		t.Fatalf("expected *StdoutExporter, got %T", e)
	}
}

func TestNew_UnknownMode(t *testing.T) {
	if _, err := New(Config{Mode: "kafka"}); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected an error for an empty mode")
	}
}

func TestEncodeLine_SingleLine(t *testing.T) {
	line, err := encodeLine(&metrics.Batch{AgentInstanceID: "a\nb\tc"})
	if err != nil {
		t.Fatalf("encodeLine: %v", err)
	}
	if bytes.Count(line, []byte("\n")) != 1 || line[len(line)-1] != '\n' {
		t.Errorf("a batch must encode to exactly one terminated line, got %q", line)
	}
	if _, err := encodeLine(nil); err == nil {
		t.Error("expected an error for a nil batch")
	}
}
