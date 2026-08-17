package mockingest

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"kafka-metrics-agent/internal/export"
)

// These drive the REAL HTTPExporter against the real validator over a real
// socket, so every retry is conformance-checked too and a retry that corrupts
// the batch fails the test instead of passing quietly.
//
// Everything here is hermetic — no Kafka, no Docker — so it runs in the default
// `go test -race ./...`. Keep it that way: this is the only part of the http
// harness CI can execute.

// syncBuffer is the report sink; the server writes from the handler goroutine
// while the test reads.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func newLive(t *testing.T, mutate func(*Config)) (*Server, string, *syncBuffer) {
	t.Helper()
	out := &syncBuffer{}
	s := newTestServer(t, func(c *Config) {
		c.Out = out
		if mutate != nil {
			mutate(c)
		}
	})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts.URL + DefaultPath, out
}

func newExporter(t *testing.T, endpoint string, mutate func(*export.HTTPExporterConfig)) *export.HTTPExporter {
	t.Helper()
	cfg := export.HTTPExporterConfig{
		Endpoint:   endpoint,
		APIKey:     "k",
		Gzip:       true,
		MaxRetries: 6,
		BaseDelay:  time.Millisecond,
		Timeout:    2 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return export.NewHTTPExporter(cfg)
}

// shipAndSettle exports the batches and waits for every one to reach a terminal
// state. It polls the counters rather than just calling Close, because Close
// abandons an in-flight backoff on purpose, cancelling exactly the retries these
// tests exist to exercise.
func shipAndSettle(t *testing.T, e *export.HTTPExporter, seqs ...uint64) export.Stats {
	t.Helper()
	for _, seq := range seqs {
		if err := e.Export(context.Background(), validBatch(seq)); err != nil {
			t.Fatalf("export seq %d: %v", seq, err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		st := e.Stats()
		if int(st.BatchesExported+st.BatchesDropped+st.BatchesRejected) >= len(seqs) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for export to settle: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return e.Stats()
}

func fatalCodes(s *Server) []string {
	var out []string
	for _, f := range s.Failures() {
		if f.Severity == SeverityFatal {
			out = append(out, f.Code)
		}
	}
	return out
}

func TestExporterHappyPath(t *testing.T) {
	for _, gz := range []bool{true, false} {
		name := "gzip"
		if !gz {
			name = "plain"
		}
		t.Run(name, func(t *testing.T) {
			s, url, _ := newLive(t, nil)
			e := newExporter(t, url, func(c *export.HTTPExporterConfig) { c.Gzip = gz })

			st := shipAndSettle(t, e, 1, 2, 3)
			if st.BatchesExported != 3 || st.BatchesDropped != 0 || st.BatchesRejected != 0 {
				t.Fatalf("exporter stats = %+v", st)
			}
			if got := s.Stats(); got.OK != 3 || got.Accepted != 3 {
				t.Fatalf("server stats = %+v", got)
			}
			if got := fatalCodes(s); len(got) != 0 {
				t.Fatalf("conformance failures: %v", got)
			}
		})
	}
}

func TestExporterRetriesThroughInjected500s(t *testing.T) {
	s, url, _ := newLive(t, func(c *Config) { c.Faults = Faults{FailFirst: 2} })
	e := newExporter(t, url, nil)

	st := shipAndSettle(t, e, 1)
	if st.BatchesExported != 1 {
		t.Fatalf("exporter stats = %+v", st)
	}
	if st.ExportRetries < 2 {
		t.Fatalf("expected at least two retries, got %d", st.ExportRetries)
	}
	// Same batch, same key, three deliveries: the redeliveries must not be
	// reported as replays, because a 500 is not an acknowledgement.
	if got := s.Stats(); got.Requests != 3 || got.Duplicates != 2 {
		t.Fatalf("server stats = %+v", got)
	}
	if got := fatalCodes(s); len(got) != 0 {
		t.Fatalf("conformance failures: %v", got)
	}
}

func TestExporterHonoursRetryAfter(t *testing.T) {
	if testing.Short() {
		t.Skip("Retry-After has one-second granularity, so this one is genuinely slow")
	}
	s, url, _ := newLive(t, func(c *Config) {
		c.Faults = Faults{FailFirst: 1, FailStatus: 429, RetryAfterSec: 1}
	})
	e := newExporter(t, url, nil)

	start := time.Now()
	st := shipAndSettle(t, e, 1)
	elapsed := time.Since(start)

	if st.BatchesExported != 1 {
		t.Fatalf("exporter stats = %+v", st)
	}
	// The exporter's own backoff here is a millisecond, so anything near a
	// second can only have come from the header.
	if elapsed < 900*time.Millisecond {
		t.Fatalf("retried after %s; Retry-After: 1 was ignored", elapsed)
	}
	if got := fatalCodes(s); len(got) != 0 {
		t.Fatalf("conformance failures: %v", got)
	}
}

func TestExporterDoesNotRetryTerminal4xx(t *testing.T) {
	s, url, _ := newLive(t, func(c *Config) { c.Faults = Faults{TerminalOnce: 400} })
	e := newExporter(t, url, nil)

	st := shipAndSettle(t, e, 1)
	if st.BatchesRejected != 1 || st.BatchesExported != 0 {
		t.Fatalf("exporter stats = %+v", st)
	}
	if got := s.Stats(); got.Requests != 1 {
		t.Fatalf("a terminal 4xx must not be retried, got %d requests", got.Requests)
	}
}

func TestExporterRecoversFromADroppedConnection(t *testing.T) {
	s, url, out := newLive(t, func(c *Config) { c.Faults = Faults{DropFirst: 1} })
	e := newExporter(t, url, nil)

	st := shipAndSettle(t, e, 1)
	if st.BatchesExported != 1 {
		t.Fatalf("exporter stats = %+v", st)
	}
	if got := s.Stats(); got.Requests != 2 {
		t.Fatalf("expected one drop and one redelivery, got %d requests", got.Requests)
	}
	if got := fatalCodes(s); len(got) != 0 {
		t.Fatalf("conformance failures: %v", got)
	}
	// Marking the redelivery as following a fault is what stops it reading as a
	// duplicate the agent invented.
	if !strings.Contains(out.String(), "[retry*]") {
		t.Fatalf("expected the redelivery to be marked as following an injected fault:\n%s", out.String())
	}
}

func TestExporterRecoversFromAClientTimeout(t *testing.T) {
	s, url, _ := newLive(t, func(c *Config) {
		c.Faults = Faults{DelayMS: 400, DelayFirst: 1}
	})
	e := newExporter(t, url, func(c *export.HTTPExporterConfig) { c.Timeout = 50 * time.Millisecond })

	st := shipAndSettle(t, e, 1)
	if st.BatchesExported != 1 {
		t.Fatalf("exporter stats = %+v", st)
	}
	// The mock answered 200 to the first delivery, but nobody was listening, so
	// it must not have been recorded as acknowledged.
	if got := fatalCodes(s); len(got) != 0 {
		t.Fatalf("conformance failures: %v", got)
	}
}

func TestCorruptBatchIsRejectedByName(t *testing.T) {
	s, url, _ := newLive(t, func(c *Config) { c.Strict = true })
	e := newExporter(t, url, nil)

	b := validBatch(1)
	// Committed above the high watermark: negative lag, which the collector's
	// phase order exists to make impossible.
	b.Offsets[0].Offsets[0].Offset = i64(105)

	if err := e.Export(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if st := e.Stats(); st.BatchesRejected != 1 {
		t.Fatalf("exporter stats = %+v", st)
	}
	if !hasCode(s.Failures(), "data.negative_lag") {
		t.Fatalf("want data.negative_lag, got %v", fatalCodes(s))
	}
	// The agent logs at most errBodyLimit bytes of the response, so the reason
	// has to survive in that budget.
	if st := e.Stats(); !strings.Contains(st.LastError, "data.negative_lag") {
		t.Fatalf("the reason did not reach the agent's own telemetry: %q", st.LastError)
	}
}
