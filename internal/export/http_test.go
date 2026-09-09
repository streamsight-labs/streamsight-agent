package export

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// decodeBatch reads a request body, transparently un-gzipping it.
func decodeBatch(t *testing.T, r *http.Request) metrics.Batch {
	t.Helper()

	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Fatalf("gzip reader: %v", err)
		}
		defer zr.Close()
		body = zr
	}

	var batch metrics.Batch
	if err := json.NewDecoder(body).Decode(&batch); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return batch
}

func TestHTTPExporter_Export_Success(t *testing.T) {
	var received atomic.Int32
	var idempotency atomic.Value

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json")
		}
		if r.Header.Get("X-API-Key") != "test-key" {
			t.Errorf("expected X-API-Key test-key")
		}
		if enc := r.Header.Get("Content-Encoding"); enc != "gzip" {
			t.Errorf("expected Content-Encoding gzip, got %q", enc)
		}
		idempotency.Store(r.Header.Get("Idempotency-Key"))

		batch := decodeBatch(t, r)
		if batch.BatchSeq != 7 {
			t.Errorf("expected batch_seq 7, got %d", batch.BatchSeq)
		}

		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint: server.URL,
		APIKey:   "test-key",
	})

	batch := &metrics.Batch{
		SchemaVersion:   metrics.SchemaVersion,
		AgentInstanceID: "agent-abc",
		BatchSeq:        7,
		CollectedAt:     time.Now(),
		CollectionMs:    50,
	}

	if err := exporter.Export(context.Background(), batch); err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	exporter.Close()

	if received.Load() != 1 {
		t.Errorf("expected 1 request, got %d", received.Load())
	}
	if got := idempotency.Load(); got != "agent-abc-7" {
		t.Errorf("expected Idempotency-Key agent-abc-7, got %v", got)
	}

	stats := exporter.Stats()
	if stats.BatchesExported != 1 {
		t.Errorf("expected 1 exported, got %d", stats.BatchesExported)
	}
	if stats.BatchesDropped != 0 || stats.BatchesRejected != 0 || stats.ExportRetries != 0 {
		t.Errorf("unexpected failure counters: %+v", stats)
	}
	if stats.LastError != "" {
		t.Errorf("expected no last error, got %q", stats.LastError)
	}
}

func TestHTTPExporter_Export_Gzip(t *testing.T) {
	var rawLen atomic.Int64
	var seen atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("expected Content-Encoding gzip, got %q", r.Header.Get("Content-Encoding"))
		}
		if r.ContentLength > 0 {
			rawLen.Store(r.ContentLength)
		}
		batch := decodeBatch(t, r)
		if len(batch.Topics) != 200 {
			t.Errorf("expected 200 topics after gunzip, got %d", len(batch.Topics))
		}
		seen.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint: server.URL,
		APIKey:   "test-key",
	})

	batch := &metrics.Batch{CollectedAt: time.Now()}
	for i := 0; i < 200; i++ {
		batch.Topics = append(batch.Topics, metrics.TopicMetrics{
			Name:              "orders-events-topic",
			PartitionCount:    12,
			ReplicationFactor: 3,
		})
	}

	uncompressed, err := encodeLine(batch)
	if err != nil {
		t.Fatalf("encodeLine: %v", err)
	}

	if err := exporter.Export(context.Background(), batch); err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	exporter.Close()

	if seen.Load() != 1 {
		t.Fatalf("expected 1 request, got %d", seen.Load())
	}
	if got := rawLen.Load(); got == 0 || got >= int64(len(uncompressed)) {
		t.Errorf("expected compressed body smaller than %d bytes, got %d", len(uncompressed), got)
	}
}

func TestHTTPExporter_Export_RetryOn5xx(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attempts.Add(1)
		if count < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint:   server.URL,
		APIKey:     "test-key",
		MaxRetries: 3,
		BaseDelay:  time.Millisecond,
	})

	exporter.Export(context.Background(), &metrics.Batch{CollectedAt: time.Now()})

	// Wait for retries before shutdown interrupts them.
	time.Sleep(200 * time.Millisecond)
	exporter.Close()

	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
	stats := exporter.Stats()
	if stats.BatchesExported != 1 {
		t.Errorf("expected 1 exported, got %d", stats.BatchesExported)
	}
	if stats.ExportRetries != 2 {
		t.Errorf("expected 2 retries, got %d", stats.ExportRetries)
	}
}

// 408 and 429 are transient; treating them as terminal, as a plain "4xx is
// fatal" rule does, permanently discards customer data on a rate limit.
func TestHTTPExporter_Export_RetriesTransient4xx(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var attempts atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if attempts.Add(1) < 2 {
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			exporter := NewHTTPExporter(HTTPExporterConfig{
				Endpoint:   server.URL,
				APIKey:     "test-key",
				MaxRetries: 3,
				BaseDelay:  time.Millisecond,
			})

			exporter.Export(context.Background(), &metrics.Batch{CollectedAt: time.Now()})
			time.Sleep(200 * time.Millisecond)
			exporter.Close()

			if attempts.Load() != 2 {
				t.Errorf("expected 2 attempts, got %d", attempts.Load())
			}
			if s := exporter.Stats(); s.BatchesExported != 1 || s.BatchesRejected != 0 {
				t.Errorf("expected exported=1 rejected=0, got %+v", s)
			}
		})
	}
}

func TestHTTPExporter_Export_HonorsRetryAfter(t *testing.T) {
	var attempts atomic.Int32
	var gap atomic.Int64
	var last atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().UnixNano()
		if prev := last.Swap(now); prev != 0 {
			gap.Store(now - prev)
		}
		if attempts.Add(1) < 2 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint:   server.URL,
		APIKey:     "test-key",
		MaxRetries: 2,
		BaseDelay:  time.Millisecond, // would be ~0 without Retry-After
	})

	exporter.Export(context.Background(), &metrics.Batch{CollectedAt: time.Now()})
	time.Sleep(1500 * time.Millisecond)
	exporter.Close()

	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
	if d := time.Duration(gap.Load()); d < 900*time.Millisecond {
		t.Errorf("expected retry delayed ~1s by Retry-After, waited %v", d)
	}
}

func TestHTTPExporter_Export_NoRetryOnTerminal4xx(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "schema_version unsupported")
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint:   server.URL,
		APIKey:     "test-key",
		MaxRetries: 3,
		BaseDelay:  time.Millisecond,
	})

	exporter.Export(context.Background(), &metrics.Batch{CollectedAt: time.Now()})
	exporter.Close()

	if attempts.Load() != 1 {
		t.Errorf("expected 1 attempt (no retry on terminal 4xx), got %d", attempts.Load())
	}
	stats := exporter.Stats()
	if stats.BatchesRejected != 1 {
		t.Errorf("expected 1 rejected, got %d", stats.BatchesRejected)
	}
	if !strings.Contains(stats.LastError, "HTTP 400") {
		t.Errorf("expected last error to mention HTTP 400, got %q", stats.LastError)
	}
}

func TestHTTPExporter_Export_ExhaustedRetriesCountAsDropped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint:   server.URL,
		APIKey:     "test-key",
		MaxRetries: 2,
		BaseDelay:  time.Millisecond,
	})

	exporter.Export(context.Background(), &metrics.Batch{CollectedAt: time.Now()})
	time.Sleep(200 * time.Millisecond)
	exporter.Close()

	stats := exporter.Stats()
	if stats.BatchesDropped != 1 {
		t.Errorf("expected 1 dropped, got %d", stats.BatchesDropped)
	}
	if stats.BatchesExported != 0 {
		t.Errorf("expected 0 exported, got %d", stats.BatchesExported)
	}
}

func TestHTTPExporter_Close_DrainsQueue(t *testing.T) {
	var received atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint: server.URL,
		APIKey:   "test-key",
	})

	for i := 0; i < 5; i++ {
		if err := exporter.Export(context.Background(), &metrics.Batch{CollectedAt: time.Now()}); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}

	exporter.Close()

	if received.Load() != 5 {
		t.Errorf("expected 5 requests after close, got %d", received.Load())
	}
}

func TestHTTPExporter_Export_QueueFull(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var got []uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		b := decodeBatch(t, r)
		mu.Lock()
		got = append(got, b.BatchSeq)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint:  server.URL,
		APIKey:    "test-key",
		QueueSize: 2,
	})

	// The newest batch is always accepted, so Export never reports a full queue
	// to its caller: the batch it was handed IS queued. What was lost is an
	// older batch, and that is reported through the counters instead -- the
	// agent logs "export failed" on a returned error, which would be a lie here.
	for i := uint64(0); i < 8; i++ {
		if err := exporter.Export(context.Background(), &metrics.Batch{BatchSeq: i, CollectedAt: time.Now()}); err != nil {
			t.Fatalf("Export(seq %d) = %v, want nil: drop-oldest accepts the incoming batch", i, err)
		}
	}

	stats := exporter.Stats()
	if stats.BatchesDropped == 0 {
		t.Fatal("expected evictions with a 2-slot queue and a blocked server")
	}
	// LastError is a string on the wire, not an error, so this is a plain
	// comparison against the message rather than errors.Is.
	if stats.LastError != ErrQueueFull.Error() {
		t.Errorf("expected last error %q, got %q", ErrQueueFull, stats.LastError)
	}

	close(release)
	exporter.Close()

	mu.Lock()
	defer mu.Unlock()
	// THE POINT OF THE POLICY: what survives is the newest. One batch is in
	// flight and the queue holds the last two, so the final batch must arrive
	// and an early one must not. Under drop-newest this assertion inverts.
	if !slices.Contains(got, 7) {
		t.Errorf("newest batch (seq 7) was dropped; delivered %v", got)
	}
	if slices.Contains(got, 3) {
		t.Errorf("a superseded batch (seq 3) was delivered instead of being evicted; delivered %v", got)
	}
	if len(got) > 3 {
		t.Errorf("delivered %d batches, want at most 3 (1 in flight + 2 queued): %v", len(got), got)
	}
}

// The retry budget is what stops one wedged batch from occupying the single
// worker while the queue behind it turns over.
func TestHTTPExporter_RetryBudgetAbandonsAWedgedBatch(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		// A hostile Retry-After: honoured verbatim, three of these would hold
		// the worker for 15 minutes.
		w.Header().Set("Retry-After", "300")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint:  server.URL,
		APIKey:    "test-key",
		QueueSize: 2,
	})
	exporter.retryBudget = 50 * time.Millisecond

	start := time.Now()
	if err := exporter.Export(context.Background(), &metrics.Batch{BatchSeq: 1, CollectedAt: time.Now()}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	exporter.Close()
	elapsed := time.Since(start)

	// Without the budget this sleeps for the clamped Retry-After before its
	// second attempt; with it, the batch is abandoned as soon as the next delay
	// is known to outlive the budget.
	if elapsed > 5*time.Second {
		t.Errorf("took %s: the retry budget did not bound the wedged batch", elapsed)
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("server saw %d attempts, want 1: the 300s Retry-After exceeds the budget so no retry should be waited out", n)
	}
	if s := exporter.Stats(); s.BatchesDropped != 1 {
		t.Errorf("BatchesDropped = %d, want 1", s.BatchesDropped)
	}
}

func TestHTTPExporter_Export_AfterClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint: server.URL,
		APIKey:   "test-key",
	})

	exporter.Close()
	if err := exporter.Close(); err != nil {
		t.Errorf("second Close should be a no-op, got %v", err)
	}

	err := exporter.Export(context.Background(), &metrics.Batch{CollectedAt: time.Now()})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestHTTPExporter_Export_CancelledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be reached")
	}))
	defer server.Close()

	exporter := NewHTTPExporter(HTTPExporterConfig{
		Endpoint: server.URL,
		APIKey:   "test-key",
	})
	defer exporter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := exporter.Export(ctx, &metrics.Batch{CollectedAt: time.Now()}); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRetryableStatus(t *testing.T) {
	cases := map[int]bool{
		200: false,
		400: false,
		401: false,
		403: false,
		404: false,
		408: true,
		413: false,
		422: false,
		429: true,
		500: true,
		502: true,
		503: true,
		504: true,
	}
	for code, want := range cases {
		if got := retryableStatus(code); got != want {
			t.Errorf("retryableStatus(%d) = %v, want %v", code, got, want)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter(""); got != 0 {
		t.Errorf("empty header: got %v, want 0", got)
	}
	if got := parseRetryAfter("garbage"); got != 0 {
		t.Errorf("garbage header: got %v, want 0", got)
	}
	if got := parseRetryAfter("5"); got != 5*time.Second {
		t.Errorf("delta-seconds: got %v, want 5s", got)
	}
	if got := parseRetryAfter("-3"); got != 0 {
		t.Errorf("negative delta: got %v, want 0", got)
	}
	if got := parseRetryAfter("99999"); got != maxRetryAfter {
		t.Errorf("oversized delta should clamp: got %v, want %v", got, maxRetryAfter)
	}

	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(future); got < 20*time.Second || got > 31*time.Second {
		t.Errorf("http-date: got %v, want ~30s", got)
	}
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(past); got != 0 {
		t.Errorf("past http-date: got %v, want 0", got)
	}
}

func TestHTTPExporter_Backoff_FullJitter(t *testing.T) {
	e := NewHTTPExporter(HTTPExporterConfig{
		Endpoint:  "http://127.0.0.1:1",
		APIKey:    "k",
		BaseDelay: time.Second,
	})
	defer e.Close()

	// Attempt 3's ceiling is 4s; every draw must land inside [0, 4s] and the
	// draws must not all be identical (that would be bare doubling).
	var distinct int
	prev := time.Duration(-1)
	for i := 0; i < 50; i++ {
		d := e.backoff(3)
		if d < 0 || d > 4*time.Second {
			t.Fatalf("backoff(3) = %v, want within [0, 4s]", d)
		}
		if d != prev {
			distinct++
		}
		prev = d
	}
	if distinct < 2 {
		t.Error("expected jittered backoff, got a constant delay")
	}

	if d := e.backoff(40); d > maxBackoff {
		t.Errorf("backoff(40) = %v, want <= %v", d, maxBackoff)
	}
}
