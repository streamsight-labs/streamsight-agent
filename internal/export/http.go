package export

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"kafka-metrics-agent/internal/metrics"
)

const (
	defaultQueueSize   = 100
	defaultMaxRetries  = 3
	defaultBaseDelay   = 1 * time.Second
	defaultHTTPTimeout = 10 * time.Second

	// maxBackoff caps the exponential before jitter is applied.
	maxBackoff = 30 * time.Second
	// maxRetryAfter bounds a server-supplied Retry-After so a hostile or
	// broken header cannot stall the worker indefinitely.
	maxRetryAfter = 5 * time.Minute
	// errBodyLimit is how much of an error response we read back. The body is
	// drained regardless so the connection can be reused.
	errBodyLimit = 4 << 10
)

// HTTPExporter POSTs one Batch per request to the global ingest endpoint. A
// bounded queue and a single worker keep collection off the network path:
// collection never blocks, and an overflowing queue drops the newest batch
// rather than stalling the agent.
type HTTPExporter struct {
	counters

	client   *http.Client
	endpoint string
	apiKey   string
	gzip     bool

	queue chan *metrics.Batch
	wg    sync.WaitGroup
	done  chan struct{}

	// ctx is cancelled once the drain is finished (or has overrun its grace
	// period), which aborts any in-flight request.
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.RWMutex
	closed bool

	maxRetries    int
	baseDelay     time.Duration
	timeout       time.Duration
	shutdownGrace time.Duration

	rndMu sync.Mutex
	rnd   *rand.Rand
}

type HTTPExporterConfig struct {
	Endpoint   string
	APIKey     string
	QueueSize  int
	MaxRetries int
	BaseDelay  time.Duration
	Timeout    time.Duration
	// Gzip compresses the request body. The payload is highly repetitive, so
	// this is typically an 85-90% reduction.
	Gzip bool
}

func NewHTTPExporter(cfg HTTPExporterConfig) *HTTPExporter {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = defaultBaseDelay
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultHTTPTimeout
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &HTTPExporter{
		client:        &http.Client{Timeout: cfg.Timeout},
		endpoint:      cfg.Endpoint,
		apiKey:        cfg.APIKey,
		gzip:          cfg.Gzip,
		queue:         make(chan *metrics.Batch, cfg.QueueSize),
		done:          make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
		maxRetries:    cfg.MaxRetries,
		baseDelay:     cfg.BaseDelay,
		timeout:       cfg.Timeout,
		shutdownGrace: 2 * cfg.Timeout,
		rnd:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	e.wg.Add(1)
	go e.worker()

	return e
}

// Export enqueues the batch. The enqueue is deliberately non-blocking: a slow
// or unreachable ingest must never stall collection, so a full queue drops the
// batch and reports it in Stats.
func (e *HTTPExporter) Export(ctx context.Context, batch *metrics.Batch) error {
	// Checked up front, because the enqueue below cannot block and so could
	// never observe cancellation on its own.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Held for the duration of the send so Close cannot begin draining while
	// a batch is halfway onto the queue.
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		e.dropped.Add(1)
		e.recordError(ErrClosed)
		return ErrClosed
	}

	select {
	case e.queue <- batch:
		return nil
	default:
		e.dropped.Add(1)
		e.recordError(ErrQueueFull)
		slog.Warn("export queue full, dropping batch", "queue_size", cap(e.queue))
		return ErrQueueFull
	}
}

func (e *HTTPExporter) Stats() Stats {
	s := e.snapshot()
	s.QueueDepth = len(e.queue)
	return s
}

// Close stops accepting batches, drains what is queued, and gives up after
// shutdownGrace by cancelling the in-flight request.
func (e *HTTPExporter) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	close(e.done)

	drained := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(e.shutdownGrace):
		slog.Warn("export drain timed out, cancelling in-flight request", "grace", e.shutdownGrace)
		e.cancel()
		<-drained
	}

	e.cancel()
	e.client.CloseIdleConnections()
	return nil
}

func (e *HTTPExporter) worker() {
	defer e.wg.Done()

	for {
		select {
		case batch := <-e.queue:
			e.sendWithRetry(batch)
		case <-e.done:
			for {
				select {
				case batch := <-e.queue:
					e.sendWithRetry(batch)
				default:
					return
				}
			}
		}
	}
}

func (e *HTTPExporter) sendWithRetry(batch *metrics.Batch) {
	var lastErr error
	var retryAfter time.Duration

	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			delay := retryAfter
			if delay <= 0 {
				delay = e.backoff(attempt)
			}
			e.retries.Add(1)
			slog.Debug("retrying export", "attempt", attempt, "delay", delay)
			if !e.wait(delay) {
				e.dropped.Add(1)
				e.recordError(fmt.Errorf("export abandoned during shutdown: %w", lastErr))
				slog.Warn("export abandoned during shutdown", "error", lastErr)
				return
			}
		}

		err := e.send(batch)
		if err == nil {
			e.exported.Add(1)
			slog.Debug("export successful", "collected_at", batch.CollectedAt, "batch_seq", batch.BatchSeq)
			return
		}

		lastErr = err
		retryAfter = 0

		var he *httpError
		if errors.As(err, &he) {
			if !retryableStatus(he.StatusCode) {
				e.rejected.Add(1)
				e.recordError(err)
				slog.Error("ingest rejected batch, not retrying", "error", err)
				return
			}
			retryAfter = he.RetryAfter
		}
		slog.Warn("export failed", "attempt", attempt+1, "error", err)
	}

	e.dropped.Add(1)
	e.recordError(lastErr)
	slog.Error("export failed after retries", "retries", e.maxRetries, "error", lastErr)
}

// backoff is full jitter: a uniform draw from [0, capped exponential]. Bare
// doubling synchronises every agent in the fleet onto the same retry instant.
func (e *HTTPExporter) backoff(attempt int) time.Duration {
	ceiling := e.baseDelay << (attempt - 1)
	if ceiling > maxBackoff || ceiling <= 0 {
		ceiling = maxBackoff
	}
	e.rndMu.Lock()
	d := time.Duration(e.rnd.Int63n(int64(ceiling) + 1))
	e.rndMu.Unlock()
	return d
}

// wait sleeps, but returns false as soon as shutdown starts so Close is not
// held hostage by a backoff.
func (e *HTTPExporter) wait(d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-e.done:
		return false
	}
}

func (e *HTTPExporter) send(batch *metrics.Batch) error {
	data, err := encodeBody(batch, e.gzip)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(e.ctx, e.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", e.apiKey)
	// Lets the ingest deduplicate a batch that was delivered but whose 200 we
	// never saw.
	req.Header.Set("Idempotency-Key", idempotencyKey(batch))
	if e.gzip {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	// Drain before closing: an unread body forces the transport to tear down
	// the connection instead of returning it to the keep-alive pool.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, errBodyLimit))
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return &httpError{
			StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			Body:       string(bytes.TrimSpace(body)),
		}
	}

	return nil
}

func encodeBody(batch *metrics.Batch, compress bool) ([]byte, error) {
	data, err := encodeLine(batch)
	if err != nil {
		return nil, err
	}
	if !compress {
		return data, nil
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		return nil, fmt.Errorf("gzip batch: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip batch: %w", err)
	}
	return buf.Bytes(), nil
}

func idempotencyKey(batch *metrics.Batch) string {
	id := batch.AgentInstanceID
	if id == "" {
		id = "unknown"
	}
	return id + "-" + strconv.FormatUint(batch.BatchSeq, 10)
}

type httpError struct {
	StatusCode int
	RetryAfter time.Duration
	Body       string
}

func (e *httpError) Error() string {
	if e.Body != "" {
		return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
	}
	return fmt.Sprintf("HTTP %d", e.StatusCode)
}

// retryableStatus decides whether a status is worth another attempt. 408 and
// 429 are transient despite being 4xx; discarding them throws away customer
// data on nothing more than a rate limit.
func retryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return true
	}
	return code >= 500
}

// parseRetryAfter understands both forms of the header: delta-seconds and an
// HTTP-date. Returns 0 when absent or unparseable.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return clampRetryAfter(time.Duration(secs) * time.Second)
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return clampRetryAfter(d)
		}
	}
	return 0
}

func clampRetryAfter(d time.Duration) time.Duration {
	if d > maxRetryAfter {
		return maxRetryAfter
	}
	return d
}
