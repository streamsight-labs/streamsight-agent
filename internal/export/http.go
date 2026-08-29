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
	// defaultQueueSize is 20 batches, which is 100 seconds at the 5s collection
	// interval. It is deliberately NOT sized to ride out a long ingest outage.
	//
	// Two reasons. The queue holds ENCODED bodies, and an overflow drops the
	// OLDEST, so the buffer's job is to absorb a transient stall without losing
	// anything -- it is not an archive. And metrics that arrive an hour late
	// have almost no value to a monitoring product, so buffering an hour of them
	// would trade real memory for data nobody will act on.
	//
	// 100s comfortably covers the worst-case retry chain for one batch
	// (maxRetryBudget below), which is the stall this is actually defending
	// against.
	defaultQueueSize   = 20
	defaultMaxRetries  = 3
	defaultBaseDelay   = 1 * time.Second
	defaultHTTPTimeout = 10 * time.Second

	// maxBackoff caps the exponential before jitter is applied.
	maxBackoff = 30 * time.Second
	// maxRetryAfter bounds a server-supplied Retry-After so a hostile or
	// broken header cannot stall the worker indefinitely.
	maxRetryAfter = 5 * time.Minute
	// maxRetryBudget bounds the TOTAL wall time one batch may occupy the single
	// worker, across every attempt and every wait between them.
	//
	// The per-attempt bounds are not enough on their own: three retries each
	// honouring a 5-minute Retry-After is 15 minutes on one batch, during which
	// the queue behind it turns over completely and the agent ships nothing. A
	// batch that cannot be delivered within a minute has already been overtaken
	// by fresher ones, so abandoning it is what keeps the freshest data moving.
	maxRetryBudget = time.Minute
	// errBodyLimit is how much of an error response we read back. The body is
	// drained regardless so the connection can be reused.
	errBodyLimit = 4 << 10
)

// queued is one batch already encoded for the wire, plus the few scalars the
// worker still needs after the Batch itself has been released.
//
// Encoding at ENQUEUE rather than at send is what bounds this exporter's
// memory. A *metrics.Batch is a live object graph -- a struct per partition,
// per member and per committed offset -- and retaining a queue of them costs
// roughly the uncompressed JSON, about 9x the gzipped body that will actually
// be sent. Encoding once here also means a batch that needs four attempts is
// marshalled and gzipped once rather than four times.
type queued struct {
	body        []byte
	seq         uint64
	collectedAt time.Time
	// key is the Idempotency-Key, computed here because the Batch it derives
	// from is released as soon as this struct is built.
	key string
}

// HTTPExporter POSTs one Batch per request to the global ingest endpoint. A
// bounded queue and a single worker keep collection off the network path: the
// enqueue never blocks, and an overflowing queue drops its OLDEST entry.
//
// Drop-oldest, not drop-newest: this is a monitoring agent, so when the ingest
// cannot keep up the useful thing to preserve is the most recent picture of the
// cluster. Dropping the incoming batch instead would hold a stale prefix and
// discard everything current, and on recovery the backend would receive minutes
// of history it can no longer act on followed by a hole where the present used
// to be. Either policy leaves a batch_seq gap; only this one leaves it in the
// past.
type HTTPExporter struct {
	counters

	client   *http.Client
	endpoint string
	apiKey   string

	queue chan queued
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
	// retryBudget is maxRetryBudget, a field so a test can shorten it without
	// sleeping for a minute.
	retryBudget time.Duration

	rndMu sync.Mutex
	rnd   *rand.Rand
}

// HTTPExporterConfig configures the HTTP exporter.
type HTTPExporterConfig struct {
	Endpoint   string
	APIKey     string
	QueueSize  int
	MaxRetries int
	BaseDelay  time.Duration
	Timeout    time.Duration
}

// NewHTTPExporter builds the exporter and starts its single worker. Only a
// negative MaxRetries means "unset": zero means "do not retry", for operators
// who would rather drop a batch than let a wedged ingest occupy the single
// export worker for the length of the backoff schedule.
func NewHTTPExporter(cfg HTTPExporterConfig) *HTTPExporter {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.MaxRetries < 0 {
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
		queue:         make(chan queued, cfg.QueueSize),
		done:          make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
		maxRetries:    cfg.MaxRetries,
		baseDelay:     cfg.BaseDelay,
		timeout:       cfg.Timeout,
		shutdownGrace: 2 * cfg.Timeout,
		retryBudget:   maxRetryBudget,
		// Jitter only, so many agents do not retry against a degraded ingest in
		// lockstep. Nothing here is a secret.
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // G404: jitter, not cryptography
	}

	e.wg.Add(1)
	go e.worker()

	return e
}

// Export encodes the batch and enqueues the bytes. The enqueue is deliberately
// non-blocking: a slow or unreachable ingest must never stall collection, so a
// full queue evicts its oldest entry and reports it in Stats.
//
// The caller's Batch is not retained. Once this returns, the only thing the
// exporter holds is the encoded body.
func (e *HTTPExporter) Export(ctx context.Context, batch *metrics.Batch) error {
	// Checked up front: the enqueue below cannot block, so it could never
	// observe cancellation on its own.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Encoded before the lock: marshal and gzip are pure work on a batch nobody
	// else can see yet, and holding the lock across them would put the whole
	// encode inside Close's window.
	body, err := encodeBody(batch)
	if err != nil {
		// A marshal failure is not transient -- retrying would re-encode the
		// same unencodable batch -- so it is dropped here rather than queued.
		e.dropped.Add(1)
		e.recordError(err)
		slog.Error("export encode failed, dropping batch", "error", err, "batch_seq", batch.BatchSeq)
		return err
	}
	q := queued{
		body:        body,
		seq:         batch.BatchSeq,
		collectedAt: batch.CollectedAt,
		key:         idempotencyKey(batch),
	}

	// Held across the enqueue so Close cannot begin draining while a batch is
	// halfway onto the queue.
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		e.dropped.Add(1)
		e.recordError(ErrClosed)
		return ErrClosed
	}

	select {
	case e.queue <- q:
		return nil
	default:
	}

	// Full: evict the oldest to make room for this one. Safe without further
	// synchronisation because Export is the only producer -- agent.Run calls it
	// synchronously from a single goroutine -- and the worker only ever REMOVES
	// entries, so the slot freed here cannot be taken from under us and the
	// second send cannot block. A racing receive by the worker would merely
	// free a second slot.
	select {
	case stale := <-e.queue:
		e.dropped.Add(1)
		e.recordError(ErrQueueFull)
		slog.Warn("export queue full, dropping oldest batch",
			"queue_size", cap(e.queue), "dropped_batch_seq", stale.seq, "kept_batch_seq", q.seq)
	default:
		// Drained between the two selects; nothing to evict.
	}
	select {
	case e.queue <- q:
		return nil
	default:
		// Unreachable with a single producer, but a lost batch must never be a
		// silent one.
		e.dropped.Add(1)
		e.recordError(ErrQueueFull)
		return ErrQueueFull
	}
}

// Stats returns a snapshot of the exporter's counters.
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
		case q := <-e.queue:
			e.sendWithRetry(q)
		case <-e.done:
			for {
				select {
				case q := <-e.queue:
					e.sendWithRetry(q)
				default:
					return
				}
			}
		}
	}
}

func (e *HTTPExporter) sendWithRetry(q queued) {
	var lastErr error
	var retryAfter time.Duration
	// The budget is wall time from the first attempt, so it bounds the sum of
	// every request and every wait rather than any one of them.
	deadline := time.Now().Add(e.retryBudget)

	for attempt := 0; attempt <= e.maxRetries; attempt++ {
		if attempt > 0 {
			delay := retryAfter
			if delay <= 0 {
				delay = e.backoff(attempt)
			}
			// Abandon rather than sleep past the budget: waiting out a delay we
			// already know outlives it would occupy the worker for nothing.
			if remaining := time.Until(deadline); remaining <= 0 || delay > remaining {
				e.dropped.Add(1)
				e.recordError(fmt.Errorf("export abandoned after %s retry budget: %w", e.retryBudget, lastErr))
				slog.Warn("export abandoned, retry budget exhausted",
					"budget", e.retryBudget, "attempts", attempt, "next_delay", delay,
					"batch_seq", q.seq, "error", lastErr)
				return
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

		err := e.send(q)
		if err == nil {
			e.exported.Add(1)
			slog.Debug("export successful", "collected_at", q.collectedAt, "batch_seq", q.seq)
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

// send posts one already-encoded body. It does no encoding of its own: the body
// is a pure function of the batch, so building it once at enqueue keeps a
// retried batch from being marshalled and gzipped again per attempt.
func (e *HTTPExporter) send(q queued) error {
	data := q.body

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
	req.Header.Set("Idempotency-Key", q.key)
	// Every body this exporter sends is gzipped, so the header is unconditional.
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	// Drain before closing: an unread body makes the transport tear down the
	// connection instead of returning it to the keep-alive pool.
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

// encodeBody marshals the batch and gzips it. Compression is not optional: the
// payload is highly repetitive JSON, so this is an 85-90% reduction on every
// batch, and making it a switch only ever bought an operator a bigger bill.
func encodeBody(batch *metrics.Batch) ([]byte, error) {
	data, err := encodeLine(batch)
	if err != nil {
		return nil, err
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
