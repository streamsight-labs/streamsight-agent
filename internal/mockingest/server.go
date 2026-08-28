// Package mockingest is a conformance-checking stand-in for the batch ingest
// endpoint, so EXPORT_MODE=http can be exercised end to end without a deployed
// backend. It is not a stub: it stores nothing and VALIDATES everything. An
// accept-and-200 stub proves only that a socket is open, whereas every check in
// check.go maps to an invariant the collector or the exporter promises, and
// several of them (committed <= end_offset, topics_end sampled after topics, the
// Idempotency-Key format, batch_seq monotonicity) are this repo's only runtime
// regression detectors for the correctness fixes that motivated them. Handler()
// returns a plain http.Handler, so the compose demo (cmd/mock-ingest) and the
// hermetic httptest tests in CI drive the identical validator.
//
// There is no build tag on purpose. Dockerfile and release.yml both build
// ./cmd/agent by explicit path, so this second main package can never enter a
// release artifact; a tag would only hide it from `go build ./...` and let it
// rot.
package mockingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"kafka-metrics-agent/internal/metrics"
)

const (
	// DefaultPath is the ingest route the agent is pointed at everywhere in
	// this repo (README, deploy/k8s/configmap.yaml, ci.yml).
	DefaultPath = "/v1/batches"

	defaultKeep         = 20
	defaultMaxBodyBytes = 32 << 20
	defaultClockSkew    = 5 * time.Minute
	defaultMaxErrors    = 1000

	// sizeWindow bounds the body-size sample used for the summary percentiles.
	// A demo left running for a day must not accumulate a size per batch
	// forever.
	sizeWindow = 512
)

// DefaultSections is the ordered set of sections every batch must carry, and the
// only one whose relative order is asserted. That order is load-bearing:
// committed offsets are sampled before high watermarks so lag cannot go
// negative, the LSO has to land between the two for read_committed lag to stay
// non-negative, and the throughput window has to precede the high watermarks or
// the record count it is the lower edge of comes out negative.
//
// All of them are REQUIRED, including the phases that are off by default or fire
// only on a trigger: the collector emits every section on every cycle and marks
// one it did not run as "skipped", so an absent section is a defect rather than
// a configuration, and demoting them would forfeit the ordering assertion.
//
// DefaultOptionalSections holds names that may be absent and whose position is
// not asserted. Both are Config fields rather than constants because new
// sections keep arriving and an ingest may be running against an agent either
// side of one; an unknown name warns once instead of failing.
//
// DefaultSections is still spelled out here rather than taken from
// collector.SectionNames() at run time, so the server keeps no Kafka client in
// its import graph — but it is no longer unbound: sections_test.go asserts the
// two are equal element for element. It drifted once without that assertion.
var (
	DefaultSections = []string{
		"cluster",
		"topics", "topics_window", "topics_lso", "topics_end",
		"topics_max_timestamp", "topics_local_start", "topics_remote_end",
		"groups", "offsets",
		"epoch_probes",
		"log_dirs", "reassignments", "topic_configs", "broker_configs",
		"share_groups", "broker_rpc",
	}
	DefaultOptionalSections = []string{}
)

// Config configures a Server. The zero value is usable except for Path, which
// New defaults to DefaultPath.
type Config struct {
	// Path is the ingest route; it must not collide with a control route.
	Path string
	// APIKey is compared against X-API-Key. Empty disables the check.
	APIKey string

	// Strict promotes every WARN to FATAL. CI only: a FATAL answers 400, which
	// the exporter treats as terminal, so under --strict a heuristic WARN
	// permanently destroys a batch.
	Strict bool
	// WarnOnly demotes every FATAL to WARN and always answers 2xx: the report is
	// still printed, but no batch is ever rejected.
	WarnOnly bool

	// Require > 0 closes Done() after that many conforming batches, which is
	// how the compose gate turns conformance into a process exit code.
	Require int
	// Keep is the size of the raw-body ring buffer served by GET /batches.
	Keep int

	// MaxBodyBytes caps the DECOMPRESSED body. The body is read through an
	// io.LimitReader so a gzip bomb cannot take the mock down.
	MaxBodyBytes int64
	// ClockSkew is how far collected_at may sit from the server's clock.
	ClockSkew time.Duration
	// MaxErrors is the WARN threshold on len(batch.errors). Batch.Errors is
	// unbounded in the agent today; this is what makes that visible.
	MaxErrors int
	// FailDir receives the decompressed, pretty-printed body of every batch
	// that fails conformance. Empty disables the dump.
	FailDir string

	// Sections is the ordered set of required section names.
	Sections []string
	// OptionalSections are known names that need not be present and whose
	// position is not asserted.
	OptionalSections []string

	Faults Faults

	Out io.Writer
	// Now is injectable so the clock-skew checks are testable.
	Now func() time.Time
}

// Severity is how bad a Failure is.
type Severity string

// SeverityFatal answers 400 and ends a --require run; SeverityWarn is printed
// and counted but the batch is still accepted.
const (
	SeverityFatal Severity = "fatal"
	SeverityWarn  Severity = "warn"
)

// Failure is one conformance violation. Code is a stable dotted identifier so
// an operator can grep this package for the exact assertion that fired.
type Failure struct {
	Code     string   `json:"code"`
	Severity Severity `json:"severity"`
	Message  string   `json:"message"`
	// Path is the JSON path the failure is anchored to, when there is one.
	Path string `json:"path,omitempty"`
	Seq  uint64 `json:"batch_seq,omitempty"`

	// status is the HTTP status this failure answers with. 0 means 400. It is
	// unexported because it is a transport detail, not part of the report.
	status int
}

func (f Failure) String() string {
	if f.Path != "" {
		return f.Code + " " + f.Path + ": " + f.Message
	}
	return f.Code + " " + f.Message
}

// Stats is the run summary, also served as JSON on GET /stats.
type Stats struct {
	UptimeSec int64 `json:"uptime_sec"`
	Requests  int   `json:"requests"`
	// OK is batches that passed every FATAL check. Accepted is the subset this
	// server answered 2xx to — a conforming batch that then got an injected 500
	// counts in the first only, and Require is measured against Accepted.
	OK             int   `json:"ok"`
	Accepted       int   `json:"accepted"`
	Fatal          int   `json:"fatal"`
	Warn           int   `json:"warn"`
	Duplicates     int   `json:"duplicates"`
	SeqGaps        int   `json:"seq_gaps"`
	FaultsInjected int   `json:"faults_injected"`
	Instances      int   `json:"instances"`
	BytesWire      int64 `json:"bytes_wire"`
	BytesDecoded   int64 `json:"bytes_decoded"`
}

// delivery is one Idempotency-Key we have seen.
type delivery struct {
	hash string
	seq  uint64
	// acked means we answered 2xx and the client had a chance to read it. Only
	// then is a redelivery a protocol violation.
	acked bool
	// truncated means we deliberately destroyed this delivery (dropped the
	// connection, or the client timed out on an injected delay), so the retry
	// that follows is correct behaviour and must not be flagged.
	truncated bool
	count     int
}

// instState is per agent_instance_id. config.Load mints a new random suffix on
// every boot, so an unseen id after a restart is normal rather than an anomaly.
type instState struct {
	lastAckedSeq uint64
	lastSeenSeq  uint64
	seen         map[string]*delivery
	// lastAgent is the previous batch's self-telemetry, for the cross-checks
	// that are deltas rather than absolutes.
	lastAgent     metrics.AgentStats
	haveLastAgent bool
	// sent4xxAtLast is the server-wide terminal-4xx count as of this instance's
	// previous batch, so a rise in the agent's own batches_rejected can be
	// attributed to something this server actually did.
	sent4xxAtLast int
}

// Server is the conformance-checking mock ingest.
type Server struct {
	cfg   Config
	start time.Time

	mu            sync.Mutex
	faults        Faults
	terminalArmed bool
	stats         Stats
	failures      []Failure
	bodies        []json.RawMessage
	instances     map[string]*instState
	warned        map[string]bool
	faultHits     map[string]int
	sizes         []int
	// sent4xx counts every terminal response this server produced, from a
	// conformance failure or an injected fault alike.
	sent4xx int

	doneOnce sync.Once
	done     chan struct{}

	handler http.Handler
}

// New builds a Server. It fails only on a configuration that could not work at
// all: an ingest path that is unrooted or shadows a control route.
func New(cfg Config) (*Server, error) {
	if cfg.Path == "" {
		cfg.Path = DefaultPath
	}
	if cfg.Path[0] != '/' {
		return nil, fmt.Errorf("mockingest: path %q must start with /", cfg.Path)
	}
	for _, reserved := range []string{"/healthz", "/stats", "/batches", "/control", "/control/reset"} {
		if cfg.Path == reserved {
			return nil, fmt.Errorf("mockingest: path %q collides with a control route", cfg.Path)
		}
	}
	if cfg.Strict && cfg.WarnOnly {
		return nil, errors.New("mockingest: strict and warn-only are mutually exclusive")
	}
	if cfg.Keep <= 0 {
		cfg.Keep = defaultKeep
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultMaxBodyBytes
	}
	if cfg.ClockSkew <= 0 {
		cfg.ClockSkew = defaultClockSkew
	}
	if cfg.MaxErrors <= 0 {
		cfg.MaxErrors = defaultMaxErrors
	}
	if len(cfg.Sections) == 0 {
		cfg.Sections = DefaultSections
	}
	if cfg.OptionalSections == nil {
		cfg.OptionalSections = DefaultOptionalSections
	}
	if cfg.Out == nil {
		cfg.Out = os.Stdout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	s := &Server{
		cfg:           cfg,
		start:         cfg.Now(),
		faults:        cfg.Faults.withDefaults(),
		terminalArmed: cfg.Faults.TerminalOnce > 0,
		instances:     map[string]*instState{},
		warned:        map[string]bool{},
		faultHits:     map[string]int{},
		done:          make(chan struct{}),
	}

	mux := http.NewServeMux()
	// Method and path are checked inside the handlers rather than with Go 1.22
	// method patterns: a wrong method or path is itself a conformance failure
	// worth printing loudly instead of silently 405ing.
	mux.HandleFunc(cfg.Path, s.handleIngest)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/batches", s.handleBatches)
	mux.HandleFunc("/control", s.handleControl)
	mux.HandleFunc("/control/reset", s.handleReset)
	mux.HandleFunc("/", s.handleUnknown)
	s.handler = mux

	return s, nil
}

// Handler returns the whole server as an http.Handler.
func (s *Server) Handler() http.Handler { return s.handler }

// Done is closed once Require conforming batches have arrived, or on the first
// FATAL. A Require of 0 never closes it on success, which is demo mode.
func (s *Server) Done() <-chan struct{} { return s.done }

// Stats returns a snapshot of the run summary.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.UptimeSec = int64(s.cfg.Now().Sub(s.start).Seconds())
	st.Instances = len(s.instances)
	return st
}

// Failures returns every FATAL and WARN recorded so far, in arrival order.
func (s *Server) Failures() []Failure {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Failure(nil), s.failures...)
}

// Batches returns the last Keep decompressed bodies, oldest first.
func (s *Server) Batches() []json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]json.RawMessage(nil), s.bodies...)
}

// SetFaults replaces the fault configuration. It re-arms TerminalOnce, so
// posting the same body twice injects two terminal responses.
func (s *Server) SetFaults(f Faults) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = f.withDefaults()
	s.terminalArmed = f.TerminalOnce > 0
}

// Faults returns the currently armed fault-injection settings.
func (s *Server) Faults() Faults {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faults
}

// Reset zeroes the counters, the per-instance state and the faults, so one
// long-lived mock can host several scenarios.
func (s *Server) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats = Stats{}
	s.failures = nil
	s.bodies = nil
	s.instances = map[string]*instState{}
	s.warned = map[string]bool{}
	s.faultHits = map[string]int{}
	s.sizes = nil
	s.sent4xx = 0
	s.faults = Faults{}.withDefaults()
	s.terminalArmed = false
	s.start = s.cfg.Now()
}

func (s *Server) finish() {
	s.doneOnce.Do(func() { close(s.done) })
}

// handleIngest is the whole request lifecycle: validate, record, report, then
// inject. Faults are applied AFTER validation on purpose — a forced 500 must
// still conformance-check the batch it is about to reject, or the fault modes
// would be blind spots.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	res := s.validate(r)
	defer r.Body.Close()

	fatal := s.record(res)
	s.report(res, fatal)

	if fatal {
		s.writeFailures(w, res)
		// A conformance failure ends a --require run immediately. Demo mode
		// keeps serving: taking the endpoint away would turn every later export
		// into a transport error and bury the actual finding.
		if s.cfg.Require > 0 {
			s.finish()
		}
		return
	}

	s.mu.Lock()
	f := s.decide(res.n)
	s.mu.Unlock()

	if f.active() {
		s.reportFault(res.n, f)
	}
	if f.delay > 0 && !sleepCtx(r.Context(), f.delay) {
		// The client's timeout fired first, so nothing we write can be read:
		// this delivery never landed and its retry is not a replay.
		s.markTruncated(res)
		return
	}

	switch f.kind {
	case faultDrop:
		s.markTruncated(res)
		if err := drop(w); err != nil {
			// Hijack unavailable (HTTP/2). Fall back to a transport-ish status
			// rather than answering 200 and lying about the injected fault.
			http.Error(w, "drop unavailable", http.StatusBadGateway)
		}
		return
	case faultStatus:
		if f.retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(f.retryAfter))
		}
		s.noteTerminal(f.status)
		http.Error(w, "injected fault "+f.reason, f.status)
		return
	}

	s.markAcked(res)
	s.writeAccepted(w, res)

	if n := s.noteAccepted(); s.cfg.Require > 0 && n >= s.cfg.Require {
		s.finish()
	}
}

func (s *Server) writeAccepted(w http.ResponseWriter, res *result) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":    "accepted",
		"batch_seq": res.seq,
	})
}

// writeFailures answers a conformance failure. The body MUST stay under
// export.errBodyLimit (4<<10): the exporter reads exactly that much of an error
// response into httpError.Body, so anything longer is silently clipped in the
// agent's log and the operator sees a half-sentence.
func (s *Server) writeFailures(w http.ResponseWriter, res *result) {
	status := http.StatusBadRequest
	for _, f := range res.failures {
		if f.Severity == SeverityFatal && f.status != 0 {
			status = f.status
			break
		}
	}

	body := conformanceBody(res.seq, res.failures)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

const errBodyLimit = 4 << 10

func conformanceBody(seq uint64, failures []Failure) []byte {
	type wire struct {
		Error     string    `json:"error"`
		BatchSeq  uint64    `json:"batch_seq,omitempty"`
		Failures  []Failure `json:"failures"`
		Truncated int       `json:"truncated_failures,omitempty"`
	}

	fatal := make([]Failure, 0, len(failures))
	for _, f := range failures {
		if f.Severity == SeverityFatal {
			fatal = append(fatal, f)
		}
	}

	// Drop from the tail until it fits. The first failures are the ones that
	// explain the rest, so they are the ones worth keeping.
	for n := len(fatal); ; n-- {
		w := wire{Error: "conformance", BatchSeq: seq, Failures: fatal[:n], Truncated: len(fatal) - n}
		b, err := json.Marshal(w)
		if err != nil {
			return []byte(`{"error":"conformance"}`)
		}
		if len(b) <= errBodyLimit-64 || n == 0 {
			return b
		}
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = io.WriteString(w, "ok\n")
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Stats())
}

// handleBatches serves the ring buffer, newest last. ?n=N returns only the last
// N.
func (s *Server) handleBatches(w http.ResponseWriter, r *http.Request) {
	bodies := s.Batches()
	if v := r.URL.Query().Get("n"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "n must be a non-negative integer"})
			return
		}
		if n < len(bodies) {
			bodies = bodies[len(bodies)-n:]
		}
	}
	writeJSON(w, http.StatusOK, bodies)
}

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var f Faults
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&f); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		s.SetFaults(f)
		s.reportFaultsChanged(s.Faults())
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"faults": s.Faults(),
		"stats":  s.Stats(),
	})
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.Reset()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

// handleUnknown is the catch-all. A request that lands here is a
// misconfiguration, usually EXPORT_ENDPOINT pointing at the wrong path.
func (s *Server) handleUnknown(w http.ResponseWriter, r *http.Request) {
	s.reportUnknownRoute(r)
	writeJSON(w, http.StatusNotFound, map[string]string{
		"error": fmt.Sprintf("no route %s %s; ingest is POST %s", r.Method, r.URL.Path, s.cfg.Path),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
