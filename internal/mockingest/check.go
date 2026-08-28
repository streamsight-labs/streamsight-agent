package mockingest

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strconv"
	"time"

	"kafka-metrics-agent/internal/metrics"
)

// knownErrorKinds mirrors the kind* constants in internal/collector/errors.go.
// They are unexported there, so the mock restates them; a kind added on the
// collector side surfaces as a WARN here rather than passing unnoticed.
var knownErrorKinds = map[string]bool{
	"authorization": true,
	"coordinator":   true,
	"transport":     true,
	"unsupported":   true,
	"other":         true,
}

var validStatuses = map[metrics.SectionStatus]bool{
	metrics.SectionOK:           true,
	metrics.SectionPartial:      true,
	metrics.SectionFailed:       true,
	metrics.SectionUnauthorized: true,
	metrics.SectionSkipped:      true,
}

// result is everything one ingest request produced: what was decoded, what
// failed, and the identity needed to update the per-instance state after the
// fault decision.
type result struct {
	n        int // ingest request ordinal, 1-based
	failures []Failure
	raw      []byte // decompressed body
	batch    *metrics.Batch
	key      string
	instance string
	seq      uint64
	hash     string
	gzipped  bool
	wireLen  int
	rawLen   int

	// newInstance means a boot we have not seen before.
	newInstance bool
	// retry means this Idempotency-Key has arrived before.
	retry bool
	// afterFault means the earlier delivery of this key was one we deliberately
	// destroyed, so the retry is the harness working rather than a defect.
	afterFault bool
	// exportErr is set when the agent's last_export_error changed, which is how
	// an injected fault is seen landing in the agent's own telemetry.
	exportErr string
}

type checker struct {
	s   *Server
	now time.Time
	seq uint64
	out []Failure
}

func (c *checker) emit(sev Severity, status int, code, path, format string, args ...any) {
	if sev == SeverityFatal && c.s.cfg.WarnOnly {
		sev, status = SeverityWarn, 0
	} else if sev == SeverityWarn && c.s.cfg.Strict {
		sev = SeverityFatal
	}
	c.out = append(c.out, Failure{
		Code:     code,
		Severity: sev,
		Message:  fmt.Sprintf(format, args...),
		Path:     path,
		Seq:      c.seq,
		status:   status,
	})
}

func (c *checker) fail(code, path, format string, args ...any) {
	c.emit(SeverityFatal, 0, code, path, format, args...)
}

// failStatus is for the transport-level failures that have a more precise
// answer than 400 — 401, 404, 405, 413, 415.
func (c *checker) failStatus(status int, code, path, format string, args ...any) {
	c.emit(SeverityFatal, status, code, path, format, args...)
}

func (c *checker) warn(code, path, format string, args ...any) {
	c.emit(SeverityWarn, 0, code, path, format, args...)
}

// warnOnce is for conditions that, once true, are true of every batch (an
// unknown section name, generation permanently -1). Emitting them per batch
// would bury the report in noise.
func (c *checker) warnOnce(key, code, path, format string, args ...any) {
	c.s.mu.Lock()
	seen := c.s.warned[key]
	c.s.warned[key] = true
	c.s.mu.Unlock()
	if !seen {
		c.warn(code, path, format, args...)
	}
}

// validate runs the whole conformance suite over one ingest request. It writes
// no response and records only that a key was seen: the commit (acked /
// truncated) happens after the fault decision, because a batch we answered 500
// to was never accepted.
func (s *Server) validate(r *http.Request) *result {
	s.mu.Lock()
	s.stats.Requests++
	res := &result{n: s.stats.Requests}
	s.mu.Unlock()

	c := &checker{s: s, now: s.cfg.Now()}
	defer func() { res.failures = c.out }()

	if !s.checkTransport(c, r, res) {
		return res
	}
	if !s.decodeBody(c, r, res) {
		return res
	}

	c.seq = res.batch.BatchSeq
	res.seq = res.batch.BatchSeq
	res.instance = res.batch.AgentInstanceID

	checkEnvelope(c, res)
	checkSections(c, res.batch)
	checkTruncation(c, res.batch, checkData(c, res.batch))
	checkAgent(c, res.batch)
	s.checkState(c, res)

	return res
}

// checkTransport validates the headers. It reports whether the request is worth
// reading a body from.
func (s *Server) checkTransport(c *checker, r *http.Request, res *result) bool {
	if r.Method != http.MethodPost {
		c.failStatus(http.StatusMethodNotAllowed, "transport.method", "",
			"method %s, want POST", r.Method)
		return false
	}
	if r.URL.Path != s.cfg.Path {
		c.failStatus(http.StatusNotFound, "transport.path", "",
			"path %s, want %s", r.URL.Path, s.cfg.Path)
		return false
	}
	if s.cfg.APIKey != "" && r.Header.Get("X-API-Key") != s.cfg.APIKey {
		c.failStatus(http.StatusUnauthorized, "transport.api_key", "",
			"X-API-Key missing or wrong")
		return false
	}

	ct := r.Header.Get("Content-Type")
	if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/json" {
		// Parameters are tolerated; the agent sends the type bare.
		c.failStatus(http.StatusUnsupportedMediaType, "transport.content_type", "",
			"Content-Type %q, want application/json", ct)
	}

	res.key = r.Header.Get("Idempotency-Key")
	if res.key == "" {
		c.fail("transport.idempotency_key", "", "Idempotency-Key header is absent")
	}

	// The agent builds the body with bytes.NewReader, so Content-Length is
	// always set. Its absence means a streaming body, which changes the retry
	// semantics: a stream cannot be replayed.
	if r.ContentLength < 0 {
		c.warn("transport.content_length", "", "Content-Length is not set")
	}
	return true
}

// decodeBody reads, decompresses and strictly decodes the request body. It
// reports whether res.batch is populated.
func (s *Server) decodeBody(c *checker, r *http.Request, res *result) bool {
	limit := s.cfg.MaxBodyBytes

	wire, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		c.fail("transport.body_read", "", "reading body: %v", err)
		return false
	}
	res.wireLen = len(wire)
	if int64(len(wire)) > limit {
		c.failStatus(http.StatusRequestEntityTooLarge, "transport.body_too_large", "",
			"compressed body exceeds %d bytes", limit)
		return false
	}

	raw := wire
	switch enc := r.Header.Get("Content-Encoding"); enc {
	case "":
		// A gzip magic number under no Content-Encoding means the header and
		// the body disagree, which no ingest can recover from.
		if len(wire) >= 2 && wire[0] == 0x1f && wire[1] == 0x8b {
			c.fail("transport.gzip_undeclared", "",
				"body is gzip but Content-Encoding is absent")
			return false
		}
	case "gzip":
		res.gzipped = true
		zr, err := gzip.NewReader(bytes.NewReader(wire))
		if err != nil {
			c.fail("transport.gzip_decode", "", "gzip header: %v", err)
			return false
		}
		// Bounded: a gzip bomb must not be able to OOM the mock.
		raw, err = io.ReadAll(io.LimitReader(zr, limit+1))
		if err != nil {
			c.fail("transport.gzip_decode", "", "gzip body: %v", err)
			return false
		}
		// Close verifies the CRC32 and ISIZE trailer; skipping it accepts a
		// truncated stream.
		if err := zr.Close(); err != nil {
			c.fail("transport.gzip_decode", "", "gzip trailer: %v", err)
			return false
		}
		if int64(len(raw)) > limit {
			c.failStatus(http.StatusRequestEntityTooLarge, "transport.body_too_large", "",
				"decompressed body exceeds %d bytes", limit)
			return false
		}
		if len(raw) > 0 && float64(len(wire))/float64(len(raw)) > 0.6 {
			c.warn("transport.compression_ratio", "",
				"gzip saved almost nothing (%d/%d bytes); double-compressed?", len(wire), len(raw))
		}
	default:
		c.fail("transport.content_encoding", "", "Content-Encoding %q, want gzip or absent", enc)
		return false
	}

	res.raw = raw
	res.rawLen = len(raw)
	sum := sha256.Sum256(raw)
	res.hash = hex.EncodeToString(sum[:])

	if len(bytes.TrimSpace(raw)) == 0 {
		c.fail("body.empty", "", "body is empty")
		return false
	}
	if bytes.TrimSpace(raw)[0] != '{' {
		c.fail("body.not_object", "", "top-level JSON value is not an object")
		return false
	}

	var batch metrics.Batch
	dec := json.NewDecoder(bytes.NewReader(raw))
	// Strict against hand-written fixtures and an agent newer than this build,
	// but INERT when both are compiled from this tree — see FieldPaths for the
	// check that is not.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&batch); err != nil {
		c.fail("schema.decode", "", "strict decode at byte %d: %v", dec.InputOffset(), err)
		return false
	}
	// The exporter appends exactly one newline (encodeLine), which the decoder
	// treats as whitespace. A second value on the wire means the exporter
	// started batching without saying so.
	if dec.More() {
		c.fail("body.trailing_data", "", "more than one JSON value in the body")
	}

	res.batch = &batch
	return true
}

func checkEnvelope(c *checker, res *result) {
	b := res.batch

	if b.SchemaVersion != metrics.SchemaVersion {
		c.fail("schema.version", "$.schema_version",
			"schema_version %d, want %d", b.SchemaVersion, metrics.SchemaVersion)
	}
	if b.AgentVersion == "" {
		c.fail("envelope.agent_version", "$.agent_version",
			"empty; the build was linked without -X main.version")
	}
	if b.AgentInstanceID == "" {
		c.fail("envelope.instance_id", "$.agent_instance_id", "empty")
	}
	if b.BatchSeq < 1 {
		c.fail("envelope.batch_seq", "$.batch_seq", "batch_seq %d, want >= 1", b.BatchSeq)
	}

	// The exact format the exporter builds (idempotencyKey). Getting it wrong
	// breaks server-side deduplication silently, so it is asserted literally.
	if want := idempotencyKey(b); res.key != "" && res.key != want {
		c.fail("envelope.idempotency_key_format", "",
			"Idempotency-Key %q, want %q (agent_instance_id + \"-\" + batch_seq)", res.key, want)
	}

	if b.CollectedAt.IsZero() {
		c.fail("envelope.collected_at_zero", "$.collected_at", "collected_at is the zero time")
	} else {
		skew := b.CollectedAt.Sub(c.now)
		if skew > c.s.cfg.ClockSkew || -skew > c.s.cfg.ClockSkew {
			c.fail("envelope.collected_at_skew", "$.collected_at",
				"collected_at is %s from server time (limit %s)", skew.Round(time.Second), c.s.cfg.ClockSkew)
		}
	}
	if b.CollectionMs < 0 {
		c.fail("envelope.collection_ms", "$.collection_ms", "negative: %d", b.CollectionMs)
	}
}

func idempotencyKey(b *metrics.Batch) string {
	id := b.AgentInstanceID
	if id == "" {
		id = "unknown"
	}
	return id + "-" + strconv.FormatUint(b.BatchSeq, 10)
}

func checkSections(c *checker, b *metrics.Batch) {
	cfg := c.s.cfg
	canon := make(map[string]int, len(cfg.Sections))
	for i, name := range cfg.Sections {
		canon[name] = i
	}
	optional := make(map[string]bool, len(cfg.OptionalSections))
	for _, name := range cfg.OptionalSections {
		optional[name] = true
	}

	byName := make(map[string]metrics.Section, len(b.Sections))
	prev := -1
	for i, sec := range b.Sections {
		path := fmt.Sprintf("$.sections[%d]", i)

		if sec.Name == "" {
			// finish() on a nil *section produces exactly this: a nameless
			// skipped entry. It means a collection phase never ran.
			c.fail("sections.empty_name", path,
				"empty section name (a nil *section reached finish(), status=%q)", sec.Status)
		} else if _, dup := byName[sec.Name]; dup {
			c.fail("sections.duplicate", path, "section %q appears twice", sec.Name)
		} else {
			byName[sec.Name] = sec
		}

		switch pos, required := canon[sec.Name]; {
		case required:
			// Relative order among the required sections only: an optional or
			// unknown name must not break the ordering of the names whose
			// order carries meaning.
			if pos <= prev {
				c.fail("sections.order", path,
					"section %q is out of order relative to %v", sec.Name, cfg.Sections)
			}
			prev = pos
		case sec.Name == "" || optional[sec.Name]:
		default:
			c.warnOnce("sections.unknown_name/"+sec.Name, "sections.unknown_name", path,
				"section %q is in neither the required set %v nor the optional set %v",
				sec.Name, cfg.Sections, cfg.OptionalSections)
		}

		if !validStatuses[sec.Status] {
			c.fail("sections.status", path, "status %q is not a known SectionStatus", sec.Status)
		}
		if sec.SampledAt.IsZero() {
			c.fail("sections.sampled_at", path, "sampled_at is the zero time")
		}
		if sec.DurationMs < 0 {
			c.fail("sections.duration", path, "duration_ms is negative: %d", sec.DurationMs)
		}
	}

	for _, want := range cfg.Sections {
		if _, ok := byName[want]; !ok {
			c.fail("sections.missing", "$.sections", "required section %q is absent", want)
		}
	}

	topics, hasTopics := byName["topics"]
	topicsEnd, hasEnd := byName["topics_end"]
	offsets, hasOffsets := byName["offsets"]

	// Unconditional: topics_end is constructed after the topics section in
	// every code path, so an inversion is a clock or a plumbing bug.
	if hasTopics && hasEnd && topicsEnd.SampledAt.Before(topics.SampledAt) {
		c.fail("sections.topics_end_before_topics", "$.sections",
			"topics_end sampled at %s, before topics at %s",
			topicsEnd.SampledAt.Format(time.RFC3339Nano), topics.SampledAt.Format(time.RFC3339Nano))
	}

	// The phase-order constraint: end offsets must be listed only after
	// committed offsets have completed, so lag errs high instead of going
	// negative. It has a legitimate escape hatch — the collector abandons the
	// wait on ctx.Done() — so it is gated on the two sections it actually
	// reads carrying data, NOT on the whole batch being pristine.
	if hasEnd && hasOffsets && usable(b, "topics_end", "offsets") {
		done := offsets.SampledAt.Add(time.Duration(offsets.DurationMs) * time.Millisecond)
		if topicsEnd.SampledAt.Before(done) {
			c.warn("sections.phase_order", "$.sections",
				"topics_end sampled at %s, before offsets finished at %s",
				topicsEnd.SampledAt.Format(time.RFC3339Nano), done.Format(time.RFC3339Nano))
		}

		// The LSO belongs between the two, which is what makes
		// committed <= last_stable_offset <= end_offset hold across three
		// separate calls. Its position in sections[] is not asserted — only its
		// sampling instant, which is the part that carries meaning.
		if lso, ok := byName["topics_lso"]; ok && usable(b, "topics_lso") {
			if lso.SampledAt.Before(done) {
				c.warn("sections.lso_before_offsets", "$.sections",
					"topics_lso sampled at %s, before offsets finished at %s",
					lso.SampledAt.Format(time.RFC3339Nano), done.Format(time.RFC3339Nano))
			}
			if topicsEnd.SampledAt.Before(lso.SampledAt) {
				c.warn("sections.lso_after_end", "$.sections",
					"topics_lso sampled at %s, after topics_end at %s",
					lso.SampledAt.Format(time.RFC3339Nano), topicsEnd.SampledAt.Format(time.RFC3339Nano))
			}
		}
	}

	counted := map[string]int{}
	for i, e := range b.Errors {
		path := fmt.Sprintf("$.errors[%d]", i)
		counted[e.Section]++
		if _, ok := byName[e.Section]; !ok {
			c.fail("errors.unknown_section", path,
				"error attributed to section %q, which is not in sections[]", e.Section)
		}
		if e.Kind != "" && !knownErrorKinds[e.Kind] {
			c.warnOnce("errors.kind/"+e.Kind, "errors.kind", path, "unknown error kind %q", e.Kind)
		}
		if e.Message == "" {
			c.fail("errors.message_empty", path, "error has an empty message")
		}
	}
	for name, sec := range byName {
		// Exact in both directions: error_count is "how many entries in
		// batch.errors bear this section's name", with globally dropped entries
		// already subtracted. Occurrences folded by deduplication live in
		// CollectionError.Count and errors_collapsed, not here.
		if got := counted[name]; sec.ErrorCount != got {
			c.fail("sections.error_count", "$.sections",
				"section %q declares error_count=%d but %d errors are attributed to it",
				name, sec.ErrorCount, got)
		}
		if sec.Status == metrics.SectionOK && sec.ErrorCount > 0 {
			c.warn("errors.status_ok_with_errors", "$.sections",
				"section %q is ok with error_count=%d; add() always downgrades to partial",
				name, sec.ErrorCount)
		}
	}

	if len(b.Errors) > c.s.cfg.MaxErrors {
		c.warn("errors.unbounded", "$.errors",
			"%d errors in one batch (threshold %d); Batch.Errors has no cap",
			len(b.Errors), c.s.cfg.MaxErrors)
	}
}

// usable reports whether every named section carried data this cycle. It guards
// the invariants that compare one section's numbers against another's.
//
// It is scoped to named sections rather than asking "is any section degraded".
// An earlier whole-batch version was inert: log_dirs is emitted on every cycle
// and is "skipped" whenever COLLECT_LOG_DIRS is off — the default — so it
// suppressed the negative-lag and phase-order checks on every batch a
// default-configured agent will ever send. A phase switched off by
// configuration must not silence a check that does not read it.
func usable(b *metrics.Batch, names ...string) bool {
	for _, name := range names {
		sec, ok := sectionByName(b, name)
		if !ok {
			return false
		}
		if sec.Status != metrics.SectionOK && sec.Status != metrics.SectionPartial {
			return false
		}
	}
	// A cycle abandoned mid-flight breaks phase ordering legitimately:
	// collectTopics stops waiting on the committed gate once its context is
	// done, so end offsets can genuinely precede committed ones.
	for _, e := range b.Errors {
		if e.Kind == "transport" {
			return false
		}
	}
	return true
}

func sectionByName(b *metrics.Batch, name string) (metrics.Section, bool) {
	for _, sec := range b.Sections {
		if sec.Name == name {
			return sec, true
		}
	}
	return metrics.Section{}, false
}

// countCheck compares a pre-cap count against the list that actually shipped.
// PartitionCount, MemberCount and OffsetCount are the counts BEFORE any cap, so
// `count > len(list)` is how truncation describes itself — but only if the batch
// admits to it: a shortfall with no truncation block is a silent lie about the
// customer's cluster. A count BELOW the list length is impossible either way.
func countCheck(c *checker, b *metrics.Batch, code, path, noun string, count, got int) (short int) {
	switch {
	case count < got:
		c.fail(code, path, "%s=%d but %d shipped; the count is taken before any cap and can never be lower",
			noun, count, got)
	case count > got && b.Truncation == nil:
		c.fail(code, path, "%s=%d but only %d shipped, and the batch declares no truncation",
			noun, count, got)
	case count > got:
		short = count - got
	}
	return short
}

// checkData returns the truncation the batch's own per-entity counts imply, for
// checkTruncation to reconcile against the declared block.
func checkData(c *checker, b *metrics.Batch) metrics.Truncation {
	var implied metrics.Truncation

	// Brokers have no cap, so this one stays an exact equality -- when there is a
	// cluster at all. It is absent on a cycle whose metadata request failed,
	// which sections[cluster].status already reports; asserting a count against
	// an observation that was never made would turn one failure into two.
	if b.Cluster != nil && b.Cluster.BrokerCount != len(b.Cluster.Brokers) {
		c.fail("data.broker_count", "$.cluster",
			"broker_count=%d but %d brokers", b.Cluster.BrokerCount, len(b.Cluster.Brokers))
	}

	// end offsets by topic/partition, for the negative-lag cross-check below.
	ends := map[string]int64{}
	seenTopic := map[string]bool{}
	var prevTopic string
	for i, t := range b.Topics {
		path := fmt.Sprintf("$.topics[%d]", i)
		if seenTopic[t.Name] {
			c.fail("data.duplicate_topic", path, "topic %q appears twice", t.Name)
		}
		seenTopic[t.Name] = true
		if i > 0 && t.Name < prevTopic {
			c.warnOnce("data.sort_order/topics", "data.sort_order", path,
				"topics are not sorted by name (%q after %q); kadm Sorted() should guarantee it",
				t.Name, prevTopic)
		}
		prevTopic = t.Name

		implied.Partitions += countCheck(c, b, "data.partition_count", path,
			"partition_count", t.PartitionCount, len(t.Partitions))

		seenPart := map[int32]bool{}
		minRF := -1
		var prevID int32
		for j, p := range t.Partitions {
			ppath := fmt.Sprintf("%s.partitions[%d]", path, j)
			if seenPart[p.ID] {
				c.fail("data.duplicate_partition", ppath,
					"partition %d of topic %q appears twice", p.ID, t.Name)
			}
			seenPart[p.ID] = true
			if j > 0 && p.ID < prevID {
				c.warnOnce("data.sort_order/partitions", "data.sort_order", ppath,
					"partitions of %q are not sorted by id (%d after %d)", t.Name, p.ID, prevID)
			}
			prevID = p.ID

			if p.StartOffset != nil && *p.StartOffset < 0 {
				c.fail("data.offset_negative", ppath+".start_offset",
					"negative offset %d; a raw -1 escaped nullableOffset", *p.StartOffset)
			}
			if p.EndOffset != nil && *p.EndOffset < 0 {
				c.fail("data.offset_negative", ppath+".end_offset",
					"negative offset %d; a raw -1 escaped nullableOffset", *p.EndOffset)
			}
			if p.LastStableOffset != nil && *p.LastStableOffset < 0 {
				c.fail("data.offset_negative", ppath+".last_stable_offset",
					"negative offset %d; a raw -1 escaped nullableOffset", *p.LastStableOffset)
			}
			if p.StartOffset != nil && p.EndOffset != nil && *p.EndOffset < *p.StartOffset {
				c.fail("data.offset_inverted", ppath,
					"end_offset %d < start_offset %d", *p.EndOffset, *p.StartOffset)
			}
			// start <= lso <= end is what the LSO phase promises, and it
			// survives the skew between the three calls only because the phase
			// order holds. A WARN for the same reason data.negative_lag is: a
			// topic recreated between two samples can produce it innocently.
			if p.LastStableOffset != nil && p.EndOffset != nil && *p.LastStableOffset > *p.EndOffset {
				c.warn("data.lso_above_end_offset", ppath,
					"last_stable_offset %d > end_offset %d; the LSO must be sampled before the high watermark",
					*p.LastStableOffset, *p.EndOffset)
			}
			if p.LastStableOffset != nil && p.StartOffset != nil && *p.LastStableOffset < *p.StartOffset {
				c.warn("data.lso_below_start_offset", ppath,
					"last_stable_offset %d < start_offset %d", *p.LastStableOffset, *p.StartOffset)
			}
			if p.EndOffset != nil {
				ends[tpKey(t.Name, p.ID)] = *p.EndOffset
			}

			if p.ErrorCode == 0 {
				if minRF < 0 || len(p.Replicas) < minRF {
					minRF = len(p.Replicas)
				}
				if !contains(p.Replicas, p.Leader) && p.Leader >= 0 {
					c.warn("data.leader_not_replica", ppath,
						"leader %d is not in replicas %v", p.Leader, p.Replicas)
				}
				for _, id := range p.ISR {
					if !contains(p.Replicas, id) {
						c.warn("data.isr_not_subset", ppath,
							"isr %v is not a subset of replicas %v", p.ISR, p.Replicas)
						break
					}
				}
			}
		}
		// Only when the whole partition list shipped: replication_factor is the
		// minimum over ALL partitions, so a truncated list can only ever give an
		// over-estimate of the minimum and the comparison means nothing.
		if minRF >= 0 && t.PartitionCount == len(t.Partitions) && t.ReplicationFactor != minRF {
			c.warn("data.replication_factor", path,
				"replication_factor=%d but the minimum replica count is %d",
				t.ReplicationFactor, minRF)
		}
	}

	seenGroup := map[string]bool{}
	var prevGroup string
	allMinusOne := len(b.Groups) > 0
	for i, g := range b.Groups {
		path := fmt.Sprintf("$.groups[%d]", i)
		if seenGroup[g.ID] {
			c.fail("data.duplicate_group", path, "group %q appears twice", g.ID)
		}
		seenGroup[g.ID] = true
		if i > 0 && g.ID < prevGroup {
			c.warnOnce("data.sort_order/groups", "data.sort_order", path,
				"groups are not sorted by id (%q after %q)", g.ID, prevGroup)
		}
		prevGroup = g.ID
		implied.Members += countCheck(c, b, "data.member_count", path,
			"member_count", g.MemberCount, len(g.Members))
		if g.Generation != -1 {
			allMinusOne = false
		}
	}
	if allMinusOne {
		// Generation comes from the sticky assignor's hint, which range and
		// cooperative-sticky leave at -1. The WARN exists so nobody builds
		// rebalance alerting on a field that never moves.
		c.warnOnce("data.generation_absent", "data.generation_absent", "$.groups",
			"every group reports generation=-1; it is not the authoritative generation")
	}

	var prevGID string
	seenOffset := map[string]bool{}
	for i, co := range b.Offsets {
		path := fmt.Sprintf("$.offsets[%d]", i)
		if i > 0 && co.GroupID < prevGID {
			c.warnOnce("data.sort_order/offsets", "data.sort_order", path,
				"offsets are not sorted by group_id (%q after %q)", co.GroupID, prevGID)
		}
		prevGID = co.GroupID
		implied.Offsets += countCheck(c, b, "data.offset_count", path,
			"offset_count", co.OffsetCount, len(co.Offsets))

		for j, po := range co.Offsets {
			opath := fmt.Sprintf("%s.offsets[%d]", path, j)
			key := co.GroupID + "\x00" + tpKey(po.Topic, po.Partition)
			if seenOffset[key] {
				c.fail("data.duplicate_offset", opath,
					"(%s, %s/%d) appears twice", co.GroupID, po.Topic, po.Partition)
			}
			seenOffset[key] = true

			if po.Offset == nil {
				continue
			}
			if *po.Offset < 0 {
				c.fail("data.offset_negative", opath,
					"negative committed offset %d; -1 must be null, not clamped", *po.Offset)
				continue
			}
			end, ok := ends[tpKey(po.Topic, po.Partition)]
			if !ok {
				if !seenTopic[po.Topic] {
					// Legal when the topic and group filters diverge.
					c.warnOnce("data.orphan_offset/"+po.Topic, "data.orphan_offset", opath,
						"group %q has offsets for %q, which no topic in this batch declares",
						co.GroupID, po.Topic)
				}
				continue
			}
			// THE check this harness exists for: committed above the high
			// watermark is negative lag, which the collector's phase order
			// (committed sampled before end offsets) prevents. If this fires,
			// that ordering regressed.
			if *po.Offset > end && usable(b, "topics_end", "offsets") {
				c.warn("data.negative_lag", opath,
					"%s %s/%d committed=%d > end_offset=%d",
					co.GroupID, po.Topic, po.Partition, *po.Offset, end)
			}
		}
	}

	checkLogDirs(c, b)
	return implied
}

// checkLogDirs validates the per-broker storage view. Its rows are per REPLICA,
// not per partition, so the same (topic, partition) legitimately appears once
// per broker that hosts it — the duplicate check is therefore scoped to one
// directory, not to the batch.
func checkLogDirs(c *checker, b *metrics.Batch) {
	seenDir := map[string]bool{}
	for i, d := range b.LogDirs {
		path := fmt.Sprintf("$.log_dirs[%d]", i)

		key := strconv.FormatInt(int64(d.Broker), 10) + "\x00" + d.Dir
		if seenDir[key] {
			c.fail("data.duplicate_log_dir", path,
				"directory %q on broker %d appears twice", d.Dir, d.Broker)
		}
		seenDir[key] = true
		if d.Dir == "" {
			c.fail("data.log_dir_unnamed", path, "log directory has no path")
		}

		if d.TotalBytes != nil && *d.TotalBytes < 0 {
			c.fail("data.log_dir_negative", path+".total_bytes", "negative: %d", *d.TotalBytes)
		}
		if d.UsableBytes != nil && *d.UsableBytes < 0 {
			c.fail("data.log_dir_negative", path+".usable_bytes", "negative: %d", *d.UsableBytes)
		}
		if d.TotalBytes != nil && d.UsableBytes != nil && *d.UsableBytes > *d.TotalBytes {
			c.warn("data.log_dir_usable_above_total", path,
				"usable_bytes %d exceeds total_bytes %d", *d.UsableBytes, *d.TotalBytes)
		}

		seenTP := map[string]bool{}
		for j, p := range d.Partitions {
			ppath := fmt.Sprintf("%s.partitions[%d]", path, j)
			tp := tpKey(p.Topic, p.Partition)
			if seenTP[tp] {
				c.fail("data.duplicate_log_dir_partition", ppath,
					"%s appears twice in %q on broker %d", tp, d.Dir, d.Broker)
			}
			seenTP[tp] = true
			if p.Size < 0 {
				c.fail("data.log_dir_negative", ppath+".size", "negative: %d", p.Size)
			}
			if p.OffsetLag < 0 {
				c.fail("data.log_dir_negative", ppath+".offset_lag", "negative: %d", p.OffsetLag)
			}
		}
	}
}

// checkTruncation reconciles the declared truncation and limits blocks against
// what the per-entity counts imply. Presence of the block is the contract: "nil
// means complete" is only worth something if a short list can never appear
// without it.
func checkTruncation(c *checker, b *metrics.Batch, implied metrics.Truncation) {
	for i, e := range b.Errors {
		if e.Count < 0 {
			c.fail("errors.negative_count", fmt.Sprintf("$.errors[%d]", i),
				"count is negative: %d", e.Count)
		}
	}

	if b.Limits != nil && b.Limits.MaxErrors > 0 && len(b.Errors) > b.Limits.MaxErrors {
		c.fail("errors.cap_exceeded", "$.errors",
			"%d errors on the wire but the batch declares max_errors=%d",
			len(b.Errors), b.Limits.MaxErrors)
	}

	t := b.Truncation
	if t == nil {
		for i, sec := range b.Sections {
			if sec.Truncated {
				c.fail("truncation.section_unaccounted", fmt.Sprintf("$.sections[%d]", i),
					"section %q is marked truncated but the batch declares no truncation block",
					sec.Name)
			}
		}
		return
	}

	if *t == (metrics.Truncation{}) {
		c.fail("truncation.empty", "$.truncation",
			"truncation block is present with every counter zero; its presence is what means "+
				"the batch is incomplete, so an empty one makes a complete batch look short")
	}

	entities := t.Topics + t.Partitions + t.Groups + t.Members + t.Offsets
	if entities > 0 && b.Limits == nil {
		c.fail("truncation.no_limits", "$.limits",
			"%d entities were truncated but no limits block says which cap did it", entities)
	}

	// Only under-reporting is an error. Dropping a whole topic also drops its
	// partitions, and those are not visible as a per-topic shortfall, so the
	// declared count is legitimately the larger of the two.
	if t.Partitions < implied.Partitions {
		c.fail("truncation.partitions", "$.truncation",
			"declares %d dropped partitions but the per-topic counts imply at least %d",
			t.Partitions, implied.Partitions)
	}
	if t.Members < implied.Members {
		c.fail("truncation.members", "$.truncation",
			"declares %d dropped members but the per-group counts imply at least %d",
			t.Members, implied.Members)
	}
	if t.Offsets < implied.Offsets {
		c.fail("truncation.offsets", "$.truncation",
			"declares %d dropped offsets but the per-group counts imply at least %d",
			t.Offsets, implied.Offsets)
	}

	var secCollapsed, secDropped int
	for _, sec := range b.Sections {
		secCollapsed += sec.ErrorsCollapsed
		secDropped += sec.ErrorsDropped
	}
	if t.ErrorsCollapsed != secCollapsed {
		c.fail("truncation.errors_collapsed", "$.truncation",
			"batch declares %d collapsed errors but the sections sum to %d",
			t.ErrorsCollapsed, secCollapsed)
	}
	if t.ErrorsDropped != secDropped {
		c.fail("truncation.errors_dropped", "$.truncation",
			"batch declares %d dropped errors but the sections sum to %d",
			t.ErrorsDropped, secDropped)
	}
}

func checkAgent(c *checker, b *metrics.Batch) {
	a := b.Agent
	if a.BatchesCollected != b.BatchSeq {
		c.fail("agent.batches_collected", "$.agent",
			"batches_collected=%d but batch_seq=%d; they are the same counter",
			a.BatchesCollected, b.BatchSeq)
	}
	if a.BatchesExported >= b.BatchSeq {
		c.fail("agent.batches_exported", "$.agent",
			"batches_exported=%d with batch_seq=%d; this batch cannot already be exported",
			a.BatchesExported, b.BatchSeq)
	}
	if a.BatchesExported+a.BatchesDropped+a.BatchesRejected > a.BatchesCollected {
		c.fail("agent.counter_sum", "$.agent",
			"exported+dropped+rejected=%d exceeds collected=%d",
			a.BatchesExported+a.BatchesDropped+a.BatchesRejected, a.BatchesCollected)
	}
	if a.QueueDepth < 0 {
		c.fail("agent.queue_depth", "$.agent", "queue_depth is negative: %d", a.QueueDepth)
	} else if a.QueueDepth > 0 {
		c.warn("agent.queue_depth_positive", "$.agent",
			"queue_depth=%d; expected only while a fault is slowing this server down", a.QueueDepth)
	}
}

// checkState runs the checks that need memory of earlier batches and records
// this delivery, under the lock throughout so a concurrent /control call cannot
// interleave.
func (s *Server) checkState(c *checker, res *result) {
	if res.instance == "" || res.key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	inst, known := s.instances[res.instance]
	if !known {
		inst = &instState{seen: map[string]*delivery{}}
		s.instances[res.instance] = inst
		res.newInstance = true
	}

	if d, dup := inst.seen[res.key]; dup {
		d.count++
		s.stats.Duplicates++
		switch {
		case d.hash != res.hash:
			c.fail("idempotency.body_mismatch", "",
				"Idempotency-Key %q redelivered with a different body (sha256 %s, was %s)",
				res.key, short(res.hash), short(d.hash))
		case d.acked:
			// Only a delivery we acknowledged makes a redelivery a violation. A
			// retry after a 500, a dropped connection or a client timeout is
			// exactly what the key is for.
			c.fail("idempotency.replay", "",
				"Idempotency-Key %q was already acknowledged; redelivering it duplicates data", res.key)
		}
		res.retry = true
		res.afterFault = d.truncated
	} else {
		inst.seen[res.key] = &delivery{hash: res.hash, seq: res.seq, count: 1}
	}

	switch {
	case res.retry:
		// A retry of a batch we never acked; the sequence checks below would
		// misread it as a regression.
	case res.seq <= inst.lastAckedSeq:
		c.fail("idempotency.seq_regression", "$.batch_seq",
			"batch_seq %d is not greater than the last acknowledged %d, and the key is new",
			res.seq, inst.lastAckedSeq)
	case inst.lastSeenSeq > 0 && res.seq > inst.lastSeenSeq+1:
		gap := clampToInt(res.seq - inst.lastSeenSeq - 1)
		s.stats.SeqGaps += gap
		c.warn("idempotency.seq_gap", "$.batch_seq",
			"batch_seq jumped from %d to %d (%d missing)", inst.lastSeenSeq, res.seq, gap)
		// The agent's own drop counter should account for exactly that gap.
		// The subtraction is unsigned, so a counter that went BACKWARDS would
		// wrap to a nonsense delta instead of surfacing as the anomaly it is.
		// These counters are monotonic within one agent_instance_id, so a
		// decrease means two agents are reporting under one identity.
		if inst.haveLastAgent {
			if res.batch.Agent.BatchesDropped < inst.lastAgent.BatchesDropped {
				c.warn("agent.counter_regression", "$.agent",
					"batches_dropped went backwards, %d to %d, under a single agent_instance_id",
					inst.lastAgent.BatchesDropped, res.batch.Agent.BatchesDropped)
				return
			}
			dropped := clampToInt(res.batch.Agent.BatchesDropped - inst.lastAgent.BatchesDropped)
			if dropped != gap {
				c.warn("idempotency.drop_accounting", "$.agent",
					"batch_seq gap of %d but batches_dropped rose by %d", gap, dropped)
			}
		}
	}

	if inst.haveLastAgent {
		rejected := res.batch.Agent.BatchesRejected - inst.lastAgent.BatchesRejected
		// s.sent4xx is the count BEFORE this batch was recorded, and
		// inst.sent4xxAtLast is the same count as of the previous batch: an
		// unchanged pair means nothing this server did can explain the rise.
		if rejected > 0 && s.sent4xx == inst.sent4xxAtLast {
			c.warn("agent.rejected_unexplained", "$.agent",
				"batches_rejected rose by %d but this server returned no terminal 4xx", rejected)
		}
		if res.batch.Agent.LastExportError != inst.lastAgent.LastExportError {
			res.exportErr = res.batch.Agent.LastExportError
		}
	} else if res.batch.Agent.LastExportError != "" {
		res.exportErr = res.batch.Agent.LastExportError
	}

	if res.seq > inst.lastSeenSeq {
		inst.lastSeenSeq = res.seq
	}
	inst.lastAgent = res.batch.Agent
	inst.haveLastAgent = true
	inst.sent4xxAtLast = s.sent4xx
}

// markAcked commits a delivery we actually answered 2xx to. Only after this is
// a redelivery of the same key a protocol violation.
func (s *Server) markAcked(res *result) {
	if res.instance == "" || res.key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	inst, ok := s.instances[res.instance]
	if !ok {
		return
	}
	if d, ok := inst.seen[res.key]; ok {
		d.acked = true
	}
	if res.seq > inst.lastAckedSeq {
		inst.lastAckedSeq = res.seq
	}
}

// markTruncated records that we deliberately destroyed this delivery, so the
// retry it provokes is not reported as a replay.
func (s *Server) markTruncated(res *result) {
	if res.instance == "" || res.key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst, ok := s.instances[res.instance]; ok {
		if d, ok := inst.seen[res.key]; ok {
			d.truncated = true
		}
	}
}

func tpKey(topic string, partition int32) string {
	return topic + "/" + strconv.FormatInt(int64(partition), 10)
}

func contains(ids []int32, id int32) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func short(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}

// clampToInt narrows a uint64 counter delta for reporting. These deltas are
// sequence gaps and drop counts, so a value near the ceiling means a corrupt
// counter; saturating says that better than a silently wrapped negative.
func clampToInt(v uint64) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}
