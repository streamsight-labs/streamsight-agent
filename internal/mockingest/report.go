package mockingest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Everything a human reads comes out of this file, as plain text on cfg.Out.
// Not JSON: one dense line per batch stays legible at a five-second collection
// interval where a pretty-printed object does not.

// record folds one validated request into the counters and the ring buffer, and
// reports whether the batch must be rejected.
func (s *Server) record(res *result) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	fatal := false
	for _, f := range res.failures {
		s.failures = append(s.failures, f)
		if f.Severity == SeverityFatal {
			fatal = true
		} else {
			s.stats.Warn++
		}
	}

	s.stats.BytesWire += int64(res.wireLen)
	s.stats.BytesDecoded += int64(res.rawLen)
	if res.rawLen > 0 {
		s.sizes = append(s.sizes, res.rawLen)
		if len(s.sizes) > sizeWindow {
			s.sizes = s.sizes[1:]
		}
	}

	if fatal {
		s.stats.Fatal++
		s.sent4xx++
	} else {
		s.stats.OK++
	}

	if len(res.raw) > 0 && s.cfg.Keep > 0 {
		s.bodies = append(s.bodies, json.RawMessage(append([]byte(nil), res.raw...)))
		if len(s.bodies) > s.cfg.Keep {
			s.bodies = s.bodies[1:]
		}
	}
	return fatal
}

// noteAccepted counts a batch this server actually answered 2xx to.
func (s *Server) noteAccepted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Accepted++
	return s.stats.Accepted
}

func (s *Server) noteTerminal(status int) {
	if status < 400 || status >= 500 {
		return
	}
	s.mu.Lock()
	s.sent4xx++
	s.mu.Unlock()
}

func (s *Server) printf(format string, args ...any) {
	fmt.Fprintf(s.cfg.Out, format+"\n", args...)
}

// Banner prints the startup header.
func (s *Server) Banner(addr string) {
	key := "unset (auth check disabled)"
	if s.cfg.APIKey != "" {
		key = "set"
	}
	mode := "normal"
	switch {
	case s.cfg.Strict:
		mode = "strict (every warn is fatal, and a fatal permanently rejects the batch)"
	case s.cfg.WarnOnly:
		mode = "warn-only (nothing is ever rejected)"
	}
	s.printf("mock-ingest listening on %s, ingest POST %s", addr, s.cfg.Path)
	s.printf("  api_key=%s mode=%s require=%d keep=%d max_body=%s sections=%s",
		key, mode, s.cfg.Require, s.cfg.Keep, humanBytes(s.cfg.MaxBodyBytes), strings.Join(s.cfg.Sections, ","))
	if s.cfg.Faults.armed() {
		s.reportFaultsChanged(s.Faults())
	}
}

func (s *Server) reportFaultsChanged(f Faults) {
	b, err := json.Marshal(f)
	if err != nil {
		return
	}
	s.printf("[fault] armed %s", b)
	s.printf("[fault] these failures are DELIBERATE; the agent's export warnings about them are the harness working")
}

func (s *Server) reportFault(n int, f fault) {
	s.printf("[fault] #%d %s", n, f)
}

func (s *Server) reportUnknownRoute(r *http.Request) {
	s.printf("[route] %s %s does not exist; ingest is POST %s (check EXPORT_ENDPOINT)",
		r.Method, r.URL.Path, s.cfg.Path)
}

// report prints one line per batch with warnings indented beneath it, or a
// boxed banner on a conformance failure.
func (s *Server) report(res *result, fatal bool) {
	if res.newInstance && res.instance != "" {
		s.printf("[new ] agent instance %s (a fresh random suffix per boot: this is what a restart looks like)",
			res.instance)
	}

	if fatal {
		s.reportFatal(res)
		return
	}

	s.printf("%s", s.batchLine(res))
	for _, f := range res.failures {
		s.printf("       warn %s", f)
	}
	if res.exportErr != "" {
		s.printf("       info agent.last_export_error = %q", res.exportErr)
	}
}

func (s *Server) batchLine(res *result) string {
	tag := "[ok  ]"
	if res.retry {
		tag = "[retry]"
		if res.afterFault {
			tag = "[retry*]"
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s #%d seq=%d inst=%s", tag, res.n, res.seq, res.instance)

	enc := "raw"
	if res.gzipped {
		enc = "gz"
	}
	ratio := 100.0
	if res.rawLen > 0 {
		ratio = 100 * float64(res.wireLen) / float64(res.rawLen)
	}
	fmt.Fprintf(&b, " %s %s->%s (%.1f%%)", enc,
		humanBytes(int64(res.rawLen)), humanBytes(int64(res.wireLen)), ratio)

	if res.batch != nil {
		parts := 0
		for _, t := range res.batch.Topics {
			parts += len(t.Partitions)
		}
		committed := 0
		for _, co := range res.batch.Offsets {
			committed += len(co.Offsets)
		}
		statuses := make([]string, 0, len(res.batch.Sections))
		for _, sec := range res.batch.Sections {
			statuses = append(statuses, string(sec.Status))
		}
		fmt.Fprintf(&b, " topics=%d/%dp groups=%d offsets=%d sections=%s errors=%d collect=%dms",
			len(res.batch.Topics), parts, len(res.batch.Groups), committed,
			strings.Join(statuses, ","), len(res.batch.Errors), res.batch.CollectionMs)
	}
	return b.String()
}

// reportFatal prints the boxed banner and dumps the offending body. cfg.Out
// only: `docker compose logs` and any `2>&1` merge the streams, so mirroring to
// stderr prints every failure twice and reads like two distinct defects.
func (s *Server) reportFatal(res *result) {
	var b strings.Builder
	b.WriteString("================ CONFORMANCE FAILURE ================\n")
	fmt.Fprintf(&b, "batch #%d seq=%d instance=%s key=%s\n", res.n, res.seq, res.instance, res.key)
	for _, f := range res.failures {
		label := "warn"
		if f.Severity == SeverityFatal {
			label = "FAIL"
		}
		fmt.Fprintf(&b, "  %s %s\n", label, f)
	}
	if path := s.dumpFailure(res); path != "" {
		fmt.Fprintf(&b, "body written to: %s\n", path)
	}
	b.WriteString("=====================================================")

	s.printf("%s", b.String())
}

// dumpFailure writes the decompressed, pretty-printed body so the failure can
// be replayed. Returns the path, or "" when no dump was written.
func (s *Server) dumpFailure(res *result) string {
	if s.cfg.FailDir == "" || len(res.raw) == 0 {
		return ""
	}
	name := fmt.Sprintf("fail-%d-%s.json", res.n, sanitize(res.key))
	path := filepath.Join(s.cfg.FailDir, name)

	var pretty []byte
	var out map[string]any
	if err := json.Unmarshal(res.raw, &out); err == nil {
		pretty, _ = json.MarshalIndent(out, "", "  ")
	}
	if pretty == nil {
		pretty = res.raw
	}
	if err := os.WriteFile(path, append(pretty, '\n'), 0o600); err != nil {
		return ""
	}
	return path
}

func sanitize(s string) string {
	if s == "" {
		return "nokey"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, s)
}

// Summary is the periodic and final one-liner.
func (s *Server) Summary() string {
	st := s.Stats()

	s.mu.Lock()
	sizes := append([]int(nil), s.sizes...)
	hits := make([]string, 0, len(s.faultHits))
	for reason, n := range s.faultHits {
		hits = append(hits, fmt.Sprintf("%s x%d", reason, n))
	}
	s.mu.Unlock()
	sort.Strings(hits)
	sort.Ints(sizes)

	faults := "none"
	if len(hits) > 0 {
		faults = strings.Join(hits, " ")
	}

	ratio := 0.0
	if st.BytesDecoded > 0 {
		ratio = 100 * float64(st.BytesWire) / float64(st.BytesDecoded)
	}

	return fmt.Sprintf(
		"--- summary uptime=%s requests=%d ok=%d accepted=%d fatal=%d warn=%d | retries=%d gaps=%d instances=%d | faults %s | body p50=%s p95=%s max=%s wire=%.1f%%",
		time.Duration(st.UptimeSec)*time.Second, st.Requests, st.OK, st.Accepted, st.Fatal, st.Warn,
		st.Duplicates, st.SeqGaps, st.Instances, faults,
		humanBytes(int64(percentile(sizes, 50))), humanBytes(int64(percentile(sizes, 95))),
		humanBytes(int64(percentile(sizes, 100))), ratio)
}

// PrintSummary writes Summary to cfg.Out. The caller owns the ticker, so this
// package starts no goroutines of its own.
func (s *Server) PrintSummary() { s.printf("%s", s.Summary()) }

// percentile expects sorted input. p is 0..100.
func percentile(sorted []int, p int) int {
	if len(sorted) == 0 {
		return 0
	}
	i := (p * (len(sorted) - 1)) / 100
	return sorted[i]
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
