package mockingest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"kafka-metrics-agent/internal/metrics"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// base is a fixed instant so the fixtures are reproducible. The server's clock
// is pinned to it too, which is what makes the collected_at skew checks
// testable.
var base = time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

func i64(v int64) *int64 { return &v }

// validBatch is a batch that must produce ZERO failures, warnings included.
// Every subtest below mutates exactly one thing in it, so a check that fires
// spuriously surfaces as the valid case failing.
func validBatch(seq uint64) *metrics.Batch {
	ms := func(n int) time.Time { return base.Add(time.Duration(n) * time.Millisecond) }

	// broker_rpc is on by default and costs no request, so a realistic batch
	// carries one window of it. Built through Observe rather than by hand: the
	// bucket layout is fixed so that counts add across agents, and a fixture that
	// invented its own would not be the wire format.
	rpc := metrics.BrokerAPIRPC{API: "metadata", BytesWritten: 120, BytesRead: 4096}
	rpc.E2E.Observe(3 * time.Millisecond)
	rpc.WriteWait.Observe(80 * time.Microsecond)

	return &metrics.Batch{
		SchemaVersion:   metrics.SchemaVersion,
		AgentVersion:    "v0.1.0-test",
		AgentInstanceID: "streamsight-agent-3f2a1b09",
		BatchSeq:        seq,
		CollectedAt:     base,
		CollectionMs:    41,

		Cluster: metrics.ClusterMetrics{
			ID:          "MkU3OEVBNTcwNTJENDM2Qk",
			Controller:  1,
			BrokerCount: 1,
			Brokers:     []metrics.Broker{{ID: 1, Host: "kafka", Port: 9092}},
		},
		Topics: []metrics.TopicMetrics{{
			Name:              "orders",
			PartitionCount:    2,
			ReplicationFactor: 1,
			Partitions: []metrics.Partition{
				{ID: 0, Leader: 1, LeaderEpoch: 3, Replicas: []int32{1}, ISR: []int32{1},
					StartOffset: i64(0), EndOffset: i64(100)},
				{ID: 1, Leader: 1, LeaderEpoch: 3, Replicas: []int32{1}, ISR: []int32{1},
					StartOffset: i64(0), EndOffset: i64(50)},
			},
		}},
		Groups: []metrics.GroupMetrics{{
			ID: "order-processor", State: "Stable", Coordinator: 1, Generation: 7,
			Protocol: "range", ProtocolType: "consumer", MemberCount: 1,
			Members: []metrics.GroupMember{{
				MemberID: "m-1", ClientID: "c-1", Host: "/10.0.0.1",
				Assignment: []metrics.TopicPartition{{Topic: "orders", Partition: 0}},
			}},
		}},
		Offsets: []metrics.ConsumerOffset{{
			GroupID:     "order-processor",
			OffsetCount: 2,
			Offsets: []metrics.PartitionOffset{
				{Topic: "orders", Partition: 0, Offset: i64(95), LeaderEpoch: 3},
				{Topic: "orders", Partition: 1, Offset: i64(50), LeaderEpoch: 3},
			},
		}},

		Agent: metrics.AgentStats{
			UptimeSec:        int64(seq) * 10,
			BatchesCollected: seq,
			BatchesExported:  seq - 1,
			RPC: &metrics.RPCStats{
				WindowMs: 30_000,
				Brokers:  []metrics.BrokerRPC{{BrokerID: 1, Requests: []metrics.BrokerAPIRPC{rpc}, ConnectAttempts: 1}},
			},
		},
		// The collector's real section output under DEFAULT config: every phase
		// emits its section on every cycle, and the ones that did not run —
		// log_dirs and the window because they are off, the two triggered phases
		// because nothing triggered them, group_states because the fast poll is
		// off — say "skipped" rather than going absent. topics_end is stamped
		// after offsets finished, which is the phase-order constraint.
		Sections: []metrics.Section{
			{Name: "cluster", Status: metrics.SectionOK, SampledAt: ms(0), DurationMs: 1},
			{Name: "topics", Status: metrics.SectionOK, SampledAt: ms(1), DurationMs: 2},
			{Name: "topics_window", Status: metrics.SectionSkipped, SampledAt: ms(1), DurationMs: 0},
			{Name: "topics_lso", Status: metrics.SectionOK, SampledAt: ms(8), DurationMs: 1},
			{Name: "topics_end", Status: metrics.SectionOK, SampledAt: ms(10), DurationMs: 3},
			{Name: "groups", Status: metrics.SectionOK, SampledAt: ms(2), DurationMs: 4},
			{Name: "offsets", Status: metrics.SectionOK, SampledAt: ms(3), DurationMs: 5},
			{Name: "group_states", Status: metrics.SectionSkipped, SampledAt: ms(11), DurationMs: 0},
			{Name: "epoch_probes", Status: metrics.SectionSkipped, SampledAt: ms(11), DurationMs: 0},
			{Name: "log_dirs", Status: metrics.SectionSkipped, SampledAt: ms(11), DurationMs: 0},
			{Name: "reassignments", Status: metrics.SectionSkipped, SampledAt: ms(11), DurationMs: 0},
			{Name: "broker_rpc", Status: metrics.SectionOK, SampledAt: ms(13), DurationMs: 0},
		},
	}
}

// sectionIndex locates a section by name so a mutation cannot target the wrong
// one when the section list grows.
func sectionIndex(b *metrics.Batch, name string) int {
	for i, sec := range b.Sections {
		if sec.Name == name {
			return i
		}
	}
	panic("fixture has no section named " + name)
}

func newTestServer(t *testing.T, mutate func(*Config)) *Server {
	t.Helper()
	cfg := Config{
		APIKey: "k",
		Out:    io.Discard,
		Now:    func() time.Time { return base },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func request(body []byte, key string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, DefaultPath, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-API-Key", "k")
	r.Header.Set("Idempotency-Key", key)
	r.ContentLength = int64(len(body))
	return r
}

func postBatch(t *testing.T, s *Server, b *metrics.Batch) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return postRaw(t, s, body, idempotencyKey(b))
}

func postRaw(t *testing.T, s *Server, body []byte, key string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, request(body, key))
	return w
}

func codes(fs []Failure) []string {
	out := make([]string, 0, len(fs))
	for _, f := range fs {
		out = append(out, f.Code)
	}
	return out
}

func TestValidBatchProducesNoFailures(t *testing.T) {
	s := newTestServer(t, nil)
	w := postBatch(t, s, validBatch(1))

	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body.String())
	}
	if got := s.Failures(); len(got) != 0 {
		t.Fatalf("expected a clean batch, got %v", codes(got))
	}
	if st := s.Stats(); st.OK != 1 || st.Accepted != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestDefectFiresItsCode mutates one thing per subtest and asserts the exact
// dotted code fires. The codes are a contract with whoever reads the output, so
// they are asserted literally.
func TestDefectFiresItsCode(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		mutate func(*metrics.Batch)
	}{
		{"schema version", "schema.version", func(b *metrics.Batch) { b.SchemaVersion = 2 }},
		{"no agent version", "envelope.agent_version", func(b *metrics.Batch) { b.AgentVersion = "" }},
		{"zero batch seq", "envelope.batch_seq", func(b *metrics.Batch) { b.BatchSeq = 0 }},
		{"clock skew", "envelope.collected_at_skew", func(b *metrics.Batch) {
			b.CollectedAt = base.Add(-time.Hour)
		}},
		{"zero collected_at", "envelope.collected_at_zero", func(b *metrics.Batch) {
			b.CollectedAt = time.Time{}
		}},
		{"section out of order", "sections.order", func(b *metrics.Batch) {
			i, j := sectionIndex(b, "topics"), sectionIndex(b, "topics_end")
			b.Sections[i], b.Sections[j] = b.Sections[j], b.Sections[i]
		}},
		{"nameless section", "sections.empty_name", func(b *metrics.Batch) {
			b.Sections[4].Name = ""
			b.Sections[4].Status = metrics.SectionSkipped
		}},
		{"missing section", "sections.missing", func(b *metrics.Batch) {
			b.Sections = b.Sections[:4]
		}},
		{"duplicate section", "sections.duplicate", func(b *metrics.Batch) {
			b.Sections = append(b.Sections, b.Sections[0])
		}},
		{"bad section status", "sections.status", func(b *metrics.Batch) {
			b.Sections[0].Status = "fine"
		}},
		{"topics_end before topics", "sections.topics_end_before_topics", func(b *metrics.Batch) {
			topics := b.Sections[sectionIndex(b, "topics")].SampledAt
			b.Sections[sectionIndex(b, "topics_end")].SampledAt = topics.Add(-time.Millisecond)
		}},
		{"end offsets sampled before commits finished", "sections.phase_order", func(b *metrics.Batch) {
			// topics_end at +4ms, offsets finishing at 3+5=+8ms.
			b.Sections[sectionIndex(b, "topics_end")].SampledAt = base.Add(4 * time.Millisecond)
		}},
		{"error count understated", "sections.error_count", func(b *metrics.Batch) {
			b.Errors = []metrics.CollectionError{{Section: "groups", Message: "boom", Kind: "other"}}
		}},
		{"error on an absent section", "errors.unknown_section", func(b *metrics.Batch) {
			b.Errors = []metrics.CollectionError{{Section: "nope", Message: "boom"}}
			b.Sections[3].ErrorCount = 1
		}},
		{"error with no message", "errors.message_empty", func(b *metrics.Batch) {
			b.Errors = []metrics.CollectionError{{Section: "groups"}}
			b.Sections[3].ErrorCount = 1
			b.Sections[3].Status = metrics.SectionPartial
		}},
		{"broker count", "data.broker_count", func(b *metrics.Batch) { b.Cluster.BrokerCount = 3 }},
		{"partition count", "data.partition_count", func(b *metrics.Batch) {
			b.Topics[0].PartitionCount = 9
		}},
		{"member count", "data.member_count", func(b *metrics.Batch) { b.Groups[0].MemberCount = 4 }},
		{"duplicate topic", "data.duplicate_topic", func(b *metrics.Batch) {
			b.Topics = append(b.Topics, b.Topics[0])
		}},
		{"duplicate group", "data.duplicate_group", func(b *metrics.Batch) {
			b.Groups = append(b.Groups, b.Groups[0])
		}},
		{"duplicate partition", "data.duplicate_partition", func(b *metrics.Batch) {
			b.Topics[0].Partitions[1].ID = 0
			b.Topics[0].Partitions[1].EndOffset = i64(100)
		}},
		{"duplicate group/topic/partition offset", "data.duplicate_offset", func(b *metrics.Batch) {
			b.Offsets[0].Offsets[1].Partition = 0
		}},
		{"inverted offsets", "data.offset_inverted", func(b *metrics.Batch) {
			b.Topics[0].Partitions[0].StartOffset = i64(5)
			b.Topics[0].Partitions[0].EndOffset = i64(0)
		}},
		{"raw -1 offset", "data.offset_negative", func(b *metrics.Batch) {
			b.Offsets[0].Offsets[0].Offset = i64(-1)
		}},
		{"committed above the high watermark", "data.negative_lag", func(b *metrics.Batch) {
			b.Offsets[0].Offsets[0].Offset = i64(105)
		}},
		{"collected does not match seq", "agent.batches_collected", func(b *metrics.Batch) {
			b.Agent.BatchesCollected = 99
		}},
		{"this batch already exported", "agent.batches_exported", func(b *metrics.Batch) {
			b.Agent.BatchesExported = b.BatchSeq
		}},
		{"counters exceed collected", "agent.counter_sum", func(b *metrics.Batch) {
			b.Agent.BatchesDropped = b.Agent.BatchesCollected + 1
		}},
		{"queue backed up", "agent.queue_depth_positive", func(b *metrics.Batch) {
			b.Agent.QueueDepth = 3
		}},

		// Truncation must never be silent: a short list with no truncation
		// block is the exact falsehood the counts were added to prevent.
		{"silent partition truncation", "data.partition_count", func(b *metrics.Batch) {
			b.Topics[0].PartitionCount = 40
		}},
		{"silent offset truncation", "data.offset_count", func(b *metrics.Batch) {
			b.Offsets[0].OffsetCount = 40
		}},
		{"offset_count below the list", "data.offset_count", func(b *metrics.Batch) {
			b.Offsets[0].OffsetCount = 1
			b.Truncation = &metrics.Truncation{Offsets: 1}
			b.Limits = &metrics.Limits{MaxOffsetsPerGroup: 1}
		}},
		{"empty truncation block", "truncation.empty", func(b *metrics.Batch) {
			b.Truncation = &metrics.Truncation{}
		}},
		{"truncation with no cap to explain it", "truncation.no_limits", func(b *metrics.Batch) {
			b.Topics[0].PartitionCount = 40
			b.Truncation = &metrics.Truncation{Partitions: 38}
		}},
		{"understated partition truncation", "truncation.partitions", func(b *metrics.Batch) {
			b.Topics[0].PartitionCount = 40
			b.Truncation = &metrics.Truncation{Partitions: 1}
			b.Limits = &metrics.Limits{MaxPartitionsPerTopic: 2}
		}},
		{"section truncated with no batch block", "truncation.section_unaccounted", func(b *metrics.Batch) {
			b.Sections[1].Truncated = true
		}},
		{"collapsed errors do not add up", "truncation.errors_collapsed", func(b *metrics.Batch) {
			b.Truncation = &metrics.Truncation{ErrorsCollapsed: 5}
		}},
		{"more errors than the declared cap", "errors.cap_exceeded", func(b *metrics.Batch) {
			b.Errors = []metrics.CollectionError{
				{Section: "groups", Message: "a", Kind: "other"},
				{Section: "groups", Message: "b", Kind: "other"},
			}
			b.Sections[3].ErrorCount = 2
			b.Sections[3].Status = metrics.SectionPartial
			b.Limits = &metrics.Limits{MaxErrors: 1}
		}},
		{"negative error count", "errors.negative_count", func(b *metrics.Batch) {
			b.Errors = []metrics.CollectionError{{Section: "groups", Message: "a", Kind: "other", Count: -1}}
			b.Sections[3].ErrorCount = 1
			b.Sections[3].Status = metrics.SectionPartial
		}},

		{"lso above the high watermark", "data.lso_above_end_offset", func(b *metrics.Batch) {
			b.Topics[0].Partitions[0].LastStableOffset = i64(200)
		}},
		{"lso below the log start", "data.lso_below_start_offset", func(b *metrics.Batch) {
			b.Topics[0].Partitions[0].StartOffset = i64(10)
			b.Topics[0].Partitions[0].LastStableOffset = i64(5)
		}},
		{"lso sampled after the high watermarks", "sections.lso_after_end", func(b *metrics.Batch) {
			// Move the existing section rather than appending a second one:
			// the fixture already carries topics_lso, and a duplicate name
			// trips sections.duplicate before this check is reached.
			b.Sections[sectionIndex(b, "topics_lso")].SampledAt = base.Add(20 * time.Millisecond)
		}},
		{"duplicate log directory", "data.duplicate_log_dir", func(b *metrics.Batch) {
			b.LogDirs = []metrics.LogDir{{Broker: 1, Dir: "/var/lib/kafka"}, {Broker: 1, Dir: "/var/lib/kafka"}}
		}},
		{"negative replica size", "data.log_dir_negative", func(b *metrics.Batch) {
			b.LogDirs = []metrics.LogDir{{Broker: 1, Dir: "/var/lib/kafka", Partitions: []metrics.LogDirPartition{
				{Topic: "orders", Partition: 0, Size: -1},
			}}}
		}},
		{"same replica twice in one directory", "data.duplicate_log_dir_partition", func(b *metrics.Batch) {
			b.LogDirs = []metrics.LogDir{{Broker: 1, Dir: "/var/lib/kafka", Partitions: []metrics.LogDirPartition{
				{Topic: "orders", Partition: 0}, {Topic: "orders", Partition: 0},
			}}}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t, nil)
			b := validBatch(1)
			tc.mutate(b)
			postBatch(t, s, b)

			got := codes(s.Failures())
			for _, c := range got {
				if c == tc.want {
					return
				}
			}
			t.Fatalf("want %s, got %v", tc.want, got)
		})
	}
}

// A phase switched off by configuration must not disable the checks that do not
// read it. Regression guard for a bug that made the harness inert: a whole-batch
// "is anything degraded" guard suppressed the negative-lag detector on 100% of
// real traffic, because log_dirs is "skipped" by default.
func TestSkippedOptionalSectionDoesNotSilenceChecks(t *testing.T) {
	for _, off := range []string{"log_dirs", "topics_lso"} {
		t.Run(off+" disabled", func(t *testing.T) {
			s := newTestServer(t, nil)
			b := validBatch(1)
			for i := range b.Sections {
				if b.Sections[i].Name == off {
					b.Sections[i].Status = metrics.SectionSkipped
				}
			}
			b.Offsets[0].Offsets[0].Offset = i64(105) // above end_offset 100
			postBatch(t, s, b)

			for _, c := range codes(s.Failures()) {
				if c == "data.negative_lag" {
					return
				}
			}
			t.Fatalf("negative lag went unreported while %s was skipped", off)
		})
	}
}

// The converse: when the sections the invariant actually reads are themselves
// broken, the comparison is meaningless and must stay quiet.
func TestBrokenSourceSectionSuppressesTheComparison(t *testing.T) {
	s := newTestServer(t, nil)
	b := validBatch(1)
	for i := range b.Sections {
		if b.Sections[i].Name == "topics_end" {
			b.Sections[i].Status = metrics.SectionFailed
		}
	}
	b.Offsets[0].Offsets[0].Offset = i64(105)
	postBatch(t, s, b)

	for _, c := range codes(s.Failures()) {
		if c == "data.negative_lag" {
			t.Fatal("compared against a failed topics_end section")
		}
	}
}

func TestUnknownFieldIsRejected(t *testing.T) {
	s := newTestServer(t, nil)
	b := validBatch(1)

	var raw map[string]any
	data, _ := json.Marshal(b)
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	topics := raw["topics"].([]any)
	topics[0].(map[string]any)["surprise"] = 1
	data, _ = json.Marshal(raw)

	w := postRaw(t, s, data, idempotencyKey(b))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", w.Code)
	}
	if got := codes(s.Failures()); len(got) != 1 || got[0] != "schema.decode" {
		t.Fatalf("want schema.decode, got %v", got)
	}
}

func TestIdempotencyKeyFormat(t *testing.T) {
	s := newTestServer(t, nil)
	b := validBatch(1)
	data, _ := json.Marshal(b)

	postRaw(t, s, data, "wrong-key")
	if got := codes(s.Failures()); len(got) != 1 || got[0] != "envelope.idempotency_key_format" {
		t.Fatalf("want envelope.idempotency_key_format, got %v", got)
	}
}

func TestReplayOfAnAcknowledgedBatch(t *testing.T) {
	s := newTestServer(t, nil)
	b := validBatch(1)

	postBatch(t, s, b)
	w := postBatch(t, s, b)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", w.Code)
	}
	if got := codes(s.Failures()); len(got) != 1 || got[0] != "idempotency.replay" {
		t.Fatalf("want idempotency.replay, got %v", got)
	}
}

func TestRetryAfterAnInjectedFailureIsNotAReplay(t *testing.T) {
	s := newTestServer(t, func(c *Config) { c.Faults = Faults{FailFirst: 1} })
	b := validBatch(1)

	if w := postBatch(t, s, b); w.Code != http.StatusInternalServerError {
		t.Fatalf("first delivery: status %d, want 500", w.Code)
	}
	if w := postBatch(t, s, b); w.Code != http.StatusOK {
		t.Fatalf("redelivery: status %d, want 200 (the 500 was never an ack)", w.Code)
	}
	if got := s.Failures(); len(got) != 0 {
		t.Fatalf("a retry of an unacknowledged batch must be clean, got %v", codes(got))
	}
	if st := s.Stats(); st.Duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", st.Duplicates)
	}
}

func TestSameKeyDifferentBody(t *testing.T) {
	s := newTestServer(t, func(c *Config) { c.Faults = Faults{FailFirst: 1} })
	b := validBatch(1)
	postBatch(t, s, b)

	b.CollectionMs = 999
	postBatch(t, s, b)

	if got := codes(s.Failures()); len(got) != 1 || got[0] != "idempotency.body_mismatch" {
		t.Fatalf("want idempotency.body_mismatch, got %v", got)
	}
}

func TestSequenceRegressionAndGap(t *testing.T) {
	s := newTestServer(t, nil)
	postBatch(t, s, validBatch(1))
	postBatch(t, s, validBatch(2))

	// Back to 1 under a brand new key: not a retry, a regression.
	old := validBatch(1)
	old.CollectionMs = 7
	data, _ := json.Marshal(old)
	postRaw(t, s, data, "streamsight-agent-3f2a1b09-1x")
	if !hasCode(s.Failures(), "idempotency.seq_regression") {
		t.Fatalf("want idempotency.seq_regression, got %v", codes(s.Failures()))
	}

	s2 := newTestServer(t, nil)
	postBatch(t, s2, validBatch(1))
	skipped := validBatch(4)
	// The gap is 2, and the agent's own drop counter accounts for exactly that.
	skipped.Agent.BatchesDropped = 2
	skipped.Agent.BatchesExported = 1
	postBatch(t, s2, skipped)
	if !hasCode(s2.Failures(), "idempotency.seq_gap") {
		t.Fatalf("want idempotency.seq_gap, got %v", codes(s2.Failures()))
	}
	if hasCode(s2.Failures(), "idempotency.drop_accounting") {
		t.Fatalf("the agent accounted for the gap; drop_accounting must not fire")
	}
}

func TestGapNotAccountedForByTheAgent(t *testing.T) {
	s := newTestServer(t, nil)
	postBatch(t, s, validBatch(1))
	postBatch(t, s, validBatch(4)) // batches_dropped stays 0

	if !hasCode(s.Failures(), "idempotency.drop_accounting") {
		t.Fatalf("want idempotency.drop_accounting, got %v", codes(s.Failures()))
	}
}

func TestGzipRoundTripAndUndeclaredGzip(t *testing.T) {
	s := newTestServer(t, nil)
	b := validBatch(1)
	data, _ := json.Marshal(b)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	r := request(buf.Bytes(), idempotencyKey(b))
	r.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("gzip body: status %d, failures %v", w.Code, codes(s.Failures()))
	}

	// Same bytes, no header: the two disagree and nothing downstream can tell.
	s2 := newTestServer(t, nil)
	postRaw(t, s2, buf.Bytes(), idempotencyKey(b))
	if !hasCode(s2.Failures(), "transport.gzip_undeclared") {
		t.Fatalf("want transport.gzip_undeclared, got %v", codes(s2.Failures()))
	}
}

func TestTransportFailures(t *testing.T) {
	s := newTestServer(t, nil)
	b := validBatch(1)
	data, _ := json.Marshal(b)

	t.Run("wrong api key", func(t *testing.T) {
		r := request(data, idempotencyKey(b))
		r.Header.Set("X-API-Key", "nope")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status %d, want 401", w.Code)
		}
	})

	t.Run("wrong method", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, DefaultPath, nil)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status %d, want 405", w.Code)
		}
	})

	t.Run("unknown route", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/batches/v1", bytes.NewReader(data))
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusNotFound {
			t.Fatalf("status %d, want 404", w.Code)
		}
	})

	t.Run("no idempotency key", func(t *testing.T) {
		s := newTestServer(t, nil)
		postRaw(t, s, data, "")
		if !hasCode(s.Failures(), "transport.idempotency_key") {
			t.Fatalf("got %v", codes(s.Failures()))
		}
	})
}

func TestWarnOnlyNeverRejects(t *testing.T) {
	s := newTestServer(t, func(c *Config) { c.WarnOnly = true })
	b := validBatch(1)
	b.SchemaVersion = 2

	if w := postBatch(t, s, b); w.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 under warn-only", w.Code)
	}
	if !hasCode(s.Failures(), "schema.version") {
		t.Fatalf("the failure must still be reported, got %v", codes(s.Failures()))
	}
	for _, f := range s.Failures() {
		if f.Severity != SeverityWarn {
			t.Fatalf("warn-only must demote everything, got %s on %s", f.Severity, f.Code)
		}
	}
}

func TestStrictPromotesWarnings(t *testing.T) {
	s := newTestServer(t, func(c *Config) { c.Strict = true })
	b := validBatch(1)
	b.Offsets[0].Offsets[0].Offset = i64(105) // negative lag, a WARN by default

	if w := postBatch(t, s, b); w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 under strict", w.Code)
	}
}

// internal/export reads at most errBodyLimit bytes of an error response, so a
// longer body is silently clipped in the operator's log.
func TestConformanceBodyStaysUnderTheExporterLimit(t *testing.T) {
	failures := make([]Failure, 200)
	for i := range failures {
		failures[i] = Failure{
			Code:     fmt.Sprintf("data.some_long_check_name_%03d", i),
			Severity: SeverityFatal,
			Path:     fmt.Sprintf("$.topics[%d].partitions[%d].start_offset", i, i),
			Message:  strings.Repeat("a very wordy explanation ", 4),
		}
	}
	body := conformanceBody(42, failures)
	if len(body) > errBodyLimit {
		t.Fatalf("body is %d bytes, the exporter reads only %d", len(body), errBodyLimit)
	}
	var out struct {
		Truncated int `json:"truncated_failures"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("truncated body must stay valid JSON: %v", err)
	}
	if out.Truncated == 0 {
		t.Fatal("expected the truncation count to be reported")
	}
}

func TestRequireClosesDone(t *testing.T) {
	s := newTestServer(t, func(c *Config) { c.Require = 2 })
	postBatch(t, s, validBatch(1))
	select {
	case <-s.Done():
		t.Fatal("done closed after one batch, want two")
	default:
	}
	postBatch(t, s, validBatch(2))
	select {
	case <-s.Done():
	default:
		t.Fatal("done not closed after the required batches")
	}
}

func TestControlEndpointDrivesFaults(t *testing.T) {
	s := newTestServer(t, nil)

	r := httptest.NewRequest(http.MethodPost, "/control", strings.NewReader(`{"every_nth":1,"every_nth_status":503}`))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("control: status %d", w.Code)
	}

	if got := postBatch(t, s, validBatch(1)); got.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", got.Code)
	}
	if got := s.Faults(); got.EveryNth != 1 || got.RetryAfterSec != 1 {
		t.Fatalf("faults = %+v", got)
	}
}

// TestSchemaV1Frozen is the only check in this package that actually enforces
// schema_version 1; DisallowUnknownFields cannot, because mock and agent are
// built from the same tree. Regenerate deliberately, and bump
// metrics.SchemaVersion when the change is not backwards compatible:
//
//	go test ./internal/mockingest -run TestSchemaV1Frozen -update
func TestSchemaV1Frozen(t *testing.T) {
	got := strings.Join(FieldPaths(reflect.TypeOf(metrics.Batch{})), "\n") + "\n"
	path := filepath.Join("testdata", "schema_v1.txt")

	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if got != string(want) {
		t.Fatalf("metrics.Batch no longer matches the frozen schema %d.\n"+
			"If the change is deliberate, bump metrics.SchemaVersion when it is not\n"+
			"backwards compatible, then regenerate:\n"+
			"  go test ./internal/mockingest -run TestSchemaV1Frozen -update\n\n%s",
			metrics.SchemaVersion, diffLines(string(want), got))
	}
}

func diffLines(want, got string) string {
	inWant := map[string]bool{}
	for _, l := range strings.Split(want, "\n") {
		inWant[l] = true
	}
	inGot := map[string]bool{}
	for _, l := range strings.Split(got, "\n") {
		inGot[l] = true
	}

	var b strings.Builder
	for _, l := range strings.Split(got, "\n") {
		if l != "" && !inWant[l] {
			fmt.Fprintf(&b, "+ %s\n", l)
		}
	}
	for _, l := range strings.Split(want, "\n") {
		if l != "" && !inGot[l] {
			fmt.Fprintf(&b, "- %s\n", l)
		}
	}
	return b.String()
}

func hasCode(fs []Failure, code string) bool {
	for _, f := range fs {
		if f.Code == code {
			return true
		}
	}
	return false
}
