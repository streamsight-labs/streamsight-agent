package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"kafka-metrics-agent/internal/config"
	"kafka-metrics-agent/internal/export"
	"kafka-metrics-agent/internal/metrics"
)

// fakeExporter records what the agent hands it and reports whatever Stats the
// test wants.
type fakeExporter struct {
	mu       sync.Mutex
	batches  []metrics.Batch
	stats    export.Stats
	exportEr error
	closeErr error
	closed   bool
}

var _ export.Exporter = (*fakeExporter)(nil)

func (f *fakeExporter) Export(ctx context.Context, batch *metrics.Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, *batch) // copy: the agent may reuse the pointer
	return f.exportEr
}

func (f *fakeExporter) Stats() export.Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

func (f *fakeExporter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeErr
}

func (f *fakeExporter) seen() []metrics.Batch {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]metrics.Batch(nil), f.batches...)
}

// fakeCollector returns a canned batch and records the context it was given.
type fakeCollector struct {
	batch       *metrics.Batch
	nilBatch    bool
	calls       int
	hadDeadline bool
	deadlineIn  time.Duration
}

func (f *fakeCollector) Collect(ctx context.Context) *metrics.Batch {
	f.calls++
	if dl, ok := ctx.Deadline(); ok {
		f.hadDeadline = true
		f.deadlineIn = time.Until(dl)
	}
	if f.nilBatch {
		return nil
	}
	b := metrics.Batch{CollectedAt: time.Now()}
	if f.batch != nil {
		b = *f.batch
	}
	return &b
}

func newTestAgent(t *testing.T, coll batchCollector, exp export.Exporter) *Agent {
	t.Helper()
	return &Agent{
		cfg: &config.Config{
			AgentInstanceID:    "test-host-abcd1234",
			CollectionInterval: 30 * time.Second,
			CollectionTimeout:  24 * time.Second,
		},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		version:   "v1.2.3",
		collector: coll,
		exporter:  exp,
		startedAt: time.Now().Add(-90 * time.Second),
	}
}

func TestRunCycleStampsEnvelopeAndIncrementsBatchSeq(t *testing.T) {
	exp := &fakeExporter{stats: export.Stats{
		BatchesExported: 7,
		BatchesDropped:  2,
		BatchesRejected: 1,
		ExportRetries:   5,
		QueueDepth:      3,
		LastError:       "ingest said 503",
	}}
	coll := &fakeCollector{}
	a := newTestAgent(t, coll, exp)

	for i := 0; i < 3; i++ {
		a.runCycle(context.Background())
	}

	got := exp.seen()
	if len(got) != 3 {
		t.Fatalf("exported %d batches, want 3", len(got))
	}

	for i, b := range got {
		wantSeq := uint64(i + 1)
		if b.BatchSeq != wantSeq {
			t.Errorf("batch %d: BatchSeq = %d, want %d", i, b.BatchSeq, wantSeq)
		}
		if b.SchemaVersion != metrics.SchemaVersion {
			t.Errorf("batch %d: SchemaVersion = %d, want %d", i, b.SchemaVersion, metrics.SchemaVersion)
		}
		if b.AgentVersion != "v1.2.3" {
			t.Errorf("batch %d: AgentVersion = %q", i, b.AgentVersion)
		}
		if b.AgentInstanceID != "test-host-abcd1234" {
			t.Errorf("batch %d: AgentInstanceID = %q", i, b.AgentInstanceID)
		}
		if b.Agent.BatchesCollected != wantSeq {
			t.Errorf("batch %d: BatchesCollected = %d, want %d", i, b.Agent.BatchesCollected, wantSeq)
		}
	}

	// Without these counters on the wire, "agent alive but pipe broken" is
	// invisible at the backend.
	last := got[len(got)-1]
	want := metrics.AgentStats{
		UptimeSec:        last.Agent.UptimeSec,
		BatchesCollected: 3,
		BatchesExported:  7,
		BatchesDropped:   2,
		BatchesRejected:  1,
		ExportRetries:    5,
		QueueDepth:       3,
		LastExportError:  "ingest said 503",
	}
	if last.Agent != want {
		t.Errorf("Agent = %+v, want %+v", last.Agent, want)
	}
	if last.Agent.UptimeSec < 89 {
		t.Errorf("UptimeSec = %d, want ~90", last.Agent.UptimeSec)
	}
}

func TestRunCycleAppliesCollectionTimeout(t *testing.T) {
	coll := &fakeCollector{}
	a := newTestAgent(t, coll, &fakeExporter{})

	a.runCycle(context.Background())

	if !coll.hadDeadline {
		t.Fatal("collector context had no deadline; a hung broker would eat the tick interval")
	}
	if coll.deadlineIn > 24*time.Second || coll.deadlineIn < 20*time.Second {
		t.Fatalf("deadline in %s, want ~24s (COLLECTION_TIMEOUT)", coll.deadlineIn)
	}
}

func TestRunCycleExportIsStillAttemptedAfterAFullBudgetCollection(t *testing.T) {
	// The collection deadline must not leak into the export call: a cycle that
	// used its whole budget still has a batch worth shipping.
	exp := &fakeExporter{}
	coll := &fakeCollector{}
	a := newTestAgent(t, coll, exp)
	a.cfg.CollectionTimeout = time.Nanosecond

	a.runCycle(context.Background())

	if len(exp.seen()) != 1 {
		t.Fatal("batch was not exported after an exhausted collection budget")
	}
}

func TestRunCycleSurvivesExportError(t *testing.T) {
	exp := &fakeExporter{exportEr: errors.New("queue full")}
	coll := &fakeCollector{}
	a := newTestAgent(t, coll, exp)

	a.runCycle(context.Background())
	a.runCycle(context.Background())

	if a.batchSeq != 2 {
		t.Fatalf("batchSeq = %d, want 2: an export failure must not stall the sequence", a.batchSeq)
	}
}

func TestRunCycleWithNilBatchDoesNotConsumeASequenceNumber(t *testing.T) {
	exp := &fakeExporter{}
	coll := &fakeCollector{nilBatch: true}
	a := newTestAgent(t, coll, exp)

	a.runCycle(context.Background())

	if a.batchSeq != 0 {
		t.Fatalf("batchSeq = %d, want 0", a.batchSeq)
	}
	if len(exp.seen()) != 0 {
		t.Fatal("exported a nil batch")
	}
}

func TestCloseIsNilSafeAndReportsExporterFailure(t *testing.T) {
	// A partially built agent (no client, no exporter) must still be closeable.
	empty := &Agent{}
	if err := empty.Close(); err != nil {
		t.Fatalf("Close on empty agent: %v", err)
	}

	exp := &fakeExporter{closeErr: errors.New("flush failed")}
	a := newTestAgent(t, &fakeCollector{}, exp)
	err := a.Close()
	if err == nil {
		t.Fatal("expected the exporter close error to be reported")
	}
	if !exp.closed {
		t.Fatal("exporter was not closed")
	}
}

// Every collector option must actually come from config. GROUP_STATES was
// plumbed all the way into ListGroups and then left out of this one mapping,
// so it was dead while looking wired up from both ends. The reflection sweep
// below is the guard against the next such omission.
func TestCollectorOptionsCarryEveryConfiguredSetting(t *testing.T) {
	cfg := &config.Config{
		CollectionTimeout:     24 * time.Second,
		IncludeInternalTopics: true,
		TopicIncludeRegex:     "^orders",
		TopicExcludeRegex:     "^orders-tmp",
		GroupIncludeRegex:     "^svc-",
		GroupExcludeRegex:     "^svc-canary",
		GroupStates:           []string{"Stable", "Empty"},

		CollectLastStableOffset: true,
		CollectConsumerGroups:   true,
		CollectLogDirs:          true,
		LogDirsEvery:            7,

		MaxErrors:             11,
		MaxErrorSamples:       2,
		MaxTopics:             3,
		MaxPartitionsPerTopic: 4,
		MaxGroups:             5,
		MaxMembersPerGroup:    6,
		MaxOffsetsPerGroup:    8,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := collectorOptions(cfg, logger)

	if !reflect.DeepEqual(opts.GroupStates, cfg.GroupStates) {
		t.Errorf("GroupStates = %v, want %v", opts.GroupStates, cfg.GroupStates)
	}
	if opts.Timeout != cfg.CollectionTimeout {
		t.Errorf("Timeout = %s, want %s", opts.Timeout, cfg.CollectionTimeout)
	}
	if opts.Limits.MaxGroups != cfg.MaxGroups || opts.Limits.MaxErrors != cfg.MaxErrors {
		t.Errorf("Limits = %+v", opts.Limits)
	}

	// Every field above is deliberately non-zero, so a zero here means the
	// mapping dropped it.
	for _, v := range []reflect.Value{reflect.ValueOf(opts), reflect.ValueOf(opts.Limits)} {
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).IsZero() {
				t.Errorf("%s.%s is zero: it is not mapped from config", v.Type().Name(), v.Type().Field(i).Name)
			}
		}
	}
}

// The agent must be satisfiable by every real exporter, not just the fake.
func TestRealExportersSatisfyTheAgentsDependency(t *testing.T) {
	e, err := export.New(export.Config{Mode: export.ModeFile, Path: t.TempDir() + "/metrics.jsonl"})
	if err != nil {
		t.Fatalf("export.New: %v", err)
	}
	defer e.Close()

	a := newTestAgent(t, &fakeCollector{}, e)
	a.runCycle(context.Background())

	if got := a.exporter.Stats().BatchesExported; got != 1 {
		t.Fatalf("BatchesExported = %d, want 1", got)
	}
}
