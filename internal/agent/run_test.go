package agent

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/streamsight-labs/streamsight-agent/internal/collector"
	"github.com/streamsight-labs/streamsight-agent/internal/config"
	"github.com/streamsight-labs/streamsight-agent/internal/kafka"
	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// signalCollector closes collected on its first Collect. Run installs its signal
// handler and then collects immediately, so waiting for that close is what
// proves the handler is in place before the test raises anything -- without it
// the race is lost sometimes and the default SIGTERM disposition takes the whole
// test binary down.
type signalCollector struct {
	fakeCollector
	collected chan struct{}
}

func (s *signalCollector) Collect(ctx context.Context) *metrics.Batch {
	b := s.fakeCollector.Collect(ctx)
	select {
	case <-s.collected:
	default:
		close(s.collected)
	}
	return b
}

// TestRunReturnsCleanlyOnSIGTERM pins Run's contract on a signal: return nil,
// do not exit, do not hang. cmd/agent has a single os.Exit and the deferred
// Close that flushes the exporter is what stands between a rollout and a
// truncated JSONL file, so a Run that exited from inside or blocked forever
// would lose the last batch on every restart.
func TestRunReturnsCleanlyOnSIGTERM(t *testing.T) {
	// Registered before Run so the signal can never reach a default disposition
	// and kill the test binary, whichever order the two registrations land in.
	// signal.Stop rather than signal.Reset: Reset would clear the handler for
	// every other test in this process too.
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	coll := &signalCollector{collected: make(chan struct{})}
	exp := &fakeExporter{}
	a := newTestAgent(t, coll, exp)
	// An hour, so the ticker cannot fire a second cycle and the only thing that
	// ends the loop is the signal.
	a.cfg.CollectionInterval = time.Hour

	done := make(chan error, 1)
	go func() { done <- a.Run() }()

	<-coll.collected
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("raise SIGTERM: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
	if a.batchSeq != 1 {
		t.Errorf("batchSeq = %d, want 1", a.batchSeq)
	}
	if len(exp.seen()) != 1 {
		t.Errorf("exported %d batches, want the one complete cycle", len(exp.seen()))
	}
}

// TestRunCycleDiscardsTheBatchAShutdownCutShort covers the other half of the
// shutdown path. A cycle cut short by SIGTERM produces a batch whose every
// section failed with a transport error: a description of the agent stopping,
// not of the cluster breaking. It must be discarded AND consume no sequence
// number -- a hole in batch_seq means lost data at the backend, so shipping one
// on every rollout would make routine restarts look like an outage.
func TestRunCycleDiscardsTheBatchAShutdownCutShort(t *testing.T) {
	exp := &fakeExporter{}
	coll := &fakeCollector{}
	a := newTestAgent(t, coll, exp)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a.runCycle(ctx)

	// The cycle did run: it is the batch that is thrown away, not the collection
	// that is skipped, because the cancellation is only observable after Collect
	// returns.
	if coll.calls != 1 {
		t.Fatalf("collector called %d times, want 1", coll.calls)
	}
	if a.batchSeq != 0 {
		t.Errorf("batchSeq = %d, want 0: an interrupted cycle must consume no sequence number", a.batchSeq)
	}
	if len(exp.seen()) != 0 {
		t.Errorf("exported %d batches, want none", len(exp.seen()))
	}
}

// TestCapabilityGatesDisableEveryPhaseTheClusterCannotServe is the inverse of
// the probe-failed case the other tests cover: a cluster that DID answer the
// probe and advertises nothing must lose every gated phase, and only the gated
// ones.
//
// A zero-value Capabilities is exactly that cluster -- its per-key version
// floors map is empty, so every Supports* question finds no key present and
// answers false -- which makes it the one fixture that walks the whole gate
// table in a single pass. The comparison is against an Options carrying only the
// ungated settings, so a switch added to the table without being wanted there
// fails here rather than silently disabling something an operator paid for.
func TestCapabilityGatesDisableEveryPhaseTheClusterCannotServe(t *testing.T) {
	opts := collector.Options{
		CollectConsumerGroups:   true,
		CollectLastStableOffset: true,
		CollectThroughputWindow: true,
		CollectReassignments:    true,
		CollectMaxTimestamp:     true,
		CollectTieredOffsets:    true,
		CollectLatestTiered:     true,
		CollectShareGroups:      true,
		CollectConfigs:          true,
		CollectEpochProbes:      true,

		// Ungated: log dirs need no capability at all, and the cadences are
		// operator settings no gate may touch.
		CollectLogDirs: true,
		LogDirsEvery:   24,
		ConfigsEvery:   360,
	}
	applyCapabilityGates(&opts, &kafka.Capabilities{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	want := collector.Options{CollectLogDirs: true, LogDirsEvery: 24, ConfigsEvery: 360}
	if !reflect.DeepEqual(opts, want) {
		t.Errorf("options = %+v, want %+v", opts, want)
	}
}

// TestNewFailsWhenTheClusterIsUnreachable pins New's contract on the failure
// side: a cluster that cannot be reached is a refusal to start, not an agent
// that ships empty batches forever. Port 1 refuses the connection immediately,
// so this exercises a failing cluster rather than a slow one.
func TestNewFailsWhenTheClusterIsUnreachable(t *testing.T) {
	_, err := New(&config.Config{
		KafkaBrokers:       []string{"127.0.0.1:1"},
		AgentInstanceID:    "test",
		CollectionInterval: time.Second,
		CollectionTimeout:  2 * time.Second,
		LogLevel:           "error",
	}, "test")
	if err == nil {
		t.Fatal("New succeeded against a cluster that is not there")
	}
}
