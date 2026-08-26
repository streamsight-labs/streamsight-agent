package kafka

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-metrics-agent/internal/metrics"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func meta(nodeID int32) kgo.BrokerMetadata { return kgo.BrokerMetadata{NodeID: nodeID} }

func e2e(write, readWait, read, writeWait time.Duration, bw, br int) kgo.BrokerE2E {
	return kgo.BrokerE2E{
		BytesWritten: bw,
		BytesRead:    br,
		WriteWait:    writeWait,
		TimeToWrite:  write,
		ReadWait:     readWait,
		TimeToRead:   read,
	}
}

func findBroker(t *testing.T, s *metrics.RPCStats, id int32) metrics.BrokerRPC {
	t.Helper()
	for _, b := range s.Brokers {
		if b.BrokerID == id {
			return b
		}
	}
	t.Fatalf("broker %d missing from snapshot %+v", id, s.Brokers)
	return metrics.BrokerRPC{}
}

func findAPI(t *testing.T, b metrics.BrokerRPC, api string) metrics.BrokerAPIRPC {
	t.Helper()
	for _, r := range b.Requests {
		if r.API == api {
			return r
		}
	}
	t.Fatalf("api %q missing from broker %d: %+v", api, b.BrokerID, b.Requests)
	return metrics.BrokerAPIRPC{}
}

func TestSnapshotAggregatesPerBrokerPerAPI(t *testing.T) {
	h := newRPCHooks(epoch)

	h.OnBrokerE2E(meta(1), metadataKey, e2e(time.Millisecond, 2*time.Millisecond, time.Millisecond, 500*time.Microsecond, 100, 900))
	h.OnBrokerE2E(meta(1), metadataKey, e2e(time.Millisecond, 3*time.Millisecond, time.Millisecond, 100*time.Microsecond, 100, 800))
	h.OnBrokerE2E(meta(1), listOffsetsKey, e2e(time.Millisecond, 0, time.Millisecond, 0, 50, 60))
	h.OnBrokerE2E(meta(2), metadataKey, e2e(time.Millisecond, 0, 0, 0, 10, 20))

	got := h.snapshot(epoch.Add(30 * time.Second))

	if got.WindowMs != 30_000 {
		t.Errorf("WindowMs = %d, want 30000", got.WindowMs)
	}
	if len(got.Brokers) != 2 {
		t.Fatalf("Brokers = %d, want 2", len(got.Brokers))
	}
	if got.Brokers[0].BrokerID != 1 || got.Brokers[1].BrokerID != 2 {
		t.Errorf("brokers not sorted by ID: %+v", got.Brokers)
	}

	b1 := findBroker(t, got, 1)
	if len(b1.Requests) != 2 || b1.Requests[0].API != metrics.APIKeyListOffsets || b1.Requests[1].API != metrics.APIKeyMetadata {
		t.Fatalf("requests not sorted by API name: %+v", b1.Requests)
	}

	md := findAPI(t, b1, metrics.APIKeyMetadata)
	if md.BytesWritten != 200 || md.BytesRead != 1700 {
		t.Errorf("bytes = %d/%d, want 200/1700", md.BytesWritten, md.BytesRead)
	}
	if md.Errors != 0 {
		t.Errorf("Errors = %d, want 0", md.Errors)
	}
	// DurationE2E is TimeToWrite+ReadWait+TimeToRead: 4ms then 5ms. WriteWait is
	// deliberately absent from it and carried as its own series.
	if md.E2E.Count != 2 || md.E2E.SumUs != 9_000 {
		t.Errorf("E2E = count %d sum %dus, want 2/9000", md.E2E.Count, md.E2E.SumUs)
	}
	if md.E2E.MaxUs == nil || *md.E2E.MaxUs != 5_000 {
		t.Errorf("E2E.MaxUs = %v, want 5000", md.E2E.MaxUs)
	}
	if md.WriteWait.Count != 2 || md.WriteWait.SumUs != 600 {
		t.Errorf("WriteWait = count %d sum %dus, want 2/600", md.WriteWait.Count, md.WriteWait.SumUs)
	}
}

// A histogram must stay decomposable: bucket counts are per-bucket and add
// across cycles, which is the whole reason no percentile is computed here.
func TestSnapshotHistogramIsDecomposable(t *testing.T) {
	h := newRPCHooks(epoch)
	h.OnBrokerE2E(meta(1), metadataKey, e2e(400*time.Microsecond, 0, 0, 0, 1, 1)) // 400us -> bucket 0
	h.OnBrokerE2E(meta(1), metadataKey, e2e(600*time.Microsecond, 0, 0, 0, 1, 1)) // 600us -> bucket 1
	h.OnBrokerE2E(meta(1), metadataKey, e2e(10*time.Second, 0, 0, 0, 1, 1))       // overflow

	hist := findAPI(t, findBroker(t, h.snapshot(epoch.Add(time.Second)), 1), metrics.APIKeyMetadata).E2E
	if len(hist.BoundsUs) != len(metrics.DefaultLatencyBoundsUs) {
		t.Fatalf("BoundsUs = %v, want the default layout", hist.BoundsUs)
	}
	if len(hist.Counts) != len(hist.BoundsUs)+1 {
		t.Fatalf("Counts = %d entries, want %d", len(hist.Counts), len(hist.BoundsUs)+1)
	}
	var total int64
	for _, c := range hist.Counts {
		total += c
	}
	if total != hist.Count || hist.Count != 3 {
		t.Errorf("bucket total %d, Count %d, want both 3", total, hist.Count)
	}
	if hist.Counts[0] != 1 || hist.Counts[1] != 1 || hist.Counts[len(hist.Counts)-1] != 1 {
		t.Errorf("Counts = %v, want one in bucket 0, one in bucket 1, one in overflow", hist.Counts)
	}
}

func TestFailedRequestCountsBytesButNotLatency(t *testing.T) {
	h := newRPCHooks(epoch)
	bad := e2e(time.Millisecond, 0, 0, time.Millisecond, 40, 0)
	bad.WriteErr = errors.New("connection reset")
	h.OnBrokerE2E(meta(1), metadataKey, bad)

	got := findAPI(t, findBroker(t, h.snapshot(epoch.Add(time.Second)), 1), metrics.APIKeyMetadata)
	if got.Errors != 1 {
		t.Errorf("Errors = %d, want 1", got.Errors)
	}
	if got.BytesWritten != 40 {
		t.Errorf("BytesWritten = %d, want 40", got.BytesWritten)
	}
	if got.E2E.Count != 0 || got.WriteWait.Count != 0 {
		t.Errorf("failed request entered a latency series: e2e %d, write_wait %d", got.E2E.Count, got.WriteWait.Count)
	}
	if got.E2E.MaxUs != nil {
		t.Errorf("MaxUs = %v, want nil with no observations", got.E2E.MaxUs)
	}
}

func TestConnectFailuresAndLatency(t *testing.T) {
	h := newRPCHooks(epoch)
	h.OnBrokerConnect(meta(-1), 3*time.Millisecond, nil, nil)
	h.OnBrokerConnect(meta(-1), time.Millisecond, nil, errors.New("dial tcp: refused"))
	h.OnBrokerConnect(meta(-1), time.Millisecond, nil, errors.New("dial tcp: timeout"))
	h.OnBrokerDisconnect(meta(-1), nil)
	h.OnBrokerThrottle(meta(-1), 250*time.Millisecond, true)
	h.OnBrokerThrottle(meta(-1), 750*time.Millisecond, false)

	// A seed broker keeps its negative kgo placeholder ID rather than being
	// folded into a real node.
	got := findBroker(t, h.snapshot(epoch.Add(time.Second)), -1)
	if got.ConnectAttempts != 3 || got.ConnectFailures != 2 {
		t.Errorf("connects = %d attempts / %d failures, want 3/2", got.ConnectAttempts, got.ConnectFailures)
	}
	if got.ConnectLatency.Count != int64(got.ConnectAttempts-got.ConnectFailures) {
		t.Errorf("ConnectLatency.Count = %d, want successful dials only (%d)", got.ConnectLatency.Count, got.ConnectAttempts-got.ConnectFailures)
	}
	if got.LastConnectError != "dial tcp: timeout" {
		t.Errorf("LastConnectError = %q, want the most recent failure", got.LastConnectError)
	}
	if got.Disconnects != 1 {
		t.Errorf("Disconnects = %d, want 1", got.Disconnects)
	}
	if got.ThrottleEvents != 2 || got.ThrottledMs != 1000 {
		t.Errorf("throttle = %d events / %dms, want 2/1000", got.ThrottleEvents, got.ThrottledMs)
	}
}

func TestSnapshotResetsTheWindow(t *testing.T) {
	h := newRPCHooks(epoch)
	h.OnBrokerE2E(meta(1), metadataKey, e2e(time.Millisecond, 0, 0, 0, 10, 20))

	if got := h.snapshot(epoch.Add(10 * time.Second)); len(got.Brokers) != 1 {
		t.Fatalf("first snapshot = %+v, want one broker", got.Brokers)
	}

	// Counters are per-window deltas: nothing carries over, and the second
	// window is measured from the first snapshot, not from construction.
	second := h.snapshot(epoch.Add(25 * time.Second))
	if len(second.Brokers) != 0 {
		t.Errorf("second snapshot = %+v, want empty", second.Brokers)
	}
	if second.Brokers == nil {
		t.Error("Brokers is nil, want an empty slice so the wire carries [] not null")
	}
	if second.WindowMs != 15_000 {
		t.Errorf("WindowMs = %d, want 15000", second.WindowMs)
	}
}

func TestSnapshotWindowNeverNegative(t *testing.T) {
	h := newRPCHooks(epoch)
	if got := h.snapshot(epoch.Add(-time.Minute)); got.WindowMs != 0 {
		t.Errorf("WindowMs = %d, want 0 for a backwards clock", got.WindowMs)
	}
}

func TestConnectErrorIsTruncated(t *testing.T) {
	h := newRPCHooks(epoch)
	h.OnBrokerConnect(meta(1), 0, nil, errors.New(strings.Repeat("x", maxConnectErrLen*2)))
	if got := findBroker(t, h.snapshot(epoch.Add(time.Second)), 1); len(got.LastConnectError) != maxConnectErrLen {
		t.Errorf("LastConnectError = %d bytes, want %d", len(got.LastConnectError), maxConnectErrLen)
	}
}

func TestRPCSnapshotNilWhenHooksAbsent(t *testing.T) {
	if got := (&Client{}).RPCSnapshot(); got != nil {
		t.Errorf("RPCSnapshot() = %+v, want nil so the section reports skipped", got)
	}
}

// The API name is wire contract; these are the keys the agent actually issues.
func TestAPIName(t *testing.T) {
	tests := []struct {
		key  int16
		want string
	}{
		{key: metadataKey, want: metrics.APIKeyMetadata},
		{key: listOffsetsKey, want: metrics.APIKeyListOffsets},
		{key: 9, want: metrics.APIKeyOffsetFetch},
		{key: listGroupsKey, want: metrics.APIKeyListGroups},
		{key: 15, want: metrics.APIKeyDescribeGroups},
		{key: consumerGroupDescribeKey, want: metrics.APIKeyConsumerGroupDescribe},
		{key: describeLogDirsKey, want: metrics.APIKeyDescribeLogDirs},
		{key: 60, want: metrics.APIKeyDescribeCluster},
		{key: offsetForLeaderEpochKey, want: metrics.APIKeyOffsetForLeaderEpoch},
		{key: listPartitionReassignmentsKey, want: metrics.APIKeyListPartitionReassignments},
		{key: 18, want: "api_versions"},
		{key: 17, want: "sasl_handshake"},
		{key: 36, want: "sasl_authenticate"},
		{key: 29, want: "describe_acls"},
		{key: 22, want: "init_producer_id"},
		{key: 4, want: "leader_and_isr"},
		{key: 50, want: "describe_user_scram_credentials"},
		{key: 30_000, want: "unknown_30000"},
	}
	for _, tt := range tests {
		if got := apiName(tt.key); got != tt.want {
			t.Errorf("apiName(%d) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

// The whole risk of this item: hooks fire on kgo goroutines while the collector
// snapshots. Every observation must land in exactly one window — none lost,
// none counted twice. Run with -race.
func TestConcurrentHooksAndSnapshots(t *testing.T) {
	const (
		writers = 8
		perLoop = 500
	)
	h := newRPCHooks(epoch)

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		stop  = make(chan struct{})
	)

	wg.Add(writers)
	for w := range writers {
		go func(w int) {
			defer wg.Done()
			<-start
			node := int32(w % 3)
			for range perLoop {
				h.OnBrokerE2E(meta(node), metadataKey, e2e(time.Millisecond, 0, 0, time.Millisecond, 1, 2))
				h.OnBrokerConnect(meta(node), time.Millisecond, nil, nil)
				h.OnBrokerDisconnect(meta(node), nil)
				h.OnBrokerThrottle(meta(node), time.Millisecond, false)
			}
		}(w)
	}

	// Snapshots run concurrently with the writers, and the totals below are
	// summed across every window: a swap that dropped or duplicated an
	// observation shows up as a total that is not writers*perLoop.
	var (
		requests, connects, disconnects, throttles uint64
		e2eCount, writeWaitCount, connectCount     int64
		bytesWritten, bytesRead, throttledMs       int64
	)
	collect := func(s *metrics.RPCStats) {
		for _, b := range s.Brokers {
			connects += b.ConnectAttempts
			disconnects += b.Disconnects
			throttles += b.ThrottleEvents
			throttledMs += b.ThrottledMs
			connectCount += b.ConnectLatency.Count
			for _, r := range b.Requests {
				requests += uint64(r.E2E.Count) + r.Errors
				e2eCount += r.E2E.Count
				writeWaitCount += r.WriteWait.Count
				bytesWritten += r.BytesWritten
				bytesRead += r.BytesRead
			}
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		at := epoch
		for {
			select {
			case <-stop:
				collect(h.snapshot(at))
				return
			default:
				at = at.Add(time.Second)
				collect(h.snapshot(at))
			}
		}
	}()

	close(start)
	wg.Wait()
	close(stop)
	<-done

	const want = writers * perLoop
	if requests != want || e2eCount != want || writeWaitCount != want {
		t.Errorf("requests %d, e2e %d, write_wait %d, want %d each", requests, e2eCount, writeWaitCount, want)
	}
	if connects != want || connectCount != want || disconnects != want || throttles != want {
		t.Errorf("connects %d/%d, disconnects %d, throttles %d, want %d each", connects, connectCount, disconnects, throttles, want)
	}
	if bytesWritten != want || bytesRead != 2*want || throttledMs != want {
		t.Errorf("bytes %d/%d, throttled %dms, want %d/%d/%d", bytesWritten, bytesRead, throttledMs, want, 2*want, want)
	}
}
