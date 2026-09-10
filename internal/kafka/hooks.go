package kafka

import (
	"cmp"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// maxConnectErrLen bounds LastConnectError. A TLS or DNS failure can carry a
// multi-line message with an address chain in it, and this field rides every
// batch for as long as the broker stays unreachable.
const maxConnectErrLen = 256

// kgo dispatches to a hook only if it implements the matching interface, and
// says nothing when it does not — a renamed method would disable the whole
// section silently. These assertions turn that into a compile error.
var (
	_ kgo.HookBrokerE2E        = (*rpcHooks)(nil)
	_ kgo.HookBrokerConnect    = (*rpcHooks)(nil)
	_ kgo.HookBrokerDisconnect = (*rpcHooks)(nil)
	_ kgo.HookBrokerThrottle   = (*rpcHooks)(nil)
)

// rpcHooks accumulates kgo's broker hooks into per-window counters. It observes
// traffic the agent already sends, so the whole signal costs zero requests and
// needs no ACL beyond the one that produced the traffic.
//
// Every hook fires on a kgo goroutine, concurrently with collection, so the
// accumulator sits behind one mutex rather than per-counter atomics: one
// observation updates several fields — bytes, error count, two histograms —
// that must move together, and a snapshot must be atomic against all of them.
// The critical section is a map lookup plus a bucket increment against a
// request rate of tens per collection interval, so contention is not a
// consideration here; consistency is.
type rpcHooks struct {
	mu sync.Mutex
	// since is when the current window opened: the last snapshot, or client
	// construction. It is deliberately not the collection interval — a slow
	// cycle or a missed tick makes the real window longer, and every rate the
	// backend derives divides by this.
	since   time.Time
	brokers map[int32]*brokerAccum
}

// brokerAccum is one broker's window. Nothing here is capped: the map is keyed
// by node ID and API key, both bounded by the cluster, and every snapshot
// discards it.
type brokerAccum struct {
	apis map[int16]*apiAccum

	connectAttempts uint64
	connectFailures uint64
	connectLatency  metrics.LatencyHistogram
	disconnects     uint64
	lastConnectErr  string

	throttleEvents uint64
	throttled      time.Duration
}

type apiAccum struct {
	e2e          metrics.LatencyHistogram
	writeWait    metrics.LatencyHistogram
	errors       uint64
	bytesWritten int64
	bytesRead    int64
}

// newRPCHooks opens the first window at start.
func newRPCHooks(start time.Time) *rpcHooks {
	return &rpcHooks{since: start, brokers: make(map[int32]*brokerAccum)}
}

// OnBrokerE2E records one request/response round trip.
//
// Latency is recorded as counts, a sum and fixed bucket bounds rather than as a
// percentile, because a percentile computed per cycle is a dead end: p99 does
// not average, sum or re-bucket, so a backend handed one p99 per 30s can never
// answer "p99 over the last hour" or "p99 across brokers". Counts and sums add
// exactly across cycles, across brokers and across agents.
func (h *rpcHooks) OnBrokerE2E(meta kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	h.mu.Lock()
	defer h.mu.Unlock()

	a := h.broker(meta.NodeID).api(key)
	// Bytes are counted even on failure: they left the agent either way, and
	// this is the agent's own cost against the broker.
	a.bytesWritten += int64(e2e.BytesWritten)
	a.bytesRead += int64(e2e.BytesRead)

	// A failed request's timings measure the failure, not the broker's service
	// time — a refused write returns in microseconds and would drag the latency
	// series down exactly when the broker is worst. Errors are their own
	// counter, so Count is deliberately "successful round trips", matching
	// ConnectLatency.
	if e2e.Err() != nil {
		a.errors++
		return
	}
	// DurationE2E excludes WriteWait by construction; WriteWait is agent-side
	// queueing and stays a separate series, or agent contention would read as a
	// broker problem.
	a.e2e.Observe(e2e.DurationE2E())
	a.writeWait.Observe(e2e.WriteWait)
}

// OnBrokerConnect records one dial attempt. kgo calls this once per attempt
// including its own retries, so attempts can exceed the number of connections
// the agent asked for.
func (h *rpcHooks) OnBrokerConnect(meta kgo.BrokerMetadata, initDur time.Duration, _ net.Conn, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	b := h.broker(meta.NodeID)
	b.connectAttempts++
	if err != nil {
		b.connectFailures++
		b.lastConnectErr = truncate(err.Error(), maxConnectErrLen)
		return
	}
	// initDur covers the dial plus ApiVersions plus any SASL flow, so this is
	// "time to a usable connection", not TCP handshake time.
	b.connectLatency.Observe(initDur)
}

// OnBrokerDisconnect records a closed connection.
func (h *rpcHooks) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.broker(meta.NodeID).disconnects++
}

// OnBrokerThrottle records broker-imposed quota throttling of the agent's own
// principal. kgo only calls this with a positive interval. Whether the broker
// applied the throttle before or after responding is client-side scheduling
// detail, not a fact about the quota, so it is not carried.
func (h *rpcHooks) OnBrokerThrottle(meta kgo.BrokerMetadata, interval time.Duration, _ bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	b := h.broker(meta.NodeID)
	b.throttleEvents++
	b.throttled += interval
}

// snapshot detaches the current window's counters and opens the next one.
//
// The swap happens under the same lock the hooks take, so an observation that
// lands mid-snapshot is recorded wholly in the window returned or wholly in the
// next one: it can be neither lost nor counted twice. The detached maps are
// unreachable from the accumulator afterwards, which is what makes it safe to
// convert them outside the lock.
func (h *rpcHooks) snapshot(now time.Time) *metrics.RPCStats {
	h.mu.Lock()
	brokers := h.brokers
	h.brokers = make(map[int32]*brokerAccum, len(brokers))
	window := now.Sub(h.since)
	h.since = now
	h.mu.Unlock()

	if window < 0 {
		window = 0
	}
	out := &metrics.RPCStats{
		WindowMs: window.Milliseconds(),
		Brokers:  make([]metrics.BrokerRPC, 0, len(brokers)),
	}
	for id, b := range brokers {
		row := metrics.BrokerRPC{
			BrokerID:         id,
			Requests:         make([]metrics.BrokerAPIRPC, 0, len(b.apis)),
			ConnectAttempts:  b.connectAttempts,
			ConnectFailures:  b.connectFailures,
			ConnectLatency:   b.connectLatency,
			Disconnects:      b.disconnects,
			LastConnectError: b.lastConnectErr,
			ThrottleEvents:   b.throttleEvents,
			ThrottledMs:      b.throttled.Milliseconds(),
		}
		// A broker that was never dialled this window still gets a row, so its
		// histogram must carry the bucket layout rather than a pair of nulls.
		row.ConnectLatency.EnsureBuckets()
		for key, a := range b.apis {
			// Same reason as ConnectLatency: an API whose every request failed
			// this window observed no latency but still ships a row.
			a.e2e.EnsureBuckets()
			a.writeWait.EnsureBuckets()
			row.Requests = append(row.Requests, metrics.BrokerAPIRPC{
				API:          apiName(key),
				E2E:          a.e2e,
				WriteWait:    a.writeWait,
				Errors:       a.errors,
				BytesWritten: a.bytesWritten,
				BytesRead:    a.bytesRead,
			})
		}
		slices.SortFunc(row.Requests, func(x, y metrics.BrokerAPIRPC) int { return strings.Compare(x.API, y.API) })
		out.Brokers = append(out.Brokers, row)
	}
	slices.SortFunc(out.Brokers, func(x, y metrics.BrokerRPC) int { return cmp.Compare(x.BrokerID, y.BrokerID) })
	return out
}

// broker returns a node's accumulator, creating it. The caller holds mu.
func (h *rpcHooks) broker(nodeID int32) *brokerAccum {
	b := h.brokers[nodeID]
	if b == nil {
		b = &brokerAccum{apis: make(map[int16]*apiAccum)}
		h.brokers[nodeID] = b
	}
	return b
}

// api returns an API key's accumulator, creating it. The caller holds mu.
func (b *brokerAccum) api(key int16) *apiAccum {
	a := b.apis[key]
	if a == nil {
		a = &apiAccum{}
		b.apis[key] = a
	}
	return a
}

// RPCSnapshot detaches the RPC counters gathered since the last call and opens
// a new window. Exactly one caller may take a snapshot per cycle: taking one
// resets the counters, so a second caller would silently halve the first's
// numbers.
//
// It returns nil when the hooks were not installed, which is the collector's
// cue to emit a skipped section rather than an empty one.
func (c *Client) RPCSnapshot() *metrics.RPCStats {
	if c.rpc == nil {
		return nil
	}
	return c.rpc.snapshot(time.Now())
}

// apiName is the kmsg request name in snake_case ("list_offsets"), which is
// what BrokerAPIRPC.API promises so no backend has to carry a key table. An
// unrecognised key keeps its number instead of collapsing every unknown key
// onto one row.
func apiName(key int16) string {
	name := kmsg.NameForKey(key)
	if name == "" || name == "Unknown" {
		return fmt.Sprintf("unknown_%d", key)
	}
	return snakeCase(name)
}

// snakeCase splits an ASCII Go-style name on word boundaries and lowercases it:
// "ListOffsets" to "list_offsets", "SASLHandshake" to "sasl_handshake". A
// plural after an acronym stays attached ("DescribeACLs" to "describe_acls").
func snakeCase(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i > 0 && isUpper(c) && startsWord(s, i) {
			b.WriteByte('_')
		}
		if isUpper(c) {
			c += 'a' - 'A'
		}
		b.WriteByte(c)
	}
	return b.String()
}

// startsWord reports whether the upper-case byte at i opens a new word: either
// it follows a non-upper byte, or it is the last letter of an acronym followed
// by a word ("SASLHandshake"). A lone trailing "s" does not count as that word,
// so "ACLs" stays one token.
func startsWord(s string, i int) bool {
	if !isUpper(s[i-1]) {
		return true
	}
	if i+1 >= len(s) || isUpper(s[i+1]) {
		return false
	}
	plural := s[i+1] == 's' && (i+2 >= len(s) || isUpper(s[i+2]))
	return !plural
}

func isUpper(c byte) bool { return c >= 'A' && c <= 'Z' }

// truncate cuts s to at most max bytes, dropping a partial trailing rune so the
// result is still valid UTF-8 and can be JSON encoded.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return strings.ToValidUTF8(s[:max], "")
}
