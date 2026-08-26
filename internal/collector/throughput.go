package collector

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

const windowAPI = "ListOffsetsAfterMilli"

// defaultWindowWidth backstops a hand-built Options; config.Load validates
// THROUGHPUT_WINDOW and never leaves it unset.
//
// A zero width would ask about "now": every partition answers -1, kadm re-lists
// every one of them as an end offset, and the cycle pays two fan-outs for a
// window of no width. A negative one would ask about the future.
const defaultWindowWidth = 5 * time.Minute

// collectThroughputWindow asks each leader for the first offset at or after one
// timestamp, so the produce rate over the window is measured BY THE BROKER.
//
// It is the only rate input in the batch that survives an agent restart.
// Differencing end_offset across two batches — the only alternative — is wrong
// whenever a cycle was dropped, the interval jittered or the agent's clock
// disagrees with the backend's, and on the first batch after a restart it
// cannot produce a number at all. Here both endpoints of the difference are
// broker-measured and both arrive in the SAME batch, so none of that enters.
//
// The agent ships the raw material and never divides: the timestamp asked
// about, and per partition the offset the broker matched plus the broker's own
// timestamp for it.
//
// It adds no ACL — the same API key as ListEndOffsets, authorized by DESCRIBE
// on TOPIC — but it does add a ListOffsets fan-out to every cycle it runs on,
// and TWO on a cluster whose partitions are mostly silent, because kadm
// re-lists every partition that answered -1 as an end-offset request. Its
// Metadata is normally free: kadm resolves the topic list through kgo's
// metadata cache, which this cycle's cluster phase has already filled. That is
// why run is the caller's toggle, cadence and capability gate: below ListOffsets
// v1 a broker answers with old-style offsets and no timestamp at all, with no
// error to say so, so kafka.Capabilities.SupportsListOffsetsAfterMilli is the
// only tell.
//
// run is false on the cycles between samples; the section is still built and
// returned, so "not collected" and "collected, empty" are never the same batch.
func (c *Collector) collectThroughputWindow(ctx context.Context, tds kadm.TopicDetails, width time.Duration, run bool) (*metrics.ThroughputWindow, *windowSample, *section) {
	sec := c.newSection(sectionTopicsWindow)
	defer sec.stop()

	if !run || tds == nil {
		sec.downgrade(metrics.SectionSkipped)
		return nil, nil, sec
	}

	// Same inputs as the topics phase, so the windows land on exactly the topics
	// in topics[] and a backend never gets a window with no join partner.
	selected, dropped := selectTopics(tds, c.topics, c.opts.IncludeInternalTopics, c.limits.MaxTopics)
	if dropped > 0 {
		// A flag, never a count: the topics phase counts the same topics into the
		// batch total.
		sec.truncated = true
	}
	if len(selected) == 0 {
		// Never fall through to a bare ListOffsetsAfterMilli: with no topic
		// arguments kadm lists the entire cluster, which is the opposite of a
		// filter. Same trap as collectTopics and collectLogDirs.
		sec.downgrade(metrics.SectionSkipped)
		return nil, nil, sec
	}

	names := make([]string, 0, len(selected))
	for _, td := range selected {
		names = append(names, td.Topic)
	}

	window := throughputWindow(time.Now(), width)
	listed, err := c.client.Admin.ListOffsetsAfterMilli(ctx, window.RequestedMs, names...)
	// The window is returned even when the request failed: it states what this
	// cycle asked about, which is true whatever the brokers answered.
	if !sec.requestPartial(windowAPI, err, len(listed) > 0) {
		return window, nil, sec
	}
	return window, &windowSample{sec: sec, listed: listed}, sec
}

// throughputWindow is the window one cycle asks about: a timestamp width before
// now, on the AGENT's clock. The broker's answer carries its own timestamp,
// which is the edge a rate is actually divided by.
func throughputWindow(at time.Time, width time.Duration) *metrics.ThroughputWindow {
	if width <= 0 {
		width = defaultWindowWidth
	}
	return &metrics.ThroughputWindow{
		RequestedMs: at.Add(-width).UnixMilli(),
		WidthMs:     width.Milliseconds(),
	}
}

// windowSample is one ListOffsetsAfterMilli result together with the section
// that owns its errors.
type windowSample struct {
	sec    *section
	listed kadm.ListedOffsets
}

// attach writes each partition's answer onto the topics the topics phase
// already built.
//
// It is a second step rather than part of collectThroughputWindow because the
// two have opposite ordering constraints: the REQUEST must be issued before the
// high watermarks are listed, since end_offset - window.offset is the record
// count in the window and a window sampled after the watermark makes it
// negative, while the partitions the answers land on do not exist until the
// topics phase has finished.
//
// It records this section's per-entity errors, so it must run before finalize.
// Nil-safe: a skipped or failed phase leaves every Window absent.
func (w *windowSample) attach(topics []metrics.TopicMetrics) {
	if w == nil {
		return
	}
	for i := range topics {
		w.attachTopic(&topics[i])
	}
}

func (w *windowSample) attachTopic(tm *metrics.TopicMetrics) {
	parts, ok := w.listed[tm.Name]
	if !ok {
		w.sec.recordTopic(windowAPI, tm.Name, errTopicNotListed)
		return
	}
	// kadm reports a topic-level failure as a synthetic partition -1 rather than
	// once per partition. Recorded here as the topic-scoped error it is, or a
	// topic that was deleted mid-cycle produces one entry per partition.
	if o, ok := parts[-1]; ok && o.Err != nil {
		w.sec.recordTopic(windowAPI, tm.Name, o.Err)
		return
	}
	for i := range tm.Partitions {
		p := &tm.Partitions[i]
		o, ok := parts[p.ID]
		if !ok {
			w.sec.recordPartition(windowAPI, tm.Name, p.ID, errNotListed)
			continue
		}
		p.Window = w.windowOffset(tm.Name, o)
	}
}

// windowOffset shapes one partition's answer.
//
// The partition's own ErrorCode is deliberately left alone: this phase is
// optional and off by default, and letting it stamp the field would make a
// partition whose three core offsets are all fine read as broken.
func (w *windowSample) windowOffset(topic string, o kadm.ListedOffset) *metrics.WindowOffset {
	win := &metrics.WindowOffset{LeaderEpoch: o.LeaderEpoch, ErrorCode: errorCode(o.Err)}
	if o.Err != nil {
		w.sec.recordPartition(windowAPI, topic, o.Partition, o.Err)
		return win
	}

	win.Offset = nullableOffset(o.Offset, nil)
	// A -1 TIMESTAMP is not a failure and is not recorded: it is how "no record
	// after the requested millisecond" comes back. kadm re-lists those partitions
	// as end offsets, so the pair is (offset = current end offset, timestamp
	// null), and a silent partition correctly yields a delta of zero.
	win.TimestampMs = nullableOffset(o.Timestamp, nil)
	if win.Offset == nil {
		// An OFFSET still at -1 with no error is different: it survived that
		// re-listing, and an end offset is never negative.
		w.sec.recordPartition(windowAPI, topic, o.Partition, errNegativeOffset)
	}
	return win
}
