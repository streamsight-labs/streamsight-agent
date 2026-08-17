package collector

import (
	"context"

	"kafka-metrics-agent/internal/metrics"
)

// collectOffsets fetches committed offsets for every listed group.
//
// This phase runs BEFORE end offsets are listed. Both halves of lag are sampled
// at different instants; taking the committed half first means the end offset is
// the fresher number and lag errs high instead of going negative.
//
// listDropped is how many groups MaxGroups removed from the shared listing, so
// this section reports itself truncated by exactly as much as groups[] does.
func (c *Collector) collectOffsets(ctx context.Context, ids []string, listDropped int, listErr error, internal map[string]bool) ([]metrics.ConsumerOffset, *section) {
	sec := c.newSection(sectionOffsets)
	defer sec.stop()

	if listDropped > 0 {
		sec.truncated = true
	}

	sec.request("ListGroups", listErr)
	if len(ids) == 0 {
		return nil, sec
	}

	// FetchManyOffsets never returns a top-level error; every failure is
	// attributed to a group in the response.
	fetched := c.client.Admin.FetchManyOffsets(ctx, ids...)

	result := make([]metrics.ConsumerOffset, 0, len(fetched))
	for _, id := range ids {
		resp, ok := fetched[id]
		if !ok {
			sec.recordGroup("OffsetFetch", id, errNoResponse)
			continue
		}
		if resp.Err != nil {
			// Emitted with its error code and nil Offsets: a coordinator that
			// will not answer must not read as a group that committed nothing.
			sec.recordGroup("OffsetFetch", id, resp.Err)
			result = append(result, metrics.ConsumerOffset{
				GroupID:   id,
				ErrorCode: errorCode(resp.Err),
			})
			continue
		}

		offsets := make([]metrics.PartitionOffset, 0, len(resp.Fetched))
		emitted, dropped := 0, 0
		for _, o := range resp.Fetched.Sorted() {
			if internal[o.Topic] && !c.opts.IncludeInternalTopics {
				continue
			}
			if !c.topics.allow(o.Topic) {
				continue
			}
			// The cap counts EMITTED offsets, so a filtered topic never consumes
			// budget. `continue` rather than `break`: the loop must keep counting
			// to produce a true offset_count.
			if ceiling := c.limits.MaxOffsetsPerGroup; ceiling > 0 && emitted >= ceiling {
				dropped++
				continue
			}

			po := metrics.PartitionOffset{
				Topic:       o.Topic,
				Partition:   o.Partition,
				Offset:      nullableOffset(o.At, o.Err),
				LeaderEpoch: o.LeaderEpoch,
				Metadata:    o.Metadata,
			}
			if o.Err != nil {
				po.ErrorCode = errorCode(o.Err)
				sec.recordPartition("OffsetFetch", o.Topic, o.Partition, o.Err)
			}
			offsets = append(offsets, po)
			emitted++
		}
		if dropped > 0 {
			sec.dropped.Offsets += dropped
			sec.truncated = true
		}

		result = append(result, metrics.ConsumerOffset{
			GroupID: id,
			Offsets: offsets,
			// The true post-filter, pre-cap count. offsets[] is the only entity
			// list with no other count to compare len() against.
			OffsetCount: emitted + dropped,
		})
	}

	return result, sec
}

// nullableOffset maps a broker-reported offset onto the wire representation.
//
// A negative offset is not an offset: for a commit it means "never committed
// here", and clamping it to 0 reports the entire retained backlog as lag for
// every new or reset group. For a listing it means the lookup failed. Both must
// travel as null.
func nullableOffset(v int64, err error) *int64 {
	if err != nil || v < 0 {
		return nil
	}
	offset := v
	return &offset
}
