package collector

import (
	"context"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// collectOffsets fetches committed offsets for every listed group.
//
// This phase runs BEFORE end offsets are listed. Both halves of lag are sampled
// at different instants; taking the committed half first means the end offset is
// the fresher number and lag errs high instead of going negative.
func (c *Collector) collectOffsets(ctx context.Context, ids []string, listErr error, internal map[string]bool) ([]metrics.ConsumerOffset, *section) {
	sec := c.newSection(sectionOffsets)
	defer sec.stop()

	sec.request("ListGroups", listErr)
	if len(ids) == 0 {
		return nil, sec
	}

	// FetchManyOffsets never returns a top-level error; every failure is
	// attributed to a group in the response.
	fetched := c.client.FetchManyOffsets(ctx, ids...)

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
		emitted := 0
		for _, o := range resp.Fetched.Sorted() {
			if internal[o.Topic] && !c.opts.IncludeInternalTopics {
				continue
			}
			if !c.topics.allow(o.Topic) {
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
		result = append(result, metrics.ConsumerOffset{
			GroupID: id,
			Offsets: offsets,
			// Equal to len(Offsets): nothing caps the list. Kept because offsets[]
			// is the only entity list with no other count to compare len()
			// against, so the field is what makes a short list detectable at all.
			OffsetCount: emitted,
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
