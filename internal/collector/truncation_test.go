package collector

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"kafka-metrics-agent/internal/metrics"
)

func offsetOf(v int64) *int64 { return &v }

// epochTopic is one partition of one topic at a given current leader epoch.
func epochTopic(name, id string, partition int32, epoch int32) metrics.TopicMetrics {
	return metrics.TopicMetrics{
		Name:       name,
		ID:         id,
		Partitions: []metrics.Partition{{ID: partition, LeaderEpoch: epoch}},
	}
}

func groupOffsets(groupID, topic string, partition int32, offset *int64, epoch int32) metrics.ConsumerOffset {
	return metrics.ConsumerOffset{
		GroupID: groupID,
		Offsets: []metrics.PartitionOffset{{
			Topic:       topic,
			Partition:   partition,
			Offset:      offset,
			LeaderEpoch: epoch,
		}},
	}
}

func TestEpochTriggersFireOnlyOnMismatch(t *testing.T) {
	// The trigger is the whole design: in steady state the epochs match and this
	// must issue nothing, or the probe becomes a per-cycle fan-out to every
	// leader for an event that is normally absent.
	topics := []metrics.TopicMetrics{epochTopic("orders", "abc", 0, 7)}

	tests := []struct {
		name   string
		offset metrics.ConsumerOffset
		want   bool
	}{
		{name: "epochs agree", offset: groupOffsets("g", "orders", 0, offsetOf(100), 7)},
		{name: "never committed", offset: groupOffsets("g", "orders", 0, nil, 5)},
		{name: "committed epoch unknown", offset: groupOffsets("g", "orders", 0, offsetOf(100), -1)},
		{name: "partition not in this batch", offset: groupOffsets("g", "other", 0, offsetOf(100), 5)},
		{name: "epochs differ", offset: groupOffsets("g", "orders", 0, offsetOf(100), 5), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := epochTriggers(topics, []metrics.ConsumerOffset{tt.offset}, 0)
			if (len(got) > 0) != tt.want {
				t.Fatalf("triggers = %+v, want any = %v", got, tt.want)
			}
			if !tt.want {
				return
			}
			trig := got[0]
			if trig.committedEpoch != 5 || trig.currentEpoch != 7 {
				t.Errorf("epochs = committed %d, current %d, want 5 and 7", trig.committedEpoch, trig.currentEpoch)
			}
			// The topic UUID travels so a later delete-and-recreate cannot
			// invalidate the proof.
			if trig.topicID != "abc" || trig.group != "g" {
				t.Errorf("trigger = %+v, want it attributed to group g on topic abc", trig)
			}
		})
	}
}

func TestEpochTriggersAreSortedAndCapped(t *testing.T) {
	topics := []metrics.TopicMetrics{
		{Name: "b", Partitions: []metrics.Partition{{ID: 0, LeaderEpoch: 9}, {ID: 1, LeaderEpoch: 9}}},
		{Name: "a", Partitions: []metrics.Partition{{ID: 0, LeaderEpoch: 9}}},
	}
	offsets := []metrics.ConsumerOffset{
		groupOffsets("z", "b", 1, offsetOf(1), 8),
		groupOffsets("y", "b", 0, offsetOf(1), 8),
		groupOffsets("x", "a", 0, offsetOf(1), 8),
	}

	got, dropped := epochTriggers(topics, offsets, 0)
	if dropped {
		t.Error("nothing was capped, so nothing may be reported dropped")
	}
	want := []struct {
		topic     string
		partition int32
	}{{"a", 0}, {"b", 0}, {"b", 1}}
	if len(got) != len(want) {
		t.Fatalf("triggers = %+v, want %d", got, len(want))
	}
	for i, w := range want {
		if got[i].topic != w.topic || got[i].partition != w.partition {
			t.Errorf("trigger %d = %s/%d, want %s/%d", i, got[i].topic, got[i].partition, w.topic, w.partition)
		}
	}

	// The cap keeps a stable prefix, so a persistent mismatch reports the same
	// partitions every cycle instead of churning.
	capped, dropped := epochTriggers(topics, offsets, 2)
	if len(capped) != 2 || !dropped {
		t.Fatalf("capped = %+v (dropped %v), want 2 and true", capped, dropped)
	}
	if capped[0].topic != "a" || capped[1].topic != "b" {
		t.Errorf("capped prefix = %+v, want the sorted head", capped)
	}
}

func TestEpochRoundsAskOneEpochPerPartition(t *testing.T) {
	// kadm's request maps (topic, partition) to ONE epoch, so groups that
	// committed the same partition at different epochs cannot share a request.
	trigs := []epochTrigger{
		{topic: "orders", partition: 0, group: "a", committedEpoch: 5},
		{topic: "orders", partition: 0, group: "b", committedEpoch: 5},
		{topic: "orders", partition: 0, group: "c", committedEpoch: 4},
		{topic: "orders", partition: 1, group: "a", committedEpoch: 5},
	}

	rounds, dropped := epochRounds(trigs)
	if dropped {
		t.Error("two distinct epochs fit inside the round budget")
	}
	if len(rounds) != 2 {
		t.Fatalf("rounds = %d, want 2 — one per distinct epoch on partition 0", len(rounds))
	}
	// Two groups agreeing on the epoch are one question, not two.
	if len(rounds[0].keys) != 2 || rounds[0].req["orders"][0] != 5 || rounds[0].req["orders"][1] != 5 {
		t.Errorf("first round = %+v, want both partitions at epoch 5 asked once", rounds[0].req)
	}
	if len(rounds[1].keys) != 1 || rounds[1].req["orders"][0] != 4 {
		t.Errorf("second round = %+v, want partition 0 at epoch 4", rounds[1].req)
	}
}

func TestEpochRoundsAreBounded(t *testing.T) {
	var trigs []epochTrigger
	for i := 0; i < maxEpochProbeRounds+3; i++ {
		trigs = append(trigs, epochTrigger{topic: "orders", partition: 0, committedEpoch: int32(i)})
	}
	rounds, dropped := epochRounds(trigs)
	if len(rounds) != maxEpochProbeRounds || !dropped {
		t.Errorf("rounds = %d (dropped %v), want the cap and true", len(rounds), dropped)
	}
}

func TestCollectEpochProbesIssuesNothingWithoutATrigger(t *testing.T) {
	// The nil client is the assertion: any path that reaches
	// OffsetForLeaderEpoch panics. Steady state must stay silent.
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	topics := []metrics.TopicMetrics{epochTopic("orders", "abc", 0, 7)}

	tests := []struct {
		name    string
		run     bool
		topics  []metrics.TopicMetrics
		offsets []metrics.ConsumerOffset
	}{
		{name: "capability missing or disabled", run: false, topics: topics,
			offsets: []metrics.ConsumerOffset{groupOffsets("g", "orders", 0, offsetOf(100), 5)}},
		{name: "epochs agree", run: true, topics: topics,
			offsets: []metrics.ConsumerOffset{groupOffsets("g", "orders", 0, offsetOf(100), 7)}},
		{name: "no groups", run: true, topics: topics},
		{name: "no topics", run: true,
			offsets: []metrics.ConsumerOffset{groupOffsets("g", "orders", 0, offsetOf(100), 5)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probes, sec := c.collectEpochProbes(context.Background(), tt.run, tt.topics, tt.offsets)
			if probes != nil {
				t.Errorf("probes = %+v, want none", probes)
			}
			if sec.status != metrics.SectionSkipped || sec.name != sectionEpochProbes {
				t.Errorf("section = %+v, want a skipped epoch_probes", sec.finish())
			}
			if len(sec.errs) != 0 {
				t.Errorf("a skipped section must record nothing, got %+v", sec.errs)
			}
		})
	}
}

func TestEpochProbeShapesTheAnswer(t *testing.T) {
	trig := epochTrigger{
		topic:           "orders",
		topicID:         "abc",
		partition:       0,
		group:           "payments",
		committedOffset: offsetOf(100),
		committedEpoch:  5,
		currentEpoch:    7,
	}

	t.Run("proven loss", func(t *testing.T) {
		sec := newSection(sectionEpochProbes)
		p := trig.probe(sec, epochAnswer{
			offset: kadm.OffsetForLeaderEpoch{NodeID: 2, Topic: "orders", LeaderEpoch: 5, EndOffset: 90},
			found:  true, usable: true,
		})
		if p.ReturnedEpoch == nil || *p.ReturnedEpoch != 5 || p.EndOffset == nil || *p.EndOffset != 90 {
			t.Fatalf("probe = %+v, want epoch 5 and end offset 90", p)
		}
		// The proof itself is the backend's to draw; the agent only has to make
		// it drawable.
		if !(p.ErrorCode == 0 && *p.CommittedOffset > *p.EndOffset) {
			t.Errorf("probe = %+v, want the proof condition expressible", p)
		}
		if p.NodeID == nil || *p.NodeID != 2 || p.TopicID != "abc" || p.GroupID != "payments" {
			t.Errorf("probe = %+v, want the leader, topic UUID and group attributed", p)
		}
		if len(sec.errs) != 0 {
			t.Errorf("a successful probe recorded %+v", sec.errs)
		}
	})

	t.Run("leader does not know the epoch", func(t *testing.T) {
		// -1 is a real answer, not a missing one: the end offset beside it
		// proves nothing, and shipping null would hide that the leader replied.
		sec := newSection(sectionEpochProbes)
		p := trig.probe(sec, epochAnswer{
			offset: kadm.OffsetForLeaderEpoch{NodeID: 2, Topic: "orders", LeaderEpoch: -1, EndOffset: -1},
			found:  true, usable: true,
		})
		if p.ReturnedEpoch == nil || *p.ReturnedEpoch != -1 {
			t.Errorf("returned_epoch = %v, want -1", p.ReturnedEpoch)
		}
		if p.EndOffset != nil {
			t.Errorf("end_offset = %v, want null rather than a negative offset", *p.EndOffset)
		}
	})

	t.Run("partition error", func(t *testing.T) {
		sec := newSection(sectionEpochProbes)
		p := trig.probe(sec, epochAnswer{
			offset: kadm.OffsetForLeaderEpoch{NodeID: 2, Topic: "orders", Err: kerr.NotLeaderForPartition},
			found:  true, usable: true,
		})
		if p.ErrorCode != kerr.NotLeaderForPartition.Code {
			t.Errorf("error_code = %d, want %d", p.ErrorCode, kerr.NotLeaderForPartition.Code)
		}
		if p.ReturnedEpoch != nil || p.EndOffset != nil {
			t.Errorf("probe = %+v, want no answer alongside an error", p)
		}
		if len(sec.errs) != 1 {
			t.Errorf("errors = %+v, want the partition failure attributed", sec.errs)
		}
	})

	t.Run("partition missing from the response", func(t *testing.T) {
		sec := newSection(sectionEpochProbes)
		p := trig.probe(sec, epochAnswer{found: false, usable: true})
		if p.NodeID != nil || p.ReturnedEpoch != nil || p.EndOffset != nil {
			t.Errorf("probe = %+v, want every answer field null", p)
		}
		if len(sec.errs) != 1 || sec.errs[0].Message != errEpochNotReturned.Error() {
			t.Errorf("errors = %+v, want the silent omission recorded", sec.errs)
		}
	})

	t.Run("whole request failed", func(t *testing.T) {
		// The failure was attributed once by sec.request; repeating it per
		// partition would bury it.
		sec := newSection(sectionEpochProbes)
		p := trig.probe(sec, epochAnswer{})
		if p.NodeID != nil || p.ReturnedEpoch != nil || p.EndOffset != nil {
			t.Errorf("probe = %+v, want every answer field null", p)
		}
		if len(sec.errs) != 0 {
			t.Errorf("errors = %+v, want none", sec.errs)
		}
		// The trigger itself still travels: the epoch mismatch is information.
		if p.CommittedEpoch != 5 || p.CurrentLeaderEpoch != 7 {
			t.Errorf("probe = %+v, want the trigger preserved", p)
		}
	})
}

// The suppression set is the phase's memory, and the only thing standing
// between a rolling restart and a permanent fan-out. These pin the property
// that makes it safe: it remembers ANSWERS, never asks.
func TestProbedEpochsRemembersAnswersNotAsks(t *testing.T) {
	k := epochProbeKey{topic: "orders", topicID: "abc", partition: 0, committed: 5, current: 7}

	t.Run("asking does not retire the question", func(t *testing.T) {
		var p probedEpochs
		if p.answered(k) {
			t.Fatal("a fresh set answered a question it never saw")
		}
		// The whole bug: this used to record on the read, so a probe whose
		// request then failed was never re-asked.
		if p.answered(k) {
			t.Error("answered recorded the question it was only asked about")
		}
	})

	t.Run("an answer retires it", func(t *testing.T) {
		var p probedEpochs
		p.remember(k)
		if !p.answered(k) {
			t.Error("remembered question was not retired")
		}
	})

	t.Run("remember is idempotent across groups", func(t *testing.T) {
		// Two groups mismatching identically on one partition share a probeKey
		// (it carries no group), ask one question, and both record it.
		var p probedEpochs
		p.remember(k)
		p.remember(k)
		if len(p.seen) != 1 {
			t.Errorf("len(seen) = %d, want 1", len(p.seen))
		}
	})

	t.Run("a different question is still open", func(t *testing.T) {
		var p probedEpochs
		p.remember(k)
		// A later election moves the current epoch on, which is a new question
		// about the same partition and must be asked.
		next := k
		next.current = 8
		if p.answered(next) {
			t.Error("a new current epoch inherited the old question's answer")
		}
		// So does a delete-and-recreate, which is why topicID is in the key.
		recreated := k
		recreated.topicID = "def"
		if p.answered(recreated) {
			t.Error("a recreated topic inherited the old topic's answer")
		}
	})

	t.Run("evicts wholesale when full", func(t *testing.T) {
		var p probedEpochs
		for i := 0; i < maxProbedEpochs; i++ {
			p.remember(epochProbeKey{topic: "t", partition: int32(i)})
		}
		if len(p.seen) != maxProbedEpochs {
			t.Fatalf("len(seen) = %d, want %d", len(p.seen), maxProbedEpochs)
		}
		p.remember(k)
		if len(p.seen) != 1 || !p.answered(k) {
			t.Errorf("len(seen) = %d, want a clean slate holding only the new key", len(p.seen))
		}
	})
}

func TestEpochAnswerSettlesOnlyOnALeaderAnswer(t *testing.T) {
	tests := []struct {
		name string
		a    epochAnswer
		want bool
	}{
		{
			name: "leader answered",
			a:    epochAnswer{offset: kadm.OffsetForLeaderEpoch{LeaderEpoch: 5, EndOffset: 90}, found: true, usable: true},
			want: true,
		},
		{
			// -1 is a real answer: re-asking cannot change it.
			name: "leader does not know the epoch",
			a:    epochAnswer{offset: kadm.OffsetForLeaderEpoch{LeaderEpoch: -1, EndOffset: -1}, found: true, usable: true},
			want: true,
		},
		{
			name: "whole request failed",
			a:    epochAnswer{found: true, usable: false},
			want: false,
		},
		{
			name: "response omitted the partition",
			a:    epochAnswer{found: false, usable: true},
			want: false,
		},
		{
			// NOT_LEADER_FOR_PARTITION during the very election that triggered
			// the probe is the case this exists for.
			name: "leader returned an error code",
			a:    epochAnswer{offset: kadm.OffsetForLeaderEpoch{Err: kerr.NotLeaderForPartition}, found: true, usable: true},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.settles(); got != tt.want {
				t.Errorf("settles() = %t, want %t", got, tt.want)
			}
		})
	}
}
