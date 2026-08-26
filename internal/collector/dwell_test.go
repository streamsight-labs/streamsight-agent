package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/metrics"
)

var watchStart = time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)

// testWatcher builds a watcher whose window starts at a fixed instant, so every
// dwell in these tests is an exact number of milliseconds. The nil client is
// safe because no test calls poll.
func testWatcher(t *testing.T, opts GroupStateWatchOptions) *GroupStateWatcher {
	t.Helper()
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	w, err := NewGroupStateWatcher(nil, opts)
	if err != nil {
		t.Fatalf("NewGroupStateWatcher: %v", err)
	}
	w.win = w.newWindow(watchStart)
	return w
}

func listing(groups ...kadm.ListedGroup) kadm.ListedGroups {
	l := make(kadm.ListedGroups, len(groups))
	for _, g := range groups {
		l[g.Group] = g
	}
	return l
}

func at(sec int) time.Time { return watchStart.Add(time.Duration(sec) * time.Second) }

func group(id, state string) kadm.ListedGroup {
	return kadm.ListedGroup{Group: id, State: state, Coordinator: 3}
}

func drain(t *testing.T, w *GroupStateWatcher, end time.Time) *metrics.GroupStateWatch {
	t.Helper()
	watch, _ := w.rotate(end).wire(end, w.interval)
	return watch
}

func dwellOf(t *testing.T, g metrics.GroupStateWindow, state string) metrics.GroupStateDwell {
	t.Helper()
	for _, d := range g.States {
		if d.State == state {
			return d
		}
	}
	t.Fatalf("group %s has no dwell for %q, got %+v", g.GroupID, state, g.States)
	return metrics.GroupStateDwell{}
}

func TestGroupStateWatchShipsRawTransitionsAndDwells(t *testing.T) {
	w := testWatcher(t, GroupStateWatchOptions{})

	// Stable for 2s, PreparingRebalance for 3s, Stable again.
	for i, state := range []string{
		"Stable", "Stable",
		"PreparingRebalance", "PreparingRebalance", "PreparingRebalance",
		"Stable", "Stable",
	} {
		w.observe(at(i), listing(group("orders", state)), nil)
	}

	watch := drain(t, w, at(7))

	if watch.Polls != 7 || watch.MissedPolls != 0 {
		t.Errorf("polls = %d, missed = %d, want 7 and 0", watch.Polls, watch.MissedPolls)
	}
	if !watch.WindowStart.Equal(watchStart) || !watch.WindowEnd.Equal(at(7)) {
		t.Errorf("window = [%s, %s], want [%s, %s]", watch.WindowStart, watch.WindowEnd, watchStart, at(7))
	}
	if len(watch.Groups) != 1 {
		t.Fatalf("groups = %+v, want exactly one", watch.Groups)
	}

	g := watch.Groups[0]
	if g.Coordinator != 3 || g.StateAtStart != "Stable" || g.StateAtEnd != "Stable" {
		t.Errorf("group = %+v, want coordinator 3 and Stable at both edges", g)
	}
	want := []metrics.GroupStateTransition{
		{At: at(0), From: "", To: "Stable"},
		{At: at(2), From: "Stable", To: "PreparingRebalance"},
		{At: at(5), From: "PreparingRebalance", To: "Stable"},
	}
	if len(g.Transitions) != len(want) || g.TransitionCount != len(want) {
		t.Fatalf("transitions = %+v (count %d), want %+v", g.Transitions, g.TransitionCount, want)
	}
	for i, tr := range want {
		if g.Transitions[i] != tr {
			t.Errorf("transition %d = %+v, want %+v", i, g.Transitions[i], tr)
		}
	}

	// The rebalance is one completed 3s sample, which is the raw material for a
	// backend percentile. The agent must ship no percentile of its own.
	rebalance := dwellOf(t, g, "PreparingRebalance")
	if rebalance.Entries != 1 || rebalance.Completed != 1 {
		t.Errorf("PreparingRebalance = %+v, want one entry and one completion", rebalance)
	}
	if len(rebalance.CompletedMs) != 1 || rebalance.CompletedMs[0] != 3000 {
		t.Errorf("PreparingRebalance samples = %v, want [3000]", rebalance.CompletedMs)
	}
	if rebalance.ObservedMs != 3000 {
		t.Errorf("PreparingRebalance observed = %d, want 3000", rebalance.ObservedMs)
	}

	// Stable's second interval is still open at the edge, and closes at the LAST
	// SIGHTING (6s) rather than the window end (7s): the group was not observed
	// after that.
	stable := dwellOf(t, g, "Stable")
	if stable.Entries != 2 || stable.Completed != 1 {
		t.Errorf("Stable = %+v, want two entries and one completion", stable)
	}
	if stable.ObservedMs != 3000 {
		t.Errorf("Stable observed = %d, want 2000 closed + 1000 censored", stable.ObservedMs)
	}
	if len(stable.CompletedMs) != 1 || stable.CompletedMs[0] != 2000 {
		t.Errorf("Stable samples = %v, want [2000] — the censored interval is not a sample", stable.CompletedMs)
	}
}

func TestGroupStateWatchCarriesDwellAcrossWindowBoundary(t *testing.T) {
	// A rebalance longer than the collection interval is exactly the population
	// a p99 is made of. Resetting the tracker at the window boundary would
	// censor every one of them and bias the percentile down.
	w := testWatcher(t, GroupStateWatchOptions{})

	w.observe(at(0), listing(group("orders", "Stable")), nil)
	w.observe(at(10), listing(group("orders", "PreparingRebalance")), nil)
	first := drain(t, w, at(20))

	if d := dwellOf(t, first.Groups[0], "PreparingRebalance"); d.Completed != 0 || d.ObservedMs != 0 {
		// Closed at the last sighting (10s), so the open interval contributes
		// nothing yet and certainly no completed sample.
		t.Errorf("first window PreparingRebalance = %+v, want no completion", d)
	}

	w.observe(at(25), listing(group("orders", "PreparingRebalance")), nil)
	w.observe(at(55), listing(group("orders", "Stable")), nil)
	second := drain(t, w, at(60))

	g := second.Groups[0]
	if g.StateAtStart != "PreparingRebalance" || g.StateAtEnd != "Stable" {
		t.Errorf("second window edges = %q -> %q, want PreparingRebalance -> Stable", g.StateAtStart, g.StateAtEnd)
	}
	d := dwellOf(t, g, "PreparingRebalance")
	if len(d.CompletedMs) != 1 || d.CompletedMs[0] != 45000 {
		t.Errorf("samples = %v, want [45000] — the true dwell, which began in the previous window", d.CompletedMs)
	}
	// Occupancy counts only the part inside this window, so summing observed_ms
	// across windows never exceeds wall clock.
	if d.ObservedMs != 35000 {
		t.Errorf("observed = %d, want 35000 (window start to the transition)", d.ObservedMs)
	}
	// The first sighting of a carried group is not a transition: the state was
	// already held when the window opened.
	if g.TransitionCount != 1 || g.Transitions[0].From != "PreparingRebalance" {
		t.Errorf("transitions = %+v, want only the observed change", g.Transitions)
	}
}

func TestGroupStateWatchDropsGroupsItStopsSeeing(t *testing.T) {
	// The carried map is what could grow forever, so a group that disappears
	// must leave it.
	w := testWatcher(t, GroupStateWatchOptions{})
	w.observe(at(0), listing(group("orders", "Stable"), group("gone", "Empty")), nil)
	drain(t, w, at(5))

	w.observe(at(6), listing(group("orders", "Stable")), nil)
	watch := drain(t, w, at(10))

	if len(watch.Groups) != 1 || watch.Groups[0].GroupID != "orders" {
		t.Fatalf("groups = %+v, want only orders", watch.Groups)
	}

	// It is carried for a bounded number of windows first, because a group that
	// vanished and a window whose polls all failed look identical at rotate
	// time, and dropping it immediately right-censors a dwell still in flight.
	// What matters is that it leaves: the map must not grow forever.
	for i := 0; i < maxUnseenWindows; i++ {
		w.observe(at(11+i*5), listing(group("orders", "Stable")), nil)
		drain(t, w, at(15+i*5))
	}
	if len(w.win.groups) != 1 {
		t.Errorf("carried groups = %d, want only orders after %d unseen windows",
			len(w.win.groups), maxUnseenWindows)
	}
}

// A window whose polls all failed must not discard a dwell that is still
// running: the group is unobserved, not gone.
func TestGroupStateWatchSurvivesAWindowOfFailedPolls(t *testing.T) {
	w := testWatcher(t, GroupStateWatchOptions{})
	w.observe(at(0), listing(group("orders", "PreparingRebalance")), nil)
	drain(t, w, at(30))

	// Every poll in this window failed, so nothing was observed at all.
	w.observe(at(35), nil, errors.New("coordinator not available"))
	drain(t, w, at(60))

	w.observe(at(65), listing(group("orders", "Stable")), nil)
	watch := drain(t, w, at(90))

	if len(watch.Groups) != 1 {
		t.Fatalf("groups = %+v, want orders carried through the failed window", watch.Groups)
	}
	g := watch.Groups[0]
	if g.StateAtStart != "PreparingRebalance" {
		t.Errorf("state at start = %q, want the state carried across the failed window", g.StateAtStart)
	}
	// The rebalance began at t=0 and ended at t=65, so the completed sample must
	// span the failed window rather than starting from it.
	var completed int64
	for _, d := range g.States {
		if d.State == "PreparingRebalance" && len(d.CompletedMs) > 0 {
			completed = d.CompletedMs[0]
		}
	}
	if completed < 60000 {
		t.Errorf("completed dwell = %dms, want >= 60000: the failed window right-censored it", completed)
	}
}

func TestGroupStateWatchCapsTransitionsAndReportsIt(t *testing.T) {
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	w := testWatcher(t, GroupStateWatchOptions{MaxTransitionsPerGroup: 3})

	// A group flapping for the whole window: the cap is what bounds memory
	// between cycles.
	states := []string{"Stable", "PreparingRebalance"}
	for i := 0; i < 20; i++ {
		w.observe(at(i), listing(group("flap", states[i%2])), nil)
	}

	watch, sec := c.collectGroupStates(w)
	g := watch.Groups[0]

	if len(g.Transitions) != 3 {
		t.Errorf("transitions kept = %d, want the cap of 3", len(g.Transitions))
	}
	if g.TransitionCount != 20 {
		t.Errorf("transition_count = %d, want the true pre-cap 20", g.TransitionCount)
	}
	if sec.dropped.GroupStateTransitions != 17 || !sec.truncated {
		t.Errorf("section dropped %d (truncated %v), want 17 and true", sec.dropped.GroupStateTransitions, sec.truncated)
	}
	// The counts a rebalance rate is built from survive the cap; only the dwell
	// distribution is short.
	rebalance := dwellOf(t, g, "PreparingRebalance")
	if rebalance.Entries != 10 {
		t.Errorf("PreparingRebalance entries = %d, want the true 10", rebalance.Entries)
	}
	if len(rebalance.CompletedMs) > 3 {
		t.Errorf("samples = %v, want at most the cap", rebalance.CompletedMs)
	}
	// Nine of the ten entries closed inside the window; the count stays true
	// even though only three durations survived the cap.
	if rebalance.Completed != 9 {
		t.Errorf("completed = %d, want the true 9 even though the samples are short", rebalance.Completed)
	}
}

func TestGroupStateWatchBoundsTrackedGroups(t *testing.T) {
	w := testWatcher(t, GroupStateWatchOptions{MaxGroups: 2, GroupExcludeRegex: "^skip"})
	w.observe(at(0), listing(
		group("a", "Stable"),
		group("b", "Stable"),
		group("c", "Stable"),
		group("skipme", "Stable"),
		group("__internal", "Stable"),
	), nil)

	win := w.rotate(at(1))
	watch, _ := win.wire(at(1), w.interval)
	if len(watch.Groups) != 2 || watch.Groups[0].GroupID != "a" || watch.Groups[1].GroupID != "b" {
		t.Errorf("groups = %+v, want the sorted prefix [a b] — internal and excluded groups never reach the cap", watch.Groups)
	}
	if !win.groupsDropped {
		t.Error("the group cap fired but the window does not report itself truncated")
	}
}

func TestGroupStateWatchCountsMissedPolls(t *testing.T) {
	w := testWatcher(t, GroupStateWatchOptions{PollInterval: time.Second})

	w.observe(at(0), listing(group("orders", "Stable")), nil)
	// A poll that answered with nothing is lost, not an observation.
	w.observe(at(1), nil, errors.New("dial tcp: connection refused"))
	// Three seconds of silence at a one-second tick: two ticks never happened.
	w.observe(at(4), listing(group("orders", "Stable")), nil)

	watch := drain(t, w, at(5))
	if watch.Polls != 2 {
		t.Errorf("polls = %d, want 2", watch.Polls)
	}
	// One failed poll plus the two ticks the gap swallowed.
	if watch.MissedPolls != 3 {
		t.Errorf("missed = %d, want 3", watch.MissedPolls)
	}
	if watch.PollIntervalMs != 1000 {
		t.Errorf("poll_interval_ms = %d, want 1000", watch.PollIntervalMs)
	}
}

func TestCollectGroupStatesEmitsSectionEveryCycle(t *testing.T) {
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Run("watch disabled", func(t *testing.T) {
		watch, sec := c.collectGroupStates(nil)
		if watch != nil {
			t.Errorf("watch = %+v, want none", watch)
		}
		if sec.status != metrics.SectionSkipped || sec.name != sectionGroupStates {
			t.Errorf("section = %+v, want a skipped group_states", sec.finish())
		}
	})

	t.Run("no poll completed yet", func(t *testing.T) {
		// Enabled but nothing observed: skipped, so "not collected" stays
		// distinguishable from "collected, no groups".
		watch, sec := c.collectGroupStates(testWatcher(t, GroupStateWatchOptions{}))
		if watch != nil {
			t.Errorf("watch = %+v, want none", watch)
		}
		if sec.status != metrics.SectionSkipped {
			t.Errorf("status = %q, want skipped", sec.status)
		}
	})

	t.Run("some polls failed", func(t *testing.T) {
		w := testWatcher(t, GroupStateWatchOptions{})
		w.observe(at(0), listing(group("orders", "Stable")), nil)
		w.observe(at(1), nil, errors.New("broker unreachable"))

		watch, sec := c.collectGroupStates(w)
		if watch == nil || len(watch.Groups) != 1 {
			t.Fatalf("watch = %+v, want the groups the successful polls saw", watch)
		}
		// Data survived, so the section must not claim it has none.
		if sec.status != metrics.SectionPartial {
			t.Errorf("status = %q, want partial", sec.status)
		}
		if len(sec.errs) != 1 {
			t.Errorf("errors = %+v, want the failed poll attributed once", sec.errs)
		}
	})

	t.Run("every poll failed", func(t *testing.T) {
		w := testWatcher(t, GroupStateWatchOptions{})
		w.observe(at(0), nil, errors.New("broker unreachable"))

		_, sec := c.collectGroupStates(w)
		if sec.status != metrics.SectionFailed {
			t.Errorf("status = %q, want failed", sec.status)
		}
	})
}

func TestCollectGroupStatesSectionSpansTheWindow(t *testing.T) {
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	w := testWatcher(t, GroupStateWatchOptions{})
	w.win = w.newWindow(time.Now().Add(-30 * time.Second))
	w.observe(time.Now().Add(-20*time.Second), listing(group("orders", "Stable")), nil)

	_, sec := c.collectGroupStates(w)
	if sec.duration < 29*time.Second {
		t.Errorf("duration = %s, want the window length, not the drain", sec.duration)
	}
}

func TestGroupStatePollIntervalIsFloored(t *testing.T) {
	// Each tick is one request per broker, so the floor is a cost control.
	tests := []struct {
		in, want time.Duration
	}{
		{0, defaultGroupStatePollInterval},
		{time.Millisecond, minGroupStatePollInterval},
		{2 * time.Second, 2 * time.Second},
	}
	for _, tt := range tests {
		if got := groupStatePollInterval(tt.in); got != tt.want {
			t.Errorf("groupStatePollInterval(%s) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestGroupStateWatchDrainsWhileItPolls(t *testing.T) {
	// The second ticker is new architecture for this agent: under -race this is
	// the assertion that a collection cycle draining the window cannot corrupt a
	// poll folding into it.
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	w := testWatcher(t, GroupStateWatchOptions{})
	w.interval = time.Millisecond

	states := []string{"Stable", "PreparingRebalance", "CompletingRebalance"}
	var n int
	w.list = func(context.Context) (kadm.ListedGroups, error) {
		n++
		return listing(group("orders", states[n%len(states)])), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	for i := 0; i < 50; i++ {
		c.collectGroupStates(w)
	}
	cancel()
	<-done
}

func TestGroupStateWatchRunStopsWithItsContext(t *testing.T) {
	w := testWatcher(t, GroupStateWatchOptions{})
	w.interval = time.Millisecond

	polls := make(chan struct{}, 64)
	w.list = func(context.Context) (kadm.ListedGroups, error) {
		select {
		case polls <- struct{}{}:
		default:
		}
		return listing(group("orders", "Stable")), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	<-polls // the first poll is immediate, so a window always has a baseline
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}
