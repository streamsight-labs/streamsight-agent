package collector

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"kafka-metrics-agent/internal/kafka"
	"kafka-metrics-agent/internal/metrics"
)

const (
	// defaultGroupStatePollInterval is the tick used when none is configured.
	defaultGroupStatePollInterval = 5 * time.Second
	// minGroupStatePollInterval floors the tick. Every tick costs ONE SHARDED
	// ListGroups, which is one request per broker: at 1s on a 30-broker cluster
	// that is 30 requests a second, 900 per 30s collection interval, against a
	// collection cycle that issues a handful. Below a second the extra
	// resolution is not actionable and the cost stays linear in brokers.
	minGroupStatePollInterval = time.Second
	// maxGroupStateWatchErrors bounds the failure exemplars one window keeps.
	// Every failed poll is counted in MissedPolls regardless, so the extras cost
	// only their attribution.
	maxGroupStateWatchErrors = 16
)

// GroupStateWatchOptions configures the fast group-state poll.
type GroupStateWatchOptions struct {
	// PollInterval is the fast tick, floored at one second. It is the
	// resolution of every dwell sample — a PreparingRebalance shorter than this
	// is invisible — which is why the wire echoes it.
	PollInterval time.Duration

	// The group filter must be the pair the collector itself uses, or the fast
	// poll and groups[] disagree about which groups exist.
	GroupIncludeRegex string
	GroupExcludeRegex string

	// MaxGroups bounds how many groups one window tracks, mirroring the
	// collector's own cap so the watcher cannot outgrow the batch it rides on.
	MaxGroups int
	// MaxTransitionsPerGroup bounds the per-group transition list and the dwell
	// samples kept alongside it. A group flapping for an hour is exactly when
	// that list is longest, so this is what keeps a rebalance storm from
	// accumulating unboundedly between cycles.
	MaxTransitionsPerGroup int

	Logger *slog.Logger
}

// GroupStateWatcher polls ListGroups on its own fast tick and accumulates the
// state transitions a collection interval is far too coarse to see.
//
// It is a SECOND ticker, independent of the collection cycle: it shares only
// the kgo client, which is safe for concurrent use, and a mutex held for the
// length of one pointer swap. A slow cycle cannot delay a poll and a slow poll
// cannot delay a cycle. Collect drains what has accumulated onto the batch it
// is already building, so "one batch per collection interval" still holds.
//
// It ships raw observations and no percentile; see metrics.GroupStateWatch.
type GroupStateWatcher struct {
	// list is the poll itself, a field so the accumulator is testable without a
	// broker.
	list     func(context.Context) (kadm.ListedGroups, error)
	interval time.Duration
	groups   *filter

	maxGroups      int
	maxTransitions int
	log            *slog.Logger

	mu  sync.Mutex
	win *stateWindow
}

// NewGroupStateWatcher builds a watcher. It fails only on a malformed group
// regex.
//
// The caller must first prove the cluster populates ListedGroup.State: kadm
// fills it only from ListGroups v4 (KIP-518, Kafka 2.6+), which is
// kafka.Capabilities.SupportsGroupStatesFilter. Below that every observation is
// an empty state and every window is one meaningless transition per group,
// which is worse than nothing because it looks like data.
func NewGroupStateWatcher(client *kafka.Client, opts GroupStateWatchOptions) (*GroupStateWatcher, error) {
	groups, err := newFilter(opts.GroupIncludeRegex, opts.GroupExcludeRegex)
	if err != nil {
		return nil, err
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	w := &GroupStateWatcher{
		interval:       groupStatePollInterval(opts.PollInterval),
		groups:         groups,
		maxGroups:      opts.MaxGroups,
		maxTransitions: opts.MaxTransitionsPerGroup,
		log:            log,
	}
	w.win = w.newWindow(time.Now())
	w.list = func(ctx context.Context) (kadm.ListedGroups, error) {
		// Deliberately unfiltered by state, even when GROUP_STATES is set for
		// the collection cycle: a state filter makes the transitions this
		// watcher exists to observe unobservable, because a group leaving the
		// filtered set is indistinguishable from a group that was deleted.
		return client.Admin.ListGroups(ctx)
	}
	return w, nil
}

// groupStatePollInterval floors the configured tick; see
// minGroupStatePollInterval for the request cost that floor defends.
func groupStatePollInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultGroupStatePollInterval
	}
	if d < minGroupStatePollInterval {
		return minGroupStatePollInterval
	}
	return d
}

// Run polls until ctx is cancelled, then returns. The caller owns the
// goroutine, so the watcher stops exactly when the agent's signal context does.
//
// The first poll is immediate: a transition into a state means nothing without
// a baseline to transition from.
func (w *GroupStateWatcher) Run(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()

	w.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.poll(ctx)
		}
	}
}

// poll issues one listing and folds it into the open window. Its deadline is
// the tick itself, so a hung broker costs at most one poll instead of stacking
// requests behind it.
func (w *GroupStateWatcher) poll(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, w.interval)
	defer cancel()

	listed, err := w.list(ctx)
	w.observe(time.Now(), listed, err)
}

// observe folds one poll's answer into the open window.
func (w *GroupStateWatcher) observe(at time.Time, listed kadm.ListedGroups, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	win := w.win

	if err != nil {
		if len(win.errs) < maxGroupStateWatchErrors {
			win.errs = append(win.errs, err)
		}
		w.log.Debug("group state poll failed", "error", err)
	}
	// kadm returns the brokers that did answer alongside a *ShardErrors, so a
	// partial listing is still an observation; only a listing with nothing in it
	// is a lost poll.
	if len(listed) == 0 {
		win.missed++
		win.lastPollAt = at
		return
	}

	// Ticks the runtime coalesced, or a poll that overran its deadline. Dwell
	// totals are understated by exactly that span, so the gap is reported rather
	// than smoothed away.
	if !win.lastPollAt.IsZero() {
		if lost := int(at.Sub(win.lastPollAt)/w.interval) - 1; lost > 0 {
			win.missed += lost
		}
	}
	win.polls++
	win.lastPollAt = at

	for _, g := range listed.Sorted() {
		if isInternalGroup(g.Group) || !w.groups.allow(g.Group) {
			continue
		}
		gw := win.groups[g.Group]
		if gw == nil {
			if w.maxGroups > 0 && len(win.groups) >= w.maxGroups {
				win.groupsDropped = true
				continue
			}
			gw = win.newGroup()
			win.groups[g.Group] = gw
		}
		gw.coordinator = g.Coordinator
		gw.observe(at, g.State)
	}
}

// rotate takes the accumulated window and opens a fresh one starting at end.
//
// Groups seen in the window are CARRIED FORWARD — their current state and the
// instant they entered it — so a rebalance spanning a window boundary still
// yields one completed dwell sample, in the window it ends in. Resetting
// instead would right-censor every dwell longer than the collection interval,
// which is precisely the population a p99 is made of. Groups not seen are
// dropped, which is what bounds the carried map to the live group count.
// maxUnseenWindows is how many consecutive windows a group may go unobserved
// before it is forgotten. Two covers a transient run of failed polls without
// keeping a deleted group's state alive for long.
const maxUnseenWindows = 2

func (w *GroupStateWatcher) rotate(end time.Time) *stateWindow {
	w.mu.Lock()
	defer w.mu.Unlock()

	win := w.win
	next := w.newWindow(end)
	for id, g := range win.groups {
		// A group not seen in this window is either deleted or was hidden by
		// polls that all failed, and rotate cannot tell those apart. Dropping it
		// immediately would discard `since` and right-censor a dwell that is
		// still running — understating exactly the long rebalances a p99 is made
		// of, and doing it worst during the coordinator trouble that caused the
		// failed polls. Carry it a bounded number of windows instead.
		if g.lastSeen.IsZero() && g.unseen >= maxUnseenWindows {
			continue
		}
		carried := next.newGroup()
		carried.coordinator = g.coordinator
		carried.current = g.current
		carried.since = g.since
		if g.lastSeen.IsZero() {
			carried.unseen = g.unseen + 1
		}
		// lastSeen deliberately stays zero: it means "seen in THIS window", so a
		// carried group that is never seen again is emitted by no window and
		// leaves the map at the next rotate.
		next.groups[id] = carried
	}
	w.win = next
	return win
}

func (w *GroupStateWatcher) newWindow(start time.Time) *stateWindow {
	return &stateWindow{start: start, maxTransitions: w.maxTransitions, groups: map[string]*groupWatch{}}
}

// collectGroupStates drains the fast poll into the batch.
//
// w is nil when the watch is off or the cluster is too old to populate group
// state, in which case the section is emitted skipped like every other phase
// that did not run. This issues no request: they were paid for one per tick by
// the watcher, which is what the phase costs and what must be budgeted.
func (c *Collector) collectGroupStates(w *GroupStateWatcher) (*metrics.GroupStateWatch, *section) {
	if w == nil {
		sec := c.newSection(sectionGroupStates)
		sec.downgrade(metrics.SectionSkipped)
		sec.stop()
		return nil, sec
	}

	end := time.Now()
	win := w.rotate(end)

	// The section covers the WINDOW, not the drain: SampledAt is when the
	// watcher started observing and DurationMs is how long it observed for, so a
	// backend cannot mistake a 30s window for an instantaneous sample.
	sec := c.newSectionAt(sectionGroupStates, win.start)
	sec.duration = end.Sub(win.start)

	for _, e := range win.errs {
		// One poll answers for the whole cluster, so a failed one is a whole
		// request lost — but other polls in the same window did produce data,
		// which is exactly requestPartial's case.
		sec.requestPartial("ListGroups", e, win.polls > 0)
	}

	if win.polls == 0 {
		sec.downgrade(metrics.SectionSkipped)
		return nil, sec
	}

	watch, dropped := win.wire(end, w.interval)
	if dropped > 0 {
		sec.dropped.GroupStateTransitions = dropped
		sec.truncated = true
	}
	if win.groupsDropped {
		// Flag only, never a count: Truncation.Groups belongs to the collection
		// cycle's own listing cap, and adding here would describe the same
		// groups as dropped twice.
		sec.truncated = true
	}
	return watch, sec
}

// stateWindow is one drain's worth of observations, guarded by the watcher's
// mutex until rotate hands it over.
type stateWindow struct {
	start          time.Time
	lastPollAt     time.Time
	polls          int
	missed         int
	errs           []error
	maxTransitions int

	groups        map[string]*groupWatch
	groupsDropped bool
}

func (w *stateWindow) newGroup() *groupWatch {
	return &groupWatch{start: w.start, maxTransitions: w.maxTransitions, states: map[string]*stateDwell{}}
}

// groupWatch is one group's accumulation. since is when the current state was
// entered and MAY PREDATE start, so that a carried-forward dwell completes with
// its true duration.
type groupWatch struct {
	start          time.Time
	maxTransitions int

	coordinator int32
	first       string
	current     string
	since       time.Time
	lastSeen    time.Time

	transitions []metrics.GroupStateTransition
	// seen is the pre-cap transition count and stays true when the list is
	// truncated; dropped is what the cap refused. samples is how many completed
	// durations were kept, against the same budget.
	seen    int
	dropped int
	samples int
	// unseen counts consecutive windows in which no poll observed this group.
	// It separates a deleted group from a window whose polls all failed, which
	// look identical at rotate time.
	unseen int

	states map[string]*stateDwell
}

// stateDwell is one state's occupancy. observed counts only the part of each
// interval that fell INSIDE the window, so occupancy stays additive across
// windows and never double-counts a carried-forward interval. completedMs
// carries each completed interval's TRUE duration, which is what a percentile
// needs and which may therefore exceed observed.
type stateDwell struct {
	entries     int
	completed   int
	observed    time.Duration
	completedMs []int64
}

// observe records one sighting of one group.
func (g *groupWatch) observe(at time.Time, state string) {
	switch {
	case g.since.IsZero():
		// First sighting in this window, not carried forward: the group may
		// have held this state for hours, which is why the transition's From is
		// empty rather than a state name.
		g.first = state
		g.enter(at, "", state)
	case state != g.current:
		if g.first == "" {
			g.first = g.current
		}
		g.close(at)
		g.enter(at, g.current, state)
	case g.first == "":
		// Carried forward and unchanged: the state at the first poll of this
		// window is the state at its start.
		g.first = state
	}
	g.lastSeen = at
}

// enter opens an interval in state and records the transition into it.
func (g *groupWatch) enter(at time.Time, from, state string) {
	g.seen++
	if g.maxTransitions > 0 && len(g.transitions) >= g.maxTransitions {
		g.dropped++
	} else {
		g.transitions = append(g.transitions, metrics.GroupStateTransition{At: at, From: from, To: state})
	}
	g.current, g.since = state, at
	g.dwell(state).entries++
}

// close ends the open interval at at, crediting the window with the part of it
// that fell inside the window and the backend with its full duration.
func (g *groupWatch) close(at time.Time) {
	d := g.dwell(g.current)
	d.observed += observedIn(g.since, at, g.start)
	d.completed++
	// The samples are one per completed interval, so they need the same budget
	// as the transitions or the cap on that list buys nothing.
	if g.maxTransitions > 0 && g.samples >= g.maxTransitions {
		return
	}
	g.samples++
	d.completedMs = append(d.completedMs, at.Sub(g.since).Milliseconds())
}

func (g *groupWatch) dwell(state string) *stateDwell {
	d := g.states[state]
	if d == nil {
		d = &stateDwell{}
		g.states[state] = d
	}
	return d
}

// observedIn is the part of [since, until] lying inside the window. A
// carried-forward interval began before the window started, and its earlier
// part was already reported by the window that saw it.
func observedIn(since, until, windowStart time.Time) time.Duration {
	if since.Before(windowStart) {
		since = windowStart
	}
	if until.Before(since) {
		return 0
	}
	return until.Sub(since)
}

// wire shapes the window for the batch and reports how many transitions the cap
// refused.
func (w *stateWindow) wire(end time.Time, interval time.Duration) (*metrics.GroupStateWatch, int) {
	watch := &metrics.GroupStateWatch{
		WindowStart:    w.start,
		WindowEnd:      end,
		PollIntervalMs: interval.Milliseconds(),
		Polls:          w.polls,
		MissedPolls:    w.missed,
		Groups:         make([]metrics.GroupStateWindow, 0, len(w.groups)),
	}

	ids := make([]string, 0, len(w.groups))
	for id := range w.groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	dropped := 0
	for _, id := range ids {
		g := w.groups[id]
		if g.lastSeen.IsZero() {
			// Carried forward and never seen again: it belongs to no window.
			continue
		}
		// The interval still open at the edge closes at the LAST SIGHTING, not
		// at the window end: a group that vanished mid-window was not observed
		// after that, and claiming otherwise would invent occupancy.
		g.dwell(g.current).observed += observedIn(g.since, g.lastSeen, w.start)

		dropped += g.dropped
		transitions := g.transitions
		if transitions == nil {
			transitions = []metrics.GroupStateTransition{}
		}
		watch.Groups = append(watch.Groups, metrics.GroupStateWindow{
			GroupID:         id,
			Coordinator:     g.coordinator,
			StateAtStart:    g.first,
			StateAtEnd:      g.current,
			Transitions:     transitions,
			TransitionCount: g.seen,
			States:          g.dwells(),
		})
	}
	return watch, dropped
}

// dwells is the per-state occupancy, sorted by state name.
func (g *groupWatch) dwells() []metrics.GroupStateDwell {
	names := make([]string, 0, len(g.states))
	for name := range g.states {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]metrics.GroupStateDwell, 0, len(names))
	for _, name := range names {
		d := g.states[name]
		samples := d.completedMs
		if samples == nil {
			samples = []int64{}
		}
		out = append(out, metrics.GroupStateDwell{
			State:       name,
			Entries:     d.entries,
			Completed:   d.completed,
			ObservedMs:  d.observed.Milliseconds(),
			CompletedMs: samples,
		})
	}
	return out
}
