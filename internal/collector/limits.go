package collector

import (
	"math"

	"kafka-metrics-agent/internal/metrics"
)

// Limits caps what one batch may contain.
//
// Every entity cap defaults to 0 = unlimited: a non-zero default would be an
// untested guess that silently changes the customer's core data on first
// deploy. Caps are per-parent, so the batch bound is a product — MaxGroups ×
// MaxOffsetsPerGroup bounds the term that dominates the payload.
//
// Only MaxTopics and MaxGroups reduce broker load, by shrinking the argument
// lists of ListStartOffsets/ListEndOffsets and DescribeGroups/FetchManyOffsets.
// MaxPartitionsPerTopic does NOT: kadm's List*Offsets take topic names, so the
// broker computes every partition regardless — it is a payload cap only.
type Limits struct {
	// MaxErrors bounds Batch.Errors. It is the one cap defaulted ON, and behind
	// deduplication it essentially never fires.
	MaxErrors int
	// MaxErrorSamples is how many verbatim occurrences of one deduplication key
	// are emitted before the rest fold into the exemplar's Count. Below 1 means 1.
	MaxErrorSamples int

	MaxTopics             int
	MaxPartitionsPerTopic int
	MaxGroups             int
	MaxMembersPerGroup    int
	MaxOffsetsPerGroup    int

	// MaxTransitionsPerGroup bounds the fast poll's per-group transition list and
	// the dwell samples kept beside it. Unlike the caps above it belongs to the
	// group-state watcher, which accumulates between cycles: it is what keeps a
	// rebalance storm — exactly when that list is longest — from growing without
	// bound.
	MaxTransitionsPerGroup int
}

// any reports whether any ENTITY cap is set, which decides whether the batch
// echoes a limits block. The caps with a non-zero default — the two error caps
// and MaxTransitionsPerGroup — are excluded because they would make any()
// unconditionally true and contradict Batch.Limits' "nil when every cap is
// unlimited". They are still echoed by wire() whenever the block is emitted.
func (l Limits) any() bool {
	return l.MaxTopics != 0 || l.MaxPartitionsPerTopic != 0 || l.MaxGroups != 0 ||
		l.MaxMembersPerGroup != 0 || l.MaxOffsetsPerGroup != 0
}

func (l Limits) wire() *metrics.Limits {
	return &metrics.Limits{
		MaxErrors:             l.MaxErrors,
		MaxErrorSamples:       l.MaxErrorSamples,
		MaxTopics:             l.MaxTopics,
		MaxPartitionsPerTopic: l.MaxPartitionsPerTopic,
		MaxGroups:             l.MaxGroups,
		MaxMembersPerGroup:    l.MaxMembersPerGroup,
		MaxOffsetsPerGroup:    l.MaxOffsetsPerGroup,

		MaxTransitionsPerGroup: l.MaxTransitionsPerGroup,
	}
}

// errorSamples is MaxErrorSamples clamped to at least one; zero would emit no
// exemplar, leaving a Count with nothing to attach to.
func (l Limits) errorSamples() int {
	if l.MaxErrorSamples < 1 {
		return 1
	}
	return l.MaxErrorSamples
}

// perSectionErrorBudget bounds how many distinct entity-scoped errors one
// section accumulates in memory before mergeErrors, which bounds the wire, runs.
func (l Limits) perSectionErrorBudget() int {
	if l.MaxErrors <= 0 {
		return math.MaxInt
	}
	return l.MaxErrors
}

// capLen returns how many of n elements to keep and how many are dropped. A
// ceiling of zero or less means unlimited.
func capLen(n, ceiling int) (keep, dropped int) {
	if ceiling <= 0 || n <= ceiling {
		return n, 0
	}
	return ceiling, n - ceiling
}

// noBroker is the errKey broker sentinel. Entity-scoped errors carry no broker,
// and MinInt32 is not a reachable node ID, so they can never collide with a
// shard error from a real broker.
const noBroker = math.MinInt32

// maxDistinctErrorKeys is a ceiling on the deduplication map, so a broker
// inventing error codes cannot make it unbounded. Five admin APIs reach far
// fewer distinct keys than this.
const maxDistinctErrorKeys = 256

// errKey is the deduplication identity; section is implicit because dedup is
// per-section. brokerID is in the key because shard errors carry the ONLY
// per-broker attribution on the wire — no per-entity error_code says "broker 7
// timed out" — so collapsing thirty brokers' timeouts would destroy the only
// signal saying which broker is sick.
type errKey struct {
	api      string
	kind     string
	code     int16
	brokerID int32
}

func keyOf(ce metrics.CollectionError) errKey {
	k := errKey{api: ce.API, kind: ce.Kind, code: ce.Code, brokerID: noBroker}
	if ce.BrokerID != nil {
		k.brokerID = *ce.BrokerID
	}
	return k
}

// Admission priorities. Lower survives a budget.
//
// The ordering exists so a cap can never evict the only UNAUTHORIZED error in
// favour of the 4000th NOT_LEADER: authorization is a one-line human fix,
// whereas a leader election is visible per partition in topics[] anyway.
const (
	// prioRequest is a whole-API failure recorded by section.request. Dropping it
	// drops the reason the section is empty.
	prioRequest = iota
	prioAuth
	prioUnsupported
	prioCoordinator
	prioTransport
	// prioEntity is the entity-scoped kindOther flood: NOT_LEADER and friends,
	// each also visible as an error_code on its entity.
	prioEntity
)

func errPriority(ce metrics.CollectionError, requestScoped bool) int {
	if requestScoped {
		return prioRequest
	}
	switch ce.Kind {
	case kindAuthorization:
		return prioAuth
	case kindUnsupported:
		return prioUnsupported
	case kindCoordinator:
		return prioCoordinator
	case kindTransport:
		return prioTransport
	}
	return prioEntity
}

// mergeErrors enforces the GLOBAL error cap and flattens every section's errors
// into one canonically ordered slice.
//
// It runs once, after wg.Wait(): a budget shared between the collecting
// goroutines would make survival depend on scheduling, and would let an early
// flood from one phase starve the topics phase.
//
// Admission walks priority levels outermost so a tiny budget spends itself on
// the errors that carry information nothing else carries. Emission then walks
// sections in order and entries in their original index order, so the output
// stays byte-diffable across cycles despite the priority-ordered selection.
//
// It writes each section's rejection count back onto that section, so
// section.finish must run after this.
func mergeErrors(secs []*section, ceiling int) (out []metrics.CollectionError, dropped int) {
	total := 0
	admitted := make([][]bool, len(secs))
	for i, s := range secs {
		if s == nil {
			continue
		}
		admitted[i] = make([]bool, len(s.errs))
		total += len(s.errs)
	}

	budget := total
	if ceiling > 0 && ceiling < total {
		budget = ceiling
	}

	left := budget
	for prio := prioRequest; prio <= prioEntity && left > 0; prio++ {
		for i, s := range secs {
			if s == nil {
				continue
			}
			for j := range s.errs {
				if left == 0 {
					break
				}
				if s.prios[j] == prio {
					admitted[i][j] = true
					left--
				}
			}
		}
	}

	out = make([]metrics.CollectionError, 0, budget)
	for i, s := range secs {
		if s == nil {
			continue
		}
		rejected := 0
		for j, ce := range s.errs {
			if admitted[i][j] {
				out = append(out, ce)
				continue
			}
			rejected++
		}
		if rejected > 0 {
			s.globalDropped = rejected
			s.dropped.ErrorsDropped += rejected
			s.truncated = true
			dropped += rejected
		}
	}
	return out, dropped
}
