package collector

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"kafka-metrics-agent/internal/metrics"
)

// Section names, part of the wire format via Section.Name and
// CollectionError.Section.
//
// The four offset phases are split because each carries its own SampledAt and
// each is the denominator of a different quantity: an end-offset rate needs
// topics_end.sampled_at, read_committed lag is (lso - committed),
// open-transaction backlog is (end - lso), and the throughput window is
// (end - window.offset) over (topics_end.sampled_at - window.timestamp_ms).
// Several phases run on their own cadence or only when a trigger fires, so a
// backend must read each SampledAt rather than Batch.CollectedAt.
//
// The order of the block is the canonical wire order Collect emits them in,
// which is SAMPLE order; see finalize.
const (
	sectionCluster       = "cluster"
	sectionTopics        = "topics"
	sectionTopicsWindow  = "topics_window"
	sectionTopicsLSO     = "topics_lso"
	sectionTopicsEnd     = "topics_end"
	sectionTopicsMaxTS   = "topics_max_timestamp"
	sectionTopicsLocal   = "topics_local_start"
	sectionTopicsRemote  = "topics_remote_end"
	sectionGroups        = "groups"
	sectionOffsets       = "offsets"
	sectionGroupStates   = "group_states"
	sectionEpochProbes   = "epoch_probes"
	sectionLogDirs       = "log_dirs"
	sectionReassignments = "reassignments"
	sectionTopicConfigs  = "topic_configs"
	sectionBrokerConfigs = "broker_configs"
	sectionShareGroups   = "share_groups"
	sectionBrokerRPC     = "broker_rpc"
)

// Coarse error kinds carried in CollectionError.Kind. A backend routes on these
// without parsing Message: "authorization" is a human ACL fix, "coordinator" is
// usually self-healing, "transport" is infrastructure.
const (
	kindAuthorization = "authorization"
	kindCoordinator   = "coordinator"
	kindTransport     = "transport"
	kindUnsupported   = "unsupported"
	kindOther         = "other"
)

var (
	errTopicNotListed = errors.New("broker returned no offsets for topic")
	errNotListed      = errors.New("broker returned no offset for partition")
	errNegativeOffset = errors.New("broker returned a negative offset with no error")
	errNoResponse     = errors.New("no offset fetch response for group")
	// errNoLogDirs is the only tell for a silent refusal. DescribeLogDirs grew a
	// top-level error code in v3 (Kafka 3.2); below that an unauthorized
	// principal receives an empty result and no error. No real broker runs with
	// zero log.dirs, so "answered, but with nothing" is a refusal.
	errNoLogDirs = errors.New("broker returned no log directories")
)

// classify maps an error to its alerting kind and, when the error came from a
// broker response, the Kafka error code behind it.
func classify(err error) (kind string, code int16) {
	if err == nil {
		return "", 0
	}

	var authErr *kadm.AuthError
	if errors.As(err, &authErr) {
		return kindAuthorization, errorCode(authErr.Err)
	}

	var kErr *kerr.Error
	if errors.As(err, &kErr) {
		return kindForCode(kErr.Code), kErr.Code
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return kindTransport, 0
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return kindTransport, 0
	}

	return kindOther, 0
}

func kindForCode(code int16) string {
	switch code {
	case kerr.TopicAuthorizationFailed.Code,
		kerr.GroupAuthorizationFailed.Code,
		kerr.ClusterAuthorizationFailed.Code,
		kerr.TransactionalIDAuthorizationFailed.Code,
		kerr.DelegationTokenAuthorizationFailed.Code:
		return kindAuthorization
	case kerr.CoordinatorNotAvailable.Code,
		kerr.NotCoordinator.Code,
		kerr.CoordinatorLoadInProgress.Code:
		return kindCoordinator
	case kerr.UnsupportedVersion.Code,
		kerr.UnsupportedForMessageFormat.Code:
		return kindUnsupported
	}
	return kindOther
}

// errorCode extracts the Kafka error code from err, or 0 when err did not come
// from a broker response.
func errorCode(err error) int16 {
	var kErr *kerr.Error
	if errors.As(err, &kErr) {
		return kErr.Code
	}
	return 0
}

// section accumulates the health, timing and attributed errors of one phase.
// It needs no synchronisation: each *section is written by exactly one
// collecting goroutine and read only after Collect's wg.Wait().
type section struct {
	name     string
	started  time.Time
	duration time.Duration
	status   metrics.SectionStatus
	lim      Limits

	// errs holds deduplicated representatives in insertion order; prios is
	// parallel to it and carries each entry's admission priority.
	errs  []metrics.CollectionError
	prios []int
	keys  map[errKey]*errBucket

	// globalDropped is the subset of dropped.ErrorsDropped that mergeErrors
	// refused; it is subtracted from ErrorCount because those entries never
	// reached the wire.
	dropped       metrics.Truncation
	globalDropped int
	truncated     bool
}

// errBucket tracks one deduplication key: repIdx is the exemplar's index in
// errs (-1 when the key was refused outright), samples is how many verbatim
// entries were emitted, seen is the total number of occurrences.
type errBucket struct {
	repIdx  int
	samples int
	seen    int
}

func newSection(name string) *section { return newSectionAt(name, time.Now()) }

// newSectionAt stamps SampledAt explicitly, for phases whose first request was
// issued before the phase body was entered.
func newSectionAt(name string, at time.Time) *section {
	return &section{name: name, started: at, status: metrics.SectionOK}
}

// The Collector's constructors stamp the configured caps onto the section; the
// package-level ones stay uncapped for tests.
func (c *Collector) newSection(name string) *section {
	return c.newSectionAt(name, time.Now())
}

func (c *Collector) newSectionAt(name string, at time.Time) *section {
	s := newSectionAt(name, at)
	s.lim = c.limits
	return s
}

func (s *section) stop() { s.duration = time.Since(s.started) }

func (s *section) finish() metrics.Section {
	if s == nil {
		return metrics.Section{Status: metrics.SectionSkipped}
	}
	return metrics.Section{
		Name:       s.name,
		Status:     s.status,
		SampledAt:  s.started,
		DurationMs: s.duration.Milliseconds(),
		// Only entries that reached Batch.Errors, so error_count means "entries
		// in batch.errors bearing this section's name".
		ErrorCount:      len(s.errs) - s.globalDropped,
		ErrorsCollapsed: s.dropped.ErrorsCollapsed,
		ErrorsDropped:   s.dropped.ErrorsDropped,
		Truncated:       s.truncated,
	}
}

// statusRank orders statuses so a section can only ever be downgraded.
// "unauthorized" outranks "failed" because it is the same outage with a
// specific, actionable cause.
func statusRank(s metrics.SectionStatus) int {
	switch s {
	case metrics.SectionOK:
		return 0
	case metrics.SectionSkipped:
		return 1
	case metrics.SectionPartial:
		return 2
	case metrics.SectionFailed:
		return 3
	case metrics.SectionUnauthorized:
		return 4
	}
	return 0
}

func (s *section) downgrade(status metrics.SectionStatus) {
	if statusRank(status) > statusRank(s.status) {
		s.status = status
	}
}

func (s *section) newError(api string, err error) metrics.CollectionError {
	kind, code := classify(err)
	return metrics.CollectionError{
		Section: s.name,
		API:     api,
		Kind:    kind,
		Code:    code,
		Message: err.Error(),
	}
}

// record is the single admission funnel every error path goes through. It
// deduplicates on (api, kind, kafka_error_code, broker_id) and enforces the
// per-section budget.
//
// Deduplication is always on, cap or no cap. Collapsing 4000 identical
// NOT_LEADERs into one exemplar plus a count loses only their individual
// attribution — still visible in topics[].partitions[] — and bounds errors[] to
// O(distinct failure modes) instead of O(failing entities).
func (s *section) record(ce metrics.CollectionError, requestScoped bool) {
	// The status downgrade happens FIRST and unconditionally. An occurrence
	// collapsed into an exemplar or refused by a cap is still a failure:
	// returning early above this line would make a cluster whose every
	// partition is offline report five "ok" sections.
	if !requestScoped {
		s.downgrade(metrics.SectionPartial)
	}

	prio := errPriority(ce, requestScoped)
	k := keyOf(ce)

	if b, ok := s.keys[k]; ok {
		b.seen++
		// A key refused outright has no exemplar to fold into. Re-running the
		// refusal below would increment ErrorsDropped once per occurrence,
		// turning "entries a cap refused" into an occurrence count.
		if b.repIdx < 0 {
			return
		}
		// Additional verbatim samples are still subject to the budget, so
		// MAX_ERROR_SAMPLES cannot multiply a section past its ceiling.
		if b.samples < s.lim.errorSamples() && (prio < prioEntity || len(s.errs) < s.lim.perSectionErrorBudget()) {
			s.emit(ce, prio)
			b.samples++
			return
		}
		// Fold into the exemplar, keeping the key's Counts summing to its
		// occurrences: each later sample stands for one, the exemplar takes the
		// rest.
		s.errs[b.repIdx].Count = b.seen - (b.samples - 1)
		s.dropped.ErrorsCollapsed++
		s.truncated = true
		return
	}

	// A new key is refused only when it is entity-scoped, as is the distinct-key
	// ceiling below. Applying either to every priority would let a section
	// already full of NOT_LEADER variants discard the single UNAUTHORIZED entry
	// naming the human-fixable cause of the outage.
	if s.keys == nil {
		s.keys = make(map[errKey]*errBucket)
	}
	mapFull := len(s.keys) >= maxDistinctErrorKeys
	if prio == prioEntity && (len(s.errs) >= s.lim.perSectionErrorBudget() || mapFull) {
		s.dropped.ErrorsDropped++
		s.truncated = true
		if !mapFull {
			// Remembered with no exemplar so later occurrences are recognised
			// rather than counted as fresh refusals. Past the ceiling nothing is
			// remembered and ErrorsDropped does count occurrences.
			s.keys[k] = &errBucket{repIdx: -1, seen: 1}
		}
		return
	}

	s.emit(ce, prio)
	s.keys[k] = &errBucket{repIdx: len(s.errs) - 1, samples: 1, seen: 1}
}

func (s *section) emit(ce metrics.CollectionError, prio int) {
	s.errs = append(s.errs, ce)
	s.prios = append(s.prios, prio)
}

// add records an entity-scoped failure. One failing partition or group is a
// partial section, never a failed one — the rest of the data is still true.
func (s *section) add(ce metrics.CollectionError) {
	s.record(ce, false)
}

func (s *section) recordTopic(api, topic string, err error) {
	ce := s.newError(api, err)
	ce.Topic = topic
	s.add(ce)
}

func (s *section) recordPartition(api, topic string, partition int32, err error) {
	ce := s.newError(api, err)
	ce.Topic = topic
	ce.Partition = &partition
	s.add(ce)
}

// recordLogDir attributes a storage failure to the broker and directory that
// reported it. Both are needed: the same path string exists on every broker.
// The directory is not part of the dedup key, so a broker with two dead mounts
// emits one exemplar with Count 2.
func (s *section) recordLogDir(api string, broker int32, dir string, err error) {
	ce := s.newError(api, err)
	ce.BrokerID = &broker
	ce.Dir = dir
	s.add(ce)
}

func (s *section) recordGroup(api, group string, err error) {
	ce := s.newError(api, err)
	ce.Group = group
	s.add(ce)
}

// requestPartial is request() for a call whose response can carry usable data
// alongside an error.
//
// kadm aborts its per-partition loop on the first TOPIC_AUTHORIZATION_FAILED
// and returns a bare *AuthError, while the offsets it had already decoded stay
// in the map it hands back. Without this, one topic outside a prefixed ACL
// nulls every offset in the batch.
//
// When data did survive, the status is corrected DOWN to partial — the one
// place the monotonic downgrade is deliberately reversed. "failed" and
// "unauthorized" both promise that the section carries no data at all, and that
// promise must not stand while entities decoded from the same response are on
// the wire. The authorization cause stays routable through the entry's Kind.
func (s *section) requestPartial(api string, err error, gotData bool) bool {
	if s.request(api, err) {
		return true
	}
	if !gotData {
		return false
	}
	switch s.status {
	case metrics.SectionFailed, metrics.SectionUnauthorized:
		s.status = metrics.SectionPartial
	}
	return true
}

// request records the outcome of one whole API call and reports whether any of
// the response is usable.
//
// kadm returns a POPULATED result alongside a *ShardErrors when only some
// brokers answered, so a shard failure must not discard the rest: one slow
// broker out of thirty is a partial section, not a cluster with no groups.
func (s *section) request(api string, err error) (usable bool) {
	if err == nil {
		return true
	}

	// ShardErrors is tested BEFORE AuthError, and the order is load-bearing.
	// kadm >= 1.12 gave *ShardErrors an Unwrap() []error, so errors.As(err,
	// **AuthError) now matches an authorization failure buried in a single
	// shard. Testing AuthError first would collapse a thirty-broker partial
	// failure into "unauthorized, no data".
	var shardErrs *kadm.ShardErrors
	if errors.As(err, &shardErrs) {
		unauthorized := false
		for _, se := range shardErrs.Errs {
			ce := s.newError(api, se.Err)
			if se.Broker.NodeID >= 0 {
				id := se.Broker.NodeID
				ce.BrokerID = &id
			}
			if ce.Kind == kindAuthorization {
				unauthorized = true
			}
			s.record(ce, true)
		}
		switch {
		case shardErrs.AllFailed && unauthorized:
			s.downgrade(metrics.SectionUnauthorized)
		case shardErrs.AllFailed:
			s.downgrade(metrics.SectionFailed)
		default:
			// Some shards answered; their data is kept even when another was
			// denied, so one topic the agent may not describe cannot blank out
			// every offset in the cluster.
			s.downgrade(metrics.SectionPartial)
		}
		return !shardErrs.AllFailed
	}

	var authErr *kadm.AuthError
	if errors.As(err, &authErr) {
		s.record(s.newError(api, err), true)
		s.downgrade(metrics.SectionUnauthorized)
		return false
	}

	ce := s.newError(api, err)
	s.record(ce, true)
	if ce.Kind == kindAuthorization {
		s.downgrade(metrics.SectionUnauthorized)
	} else {
		s.downgrade(metrics.SectionFailed)
	}
	return false
}
