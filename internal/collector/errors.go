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

// Section names. They are part of the wire format via Section.Name and
// CollectionError.Section, so they are constants, not literals.
const (
	sectionCluster = "cluster"
	sectionTopics  = "topics"
	// sectionTopicsEnd covers the ListEndOffsets call alone. High watermarks
	// are the rate-bearing field, and they are sampled measurably later than
	// the rest of the topics phase — after committed offsets complete. Giving
	// them their own SampledAt is what lets a backend divide an end-offset
	// delta by the right interval instead of one stamped a few hundred
	// milliseconds early.
	sectionTopicsEnd = "topics_end"
	sectionGroups    = "groups"
	sectionOffsets   = "offsets"
)

// Coarse error kinds carried in CollectionError.Kind. A backend routes on
// these without parsing Message: "authorization" is a human ACL fix,
// "coordinator" is usually self-healing, "transport" is infrastructure.
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
	switch kerr.ErrorForCode(code) {
	case kerr.TopicAuthorizationFailed,
		kerr.GroupAuthorizationFailed,
		kerr.ClusterAuthorizationFailed,
		kerr.TransactionalIDAuthorizationFailed,
		kerr.DelegationTokenAuthorizationFailed:
		return kindAuthorization
	case kerr.CoordinatorNotAvailable,
		kerr.NotCoordinator,
		kerr.CoordinatorLoadInProgress:
		return kindCoordinator
	case kerr.UnsupportedVersion,
		kerr.UnsupportedForMessageFormat:
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
type section struct {
	name     string
	started  time.Time
	duration time.Duration
	status   metrics.SectionStatus
	errs     []metrics.CollectionError
}

func newSection(name string) *section { return newSectionAt(name, time.Now()) }

// newSectionAt stamps SampledAt explicitly, for phases whose first request was
// issued before the phase body was entered.
func newSectionAt(name string, at time.Time) *section {
	return &section{name: name, started: at, status: metrics.SectionOK}
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
		ErrorCount: len(s.errs),
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

// add records an entity-scoped failure. One failing partition or group is a
// partial section, never a failed one — the rest of the data is still true.
func (s *section) add(ce metrics.CollectionError) {
	s.errs = append(s.errs, ce)
	s.downgrade(metrics.SectionPartial)
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

func (s *section) recordGroup(api, group string, err error) {
	ce := s.newError(api, err)
	ce.Group = group
	s.add(ce)
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

	var authErr *kadm.AuthError
	if errors.As(err, &authErr) {
		s.errs = append(s.errs, s.newError(api, err))
		s.downgrade(metrics.SectionUnauthorized)
		return false
	}

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
			s.errs = append(s.errs, ce)
		}
		switch {
		case unauthorized:
			s.downgrade(metrics.SectionUnauthorized)
		case shardErrs.AllFailed:
			s.downgrade(metrics.SectionFailed)
		default:
			s.downgrade(metrics.SectionPartial)
		}
		return !shardErrs.AllFailed && !unauthorized
	}

	ce := s.newError(api, err)
	s.errs = append(s.errs, ce)
	if ce.Kind == kindAuthorization {
		s.downgrade(metrics.SectionUnauthorized)
	} else {
		s.downgrade(metrics.SectionFailed)
	}
	return false
}
