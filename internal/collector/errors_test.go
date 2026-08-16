package collector

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-metrics-agent/internal/metrics"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantKind string
		wantCode int16
	}{
		{name: "nil", err: nil, wantKind: "", wantCode: 0},
		{name: "topic authorization", err: kerr.TopicAuthorizationFailed, wantKind: kindAuthorization, wantCode: 29},
		{name: "group authorization", err: kerr.GroupAuthorizationFailed, wantKind: kindAuthorization, wantCode: 30},
		{name: "cluster authorization", err: kerr.ClusterAuthorizationFailed, wantKind: kindAuthorization, wantCode: 31},
		{name: "kadm auth wrapper", err: &kadm.AuthError{Err: kerr.GroupAuthorizationFailed}, wantKind: kindAuthorization, wantCode: 30},
		{name: "not coordinator", err: kerr.NotCoordinator, wantKind: kindCoordinator, wantCode: 16},
		{name: "coordinator loading", err: kerr.CoordinatorLoadInProgress, wantKind: kindCoordinator, wantCode: 14},
		{name: "coordinator unavailable", err: kerr.CoordinatorNotAvailable, wantKind: kindCoordinator, wantCode: 15},
		{name: "unsupported version", err: kerr.UnsupportedVersion, wantKind: kindUnsupported, wantCode: 35},
		{name: "leader not available is not special", err: kerr.LeaderNotAvailable, wantKind: kindOther, wantCode: 5},
		{name: "deadline", err: context.DeadlineExceeded, wantKind: kindTransport, wantCode: 0},
		{name: "canceled", err: context.Canceled, wantKind: kindTransport, wantCode: 0},
		{name: "wrapped deadline", err: errors.Join(errors.New("dial"), context.DeadlineExceeded), wantKind: kindTransport, wantCode: 0},
		{name: "net error", err: &net.OpError{Op: "dial", Err: errors.New("refused")}, wantKind: kindTransport, wantCode: 0},
		{name: "plain error", err: errors.New("boom"), wantKind: kindOther, wantCode: 0},
		{name: "wrapped kafka error", err: errors.New("x: " + kerr.NotCoordinator.Error()), wantKind: kindOther, wantCode: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, code := classify(tt.err)
			if kind != tt.wantKind {
				t.Errorf("kind = %q, want %q", kind, tt.wantKind)
			}
			if code != tt.wantCode {
				t.Errorf("code = %d, want %d", code, tt.wantCode)
			}
		})
	}
}

func TestErrorCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int16
	}{
		{name: "nil", err: nil, want: 0},
		{name: "kafka error", err: kerr.NotCoordinator, want: 16},
		{name: "wrapped kafka error", err: errors.Join(errors.New("ctx"), kerr.GroupAuthorizationFailed), want: 30},
		{name: "non kafka error", err: errors.New("boom"), want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorCode(tt.err); got != tt.want {
				t.Errorf("errorCode() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSectionRequest(t *testing.T) {
	nodeID := func(id int32) kadm.BrokerDetail { return kgo.BrokerMetadata{NodeID: id} }

	tests := []struct {
		name       string
		err        error
		wantUsable bool
		wantStatus metrics.SectionStatus
		wantErrs   int
		wantBroker *int32
	}{
		{
			name:       "success",
			err:        nil,
			wantUsable: true,
			wantStatus: metrics.SectionOK,
		},
		{
			name:       "one shard of many failed keeps the rest",
			err:        &kadm.ShardErrors{Name: "ListGroups", AllFailed: false, Errs: []kadm.ShardError{{Err: errors.New("dial tcp: refused"), Broker: nodeID(7)}}},
			wantUsable: true,
			wantStatus: metrics.SectionPartial,
			wantErrs:   1,
			wantBroker: int32Ptr(7),
		},
		{
			name:       "every shard failed",
			err:        &kadm.ShardErrors{Name: "ListGroups", AllFailed: true, Errs: []kadm.ShardError{{Err: errors.New("dial"), Broker: nodeID(1)}, {Err: errors.New("dial"), Broker: nodeID(2)}}},
			wantUsable: false,
			wantStatus: metrics.SectionFailed,
			wantErrs:   2,
			wantBroker: int32Ptr(1),
		},
		{
			name:       "unmapped shard has no broker attribution",
			err:        &kadm.ShardErrors{Name: "ListGroups", Errs: []kadm.ShardError{{Err: errors.New("no broker"), Broker: nodeID(-1)}}},
			wantUsable: true,
			wantStatus: metrics.SectionPartial,
			wantErrs:   1,
			wantBroker: nil,
		},
		{
			name:       "shard authorization failure outranks partial",
			err:        &kadm.ShardErrors{Name: "ListGroups", Errs: []kadm.ShardError{{Err: kerr.GroupAuthorizationFailed, Broker: nodeID(3)}}},
			wantUsable: false,
			wantStatus: metrics.SectionUnauthorized,
			wantErrs:   1,
			wantBroker: int32Ptr(3),
		},
		{
			name:       "kadm auth error",
			err:        &kadm.AuthError{Err: kerr.TopicAuthorizationFailed},
			wantUsable: false,
			wantStatus: metrics.SectionUnauthorized,
			wantErrs:   1,
		},
		{
			name:       "plain request failure",
			err:        errors.New("connection reset"),
			wantUsable: false,
			wantStatus: metrics.SectionFailed,
			wantErrs:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sec := newSection(sectionGroups)
			usable := sec.request("ListGroups", tt.err)

			if usable != tt.wantUsable {
				t.Errorf("usable = %v, want %v", usable, tt.wantUsable)
			}
			if sec.status != tt.wantStatus {
				t.Errorf("status = %q, want %q", sec.status, tt.wantStatus)
			}
			if len(sec.errs) != tt.wantErrs {
				t.Fatalf("errors = %d, want %d", len(sec.errs), tt.wantErrs)
			}
			if tt.wantErrs == 0 {
				return
			}
			got := sec.errs[0]
			if got.Section != sectionGroups {
				t.Errorf("Section = %q, want %q", got.Section, sectionGroups)
			}
			if got.API != "ListGroups" {
				t.Errorf("API = %q, want ListGroups", got.API)
			}
			switch {
			case tt.wantBroker == nil && got.BrokerID != nil:
				t.Errorf("BrokerID = %d, want nil", *got.BrokerID)
			case tt.wantBroker != nil && got.BrokerID == nil:
				t.Errorf("BrokerID = nil, want %d", *tt.wantBroker)
			case tt.wantBroker != nil && *got.BrokerID != *tt.wantBroker:
				t.Errorf("BrokerID = %d, want %d", *got.BrokerID, *tt.wantBroker)
			}
		})
	}
}

func TestSectionStatusOnlyDowngrades(t *testing.T) {
	sec := newSection(sectionOffsets)
	if sec.status != metrics.SectionOK {
		t.Fatalf("new section status = %q, want ok", sec.status)
	}

	sec.recordGroup("OffsetFetch", "app", kerr.NotCoordinator)
	if sec.status != metrics.SectionPartial {
		t.Fatalf("status = %q, want partial", sec.status)
	}

	sec.downgrade(metrics.SectionOK)
	if sec.status != metrics.SectionPartial {
		t.Fatalf("status = %q after ok downgrade, want partial", sec.status)
	}

	sec.downgrade(metrics.SectionFailed)
	sec.downgrade(metrics.SectionPartial)
	if sec.status != metrics.SectionFailed {
		t.Fatalf("status = %q, want failed", sec.status)
	}

	sec.downgrade(metrics.SectionUnauthorized)
	if sec.status != metrics.SectionUnauthorized {
		t.Fatalf("status = %q, want unauthorized", sec.status)
	}
}

func TestSectionAttribution(t *testing.T) {
	sec := newSection(sectionOffsets)
	sec.recordGroup("OffsetFetch", "payments", kerr.NotCoordinator)
	sec.recordPartition("OffsetFetch", "orders", 4, kerr.LeaderNotAvailable)
	sec.recordTopic("ListEndOffsets", "orders", errTopicNotListed)

	if len(sec.errs) != 3 {
		t.Fatalf("errors = %d, want 3", len(sec.errs))
	}
	if sec.errs[0].Group != "payments" || sec.errs[0].Code != 16 || sec.errs[0].Kind != kindCoordinator {
		t.Errorf("group error = %+v", sec.errs[0])
	}
	if sec.errs[1].Topic != "orders" || sec.errs[1].Partition == nil || *sec.errs[1].Partition != 4 {
		t.Errorf("partition error = %+v", sec.errs[1])
	}
	if sec.errs[2].Topic != "orders" || sec.errs[2].Partition != nil {
		t.Errorf("topic error = %+v", sec.errs[2])
	}

	sec.stop()
	s := sec.finish()
	if s.Name != sectionOffsets || s.ErrorCount != 3 || s.Status != metrics.SectionPartial {
		t.Errorf("finish() = %+v", s)
	}
	if s.SampledAt.IsZero() {
		t.Error("SampledAt must be stamped at phase start")
	}
}

func TestNilSectionFinishes(t *testing.T) {
	var sec *section
	if got := sec.finish(); got.Status != metrics.SectionSkipped {
		t.Errorf("nil section status = %q, want skipped", got.Status)
	}
}

func int32Ptr(v int32) *int32 { return &v }
