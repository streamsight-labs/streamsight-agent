package kafka

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestRetryTimeoutIsBounded(t *testing.T) {
	tests := []struct {
		collection time.Duration
		want       time.Duration
	}{
		{collection: 0, want: minRetryTimeout},
		{collection: time.Second, want: minRetryTimeout},
		{collection: 10 * time.Second, want: 5 * time.Second},
		{collection: 10 * time.Minute, want: maxRetryTimeout},
	}
	for _, tt := range tests {
		if got := retryTimeout(tt.collection); got != tt.want {
			t.Errorf("retryTimeout(%s) = %s, want %s", tt.collection, got, tt.want)
		}
	}
}

func TestMetadataMinAgeIsBounded(t *testing.T) {
	tests := []struct {
		interval time.Duration
		want     time.Duration
	}{
		{interval: 0, want: metadataMinAgeFloor},
		{interval: time.Millisecond, want: metadataMinAgeFloor},
		{interval: 4 * time.Second, want: 2 * time.Second},
		{interval: time.Hour, want: metadataMinAgeCeil},
	}
	for _, tt := range tests {
		if got := metadataMinAge(tt.interval); got != tt.want {
			t.Errorf("metadataMinAge(%s) = %s, want %s", tt.interval, got, tt.want)
		}
	}
}

// A check that trusted the maximum would pass the mixed-version cluster, which
// is exactly the dangerous case.
func TestCheckGroupStateFilterTakesTheMinimumNotTheMaximum(t *testing.T) {
	tests := []struct {
		name    string
		offered []brokerVersion
		wantErr string
	}{
		{
			name:    "every broker current",
			offered: []brokerVersion{{nodeID: 1, version: 5}, {nodeID: 2, version: 4}},
		},
		{
			name:    "one old broker in a modern cluster",
			offered: []brokerVersion{{nodeID: 1, version: 5}, {nodeID: 2, version: 3}, {nodeID: 3, version: 5}},
			wantErr: "broker 2 offers ListGroups v3",
		},
		{
			name:    "single old broker",
			offered: []brokerVersion{{nodeID: 7, version: 0}},
			wantErr: "broker 7 offers ListGroups v0",
		},
		{
			// A tie must name the same broker on every run, or the failure
			// message churns across restarts of an unchanged cluster.
			name:    "ties name the lowest broker id",
			offered: []brokerVersion{{nodeID: 4, version: 2}, {nodeID: 9, version: 2}},
			wantErr: "broker 4 offers",
		},
		{
			name:    "an empty probe is a failure, not a pass",
			offered: nil,
			wantErr: "no broker answered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkGroupStateFilter(tt.offered)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("checkGroupStateFilter = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkGroupStateFilter = nil, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %v does not contain %q", err, tt.wantErr)
			}
			// Operators run Kafka releases, not wire versions.
			if !strings.Contains(err.Error(), listGroupsStatesFilterKafka) && strings.Contains(tt.wantErr, "offers") {
				t.Errorf("error %v does not name the required Kafka version", err)
			}
		})
	}
}

// A broker that cannot be interrogated is treated as unsupported: assuming it
// is fine would reintroduce the silent blow-up the probe exists to prevent, on
// the one broker already behaving oddly.
func TestListGroupsVersionsFailsClosed(t *testing.T) {
	t.Run("no brokers", func(t *testing.T) {
		if _, err := listGroupsVersions(kadm.BrokersApiVersions{}); err == nil {
			t.Fatal("an empty probe result was accepted")
		}
	})

	t.Run("broker whose probe errored", func(t *testing.T) {
		vs := kadm.BrokersApiVersions{
			3: kadm.BrokerApiVersions{NodeID: 3, Err: errors.New("dial tcp: connection refused")},
		}
		_, err := listGroupsVersions(vs)
		if err == nil {
			t.Fatal("a broker that did not answer was accepted")
		}
		if !strings.Contains(err.Error(), "broker 3") {
			t.Errorf("error %v does not name the broker", err)
		}
	})

	t.Run("broker that does not know ListGroups", func(t *testing.T) {
		// A zero-value BrokerApiVersions reports every key as absent, which is
		// how a broker predating the API would look.
		vs := kadm.BrokersApiVersions{1: kadm.BrokerApiVersions{NodeID: 1}}
		_, err := listGroupsVersions(vs)
		if err == nil {
			t.Fatal("a broker with no ListGroups support was accepted")
		}
		if !strings.Contains(err.Error(), "ListGroups") {
			t.Errorf("error %v does not say which API is missing", err)
		}
	})
}
