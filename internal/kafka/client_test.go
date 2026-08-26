package kafka

import (
	"testing"
	"time"
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
