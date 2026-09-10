package collector

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

func TestCapLen(t *testing.T) {
	tests := []struct {
		name        string
		n, max      int
		keep, dropp int
	}{
		{name: "under the cap", n: 3, max: 10, keep: 3},
		{name: "exactly at the cap", n: 10, max: 10, keep: 10},
		{name: "over the cap", n: 12, max: 10, keep: 10, dropp: 2},
		{name: "zero max is unlimited", n: 5000, max: 0, keep: 5000},
		{name: "negative max is unlimited", n: 5000, max: -1, keep: 5000},
		{name: "nothing to cap", n: 0, max: 10, keep: 0},
		{name: "nothing to cap, unlimited", n: 0, max: 0, keep: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keep, dropped := capLen(tt.n, tt.max)
			if keep != tt.keep || dropped != tt.dropp {
				t.Errorf("capLen(%d, %d) = (%d, %d), want (%d, %d)", tt.n, tt.max, keep, dropped, tt.keep, tt.dropp)
			}
			if keep+dropped != tt.n {
				t.Errorf("keep+dropped = %d, want %d: the counts must account for every element", keep+dropped, tt.n)
			}
		})
	}
}

func TestSelectTopicsAppliesBothFilters(t *testing.T) {
	tds := kadm.TopicDetails{
		"__consumer_offsets": {Topic: "__consumer_offsets", IsInternal: true},
		"prod.a":             {Topic: "prod.a"},
		"prod.b":             {Topic: "prod.b"},
		"prod.c":             {Topic: "prod.c"},
		"staging.a":          {Topic: "staging.a"},
		"staging.b":          {Topic: "staging.b"},
	}
	f, err := newFilter([]string{"/^prod\\./"}, nil)
	if err != nil {
		t.Fatalf("newFilter: %v", err)
	}

	// The regex removes the two staging topics and the internal rule a third.
	// Everything that survives is shipped: nothing truncates the remainder.
	selected := selectTopics(tds, f, false)
	if len(selected) != 3 {
		t.Fatalf("selected %d, want the three prod topics", len(selected))
	}
	for _, td := range selected {
		if td.Topic != "prod.a" && td.Topic != "prod.b" && td.Topic != "prod.c" {
			t.Errorf("selected %q, want only the prod topics", td.Topic)
		}
	}

	// The internal rule is the only filter that opts back IN.
	if selected := selectTopics(tds, nil, true); len(selected) != 6 {
		t.Fatalf("include internal: selected %d, want all 6", len(selected))
	}
	if selected := selectTopics(tds, nil, false); len(selected) != 5 {
		t.Fatalf("exclude internal: selected %d, want 5", len(selected))
	}
}

func TestSelectTopicsOrderIsStable(t *testing.T) {
	// Guards Go's map iteration order leaking into the output: an order that
	// changed per cycle would show the backend mass entity churn that looks like
	// a cluster event.
	tds := kadm.TopicDetails{}
	for _, name := range []string{"e", "b", "d", "a", "c", "f", "g", "h"} {
		tds[name] = kadm.TopicDetail{Topic: name}
	}

	var first []string
	for i := 0; i < 50; i++ {
		var got []string
		for _, td := range selectTopics(tds, nil, false) {
			got = append(got, td.Topic)
		}
		if first == nil {
			first = got
			continue
		}
		if len(got) != len(first) {
			t.Fatalf("selection length changed: %v vs %v", got, first)
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("selection %v differs from %v on iteration %d", got, first, i)
			}
		}
	}
	if len(first) != 8 || first[0] != "a" || first[7] != "h" {
		t.Errorf("selection = %v, want all eight in sorted order", first)
	}
}

func TestErrPriority(t *testing.T) {
	tests := []struct {
		name          string
		kind          string
		requestScoped bool
		want          int
	}{
		// Request scope wins over kind: dropping a whole-API failure drops the
		// reason a section is empty.
		{name: "request scoped other", kind: kindOther, requestScoped: true, want: prioRequest},
		{name: "request scoped auth", kind: kindAuthorization, requestScoped: true, want: prioRequest},
		{name: "entity auth", kind: kindAuthorization, want: prioAuth},
		{name: "entity unsupported", kind: kindUnsupported, want: prioUnsupported},
		{name: "entity coordinator", kind: kindCoordinator, want: prioCoordinator},
		{name: "entity transport", kind: kindTransport, want: prioTransport},
		{name: "entity other", kind: kindOther, want: prioEntity},
		{name: "unclassified", kind: "", want: prioEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := errPriority(metrics.CollectionError{Kind: tt.kind}, tt.requestScoped)
			if got != tt.want {
				t.Errorf("errPriority = %d, want %d", got, tt.want)
			}
		})
	}
	if !(prioRequest < prioAuth && prioAuth < prioUnsupported && prioUnsupported < prioCoordinator &&
		prioCoordinator < prioTransport && prioTransport < prioEntity) {
		t.Error("priorities must be strictly increasing; admission relies on the ordering")
	}
}

// floodedSections builds the five canonical sections with the given error
// injectors, returning them in wire order.
func floodedSections(inject map[string]func(*section)) []*section {
	names := []string{sectionCluster, sectionTopics, sectionTopicsEnd, sectionGroups, sectionOffsets}
	secs := make([]*section, 0, len(names))
	for _, name := range names {
		s := newSection(name)
		if fn := inject[name]; fn != nil {
			fn(s)
		}
		secs = append(secs, s)
	}
	return secs
}

func TestMergeErrorsKeepsCriticalUnderTinyBudget(t *testing.T) {
	secs := floodedSections(map[string]func(*section){
		sectionTopics: func(s *section) {
			for i := 0; i < 4000; i++ {
				s.recordPartition("Metadata", "orders", int32(i), kerr.NotLeaderForPartition)
			}
		},
		sectionGroups: func(s *section) {
			s.recordGroup("DescribeGroups", "payments", kerr.GroupAuthorizationFailed)
		},
	})

	out, dropped := mergeErrors(secs, 1)
	if len(out) != 1 {
		t.Fatalf("emitted %d errors, want 1", len(out))
	}
	if out[0].Kind != kindAuthorization {
		// The cap must never spend itself on the 4000th NOT_LEADER and evict the
		// one actionable error.
		t.Fatalf("survivor = %+v, want the authorization error", out[0])
	}
	if dropped != 1 {
		t.Errorf("dropped = %d, want 1 (the collapsed NOT_LEADER exemplar)", dropped)
	}
}

func TestMergeErrorsOutputOrderIsCanonical(t *testing.T) {
	secs := floodedSections(map[string]func(*section){
		// A lower-priority error in an earlier section, a higher-priority one
		// later: admission picks the later entry first, emission must not.
		sectionCluster: func(s *section) {
			s.recordTopic("Metadata", "orders", kerr.NotLeaderForPartition)
		},
		sectionOffsets: func(s *section) {
			s.recordGroup("OffsetFetch", "payments", kerr.GroupAuthorizationFailed)
		},
	})

	out, dropped := mergeErrors(secs, 2)
	if len(out) != 2 || dropped != 0 {
		t.Fatalf("emitted %d, dropped %d, want 2 and 0", len(out), dropped)
	}
	if out[0].Section != sectionCluster || out[1].Section != sectionOffsets {
		t.Errorf("order = %q, %q; want section order regardless of admission order", out[0].Section, out[1].Section)
	}
}

func TestMergeErrorsUnlimitedEmitsEverything(t *testing.T) {
	secs := floodedSections(map[string]func(*section){
		sectionTopics: func(s *section) {
			s.recordPartition("Metadata", "orders", 0, kerr.NotLeaderForPartition)
			s.recordPartition("ListStartOffsets", "orders", 1, kerr.UnknownTopicOrPartition)
		},
		sectionGroups: func(s *section) {
			s.recordGroup("DescribeGroups", "payments", kerr.NotCoordinator)
		},
	})

	out, dropped := mergeErrors(secs, 0)
	if len(out) != 3 || dropped != 0 {
		t.Fatalf("emitted %d, dropped %d, want 3 and 0", len(out), dropped)
	}
	for _, s := range secs {
		if s.dropped.ErrorsDropped != 0 {
			t.Errorf("section %q reported drops under an unlimited cap", s.name)
		}
	}
}

func TestMergeErrorsWritesBackPerSectionDropped(t *testing.T) {
	secs := floodedSections(map[string]func(*section){
		sectionTopics: func(s *section) {
			// Three distinct keys, all entity priority.
			s.recordPartition("Metadata", "orders", 0, kerr.NotLeaderForPartition)
			s.recordPartition("ListStartOffsets", "orders", 1, kerr.UnknownTopicOrPartition)
			s.recordTopic("ListEndOffsets", "orders", errTopicNotListed)
		},
		sectionGroups: func(s *section) {
			s.recordGroup("DescribeGroups", "payments", kerr.NotCoordinator)
		},
	})
	topics, groups := secs[1], secs[3]

	out, dropped := mergeErrors(secs, 2)
	if len(out) != 2 || dropped != 2 {
		t.Fatalf("emitted %d, dropped %d, want 2 and 2", len(out), dropped)
	}
	// The coordinator error outranks all three entity errors.
	if groups.dropped.ErrorsDropped != 0 || groups.globalDropped != 0 {
		t.Errorf("groups dropped %d, want 0", groups.dropped.ErrorsDropped)
	}
	if topics.dropped.ErrorsDropped != 2 || topics.globalDropped != 2 {
		t.Errorf("topics dropped %d/%d, want 2/2", topics.dropped.ErrorsDropped, topics.globalDropped)
	}
	if !topics.truncated {
		t.Error("a section whose errors were dropped must report itself truncated")
	}

	// finish must run after mergeErrors: error_count is the post-cap emitted
	// count, not what the section accumulated.
	topics.stop()
	if got := topics.finish(); got.ErrorCount != 1 || got.ErrorsDropped != 2 || !got.Truncated {
		t.Errorf("finish() = %+v, want error_count 1, errors_dropped 2, truncated", got)
	}
}

func TestMergeErrorsToleratesNilSections(t *testing.T) {
	// A phase that never ran leaves a nil *section; the merge must not panic.
	secs := []*section{nil, newSection(sectionTopics), nil}
	secs[1].recordTopic("Metadata", "orders", kerr.NotLeaderForPartition)
	out, dropped := mergeErrors(secs, 0)
	if len(out) != 1 || dropped != 0 {
		t.Fatalf("emitted %d, dropped %d, want 1 and 0", len(out), dropped)
	}
}
