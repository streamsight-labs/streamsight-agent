package collector

import "testing"

func TestNewCompilesFilters(t *testing.T) {
	c, err := New(nil, Options{
		TopicIncludeRegex: "^prod\\.",
		TopicExcludeRegex: "-dlq$",
		GroupExcludeRegex: "^test-",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !c.topics.allow("prod.orders") {
		t.Error("prod.orders should be allowed")
	}
	if c.topics.allow("prod.orders-dlq") {
		t.Error("prod.orders-dlq should be excluded")
	}
	if c.groups.allow("test-consumer") {
		t.Error("test-consumer should be excluded")
	}
	if !c.groups.allow("payments") {
		t.Error("payments should be allowed")
	}
	if c.log == nil {
		t.Error("logger must default to slog.Default()")
	}
}

func TestNewRejectsBadFilters(t *testing.T) {
	if _, err := New(nil, Options{TopicIncludeRegex: "(unclosed"}); err == nil {
		t.Error("want error for malformed topic include regex")
	}
	if _, err := New(nil, Options{GroupExcludeRegex: "(unclosed"}); err == nil {
		t.Error("want error for malformed group exclude regex")
	}
}

func TestZeroOptionsAllowEverything(t *testing.T) {
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, name := range []string{"orders", "__consumer_offsets", ""} {
		if !c.topics.allow(name) {
			t.Errorf("topic %q should be allowed by default", name)
		}
		if !c.groups.allow(name) {
			t.Errorf("group %q should be allowed by default", name)
		}
	}
	if c.opts.IncludeInternalTopics {
		t.Error("internal topics must be excluded by default")
	}
}
