package collector

import "testing"

func TestFilterAllow(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		exclude []string
		input   string
		want    bool
	}{
		{name: "no rules allows everything", input: "orders", want: true},
		{name: "empty include list allows everything", include: []string{}, input: "orders", want: true},

		// The literal form, and the bug it exists to fix.
		{name: "literal matches its own name", include: []string{"orders.events"}, input: "orders.events", want: true},
		{name: "literal does not match a longer name", include: []string{"orders"}, input: "prod.orders.v2", want: false},
		{name: "literal dot is not a wildcard", include: []string{"orders.events"}, input: "ordersXevents", want: false},
		{name: "literal exclude does not over-match", exclude: []string{"orders.events"}, input: "ordersXevents", want: true},
		{name: "literal metacharacters are escaped", include: []string{"a+b"}, input: "a+b", want: true},
		{name: "escaped metacharacter does not match its regex meaning", include: []string{"a+b"}, input: "aab", want: false},
		{name: "a lone slash is a literal name", include: []string{"/"}, input: "/", want: true},

		// The slash-wrapped form: verbatim, and therefore unanchored.
		{name: "regex include match", include: []string{"/^prod\\./"}, input: "prod.orders", want: true},
		{name: "regex include miss", include: []string{"/^prod\\./"}, input: "dev.orders", want: false},
		{name: "regex exclude match", exclude: []string{"/-dlq$/"}, input: "orders-dlq", want: false},
		{name: "regex exclude miss", exclude: []string{"/-dlq$/"}, input: "orders", want: true},
		{name: "regex is unanchored", include: []string{"/orders/"}, input: "prod.orders.v2", want: true},
		{name: "regex dot is still a wildcard", include: []string{"/^orders.events$/"}, input: "ordersXevents", want: true},

		// Lists are a union; exclude wins over include.
		{name: "any include entry admits", include: []string{"payments", "orders"}, input: "orders", want: true},
		{name: "no include entry rejects", include: []string{"payments", "orders"}, input: "billing", want: false},
		{name: "any exclude entry drops", exclude: []string{"payments", "orders"}, input: "orders", want: false},
		{name: "exclude beats include", include: []string{"/^prod\\./"}, exclude: []string{"/-dlq$/"}, input: "prod.orders-dlq", want: false},
		{name: "include and exclude both pass", include: []string{"/^prod\\./"}, exclude: []string{"/-dlq$/"}, input: "prod.orders", want: true},
		{name: "broad regex include narrowed by a literal exclude", include: []string{"/^prod\\./"}, exclude: []string{"prod.orders"}, input: "prod.orders", want: false},
		{name: "the literal exclude narrows only that one name", include: []string{"/^prod\\./"}, exclude: []string{"prod.orders"}, input: "prod.ordersX", want: true},
		{name: "mixed forms in one list", include: []string{"orders.events", "/^tmp-/"}, input: "tmp-scratch", want: true},

		// Internal names are not special to the filter; only the metadata flag
		// and INCLUDE_INTERNAL_TOPICS decide that.
		{name: "internal topic is not special to the filter", include: []string{"/^__/"}, input: "__consumer_offsets", want: true},
		{name: "user topic with underscores is not dropped", exclude: []string{"__consumer_offsets"}, input: "__events", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := newFilter(tt.include, tt.exclude)
			if err != nil {
				t.Fatalf("newFilter: %v", err)
			}
			if got := f.allow(tt.input); got != tt.want {
				t.Errorf("allow(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestFilterNilAllowsEverything(t *testing.T) {
	var f *filter
	if !f.allow("anything") {
		t.Error("nil filter must allow everything")
	}
}

// A malformed entry must fail construction rather than silently matching
// nothing: an agent that starts and exports an empty inventory is
// indistinguishable from a cluster that is empty.
func TestNewFilterRejectsBadEntries(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		exclude []string
	}{
		{name: "bad include regex", include: []string{"/(unclosed/"}},
		{name: "bad exclude regex", exclude: []string{"/(unclosed/"}},
		{name: "one bad entry fails the list", include: []string{"orders", "/(unclosed/"}},
		{name: "empty regex body in include", include: []string{"//"}},
		{name: "empty regex body in exclude", exclude: []string{"//"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newFilter(tt.include, tt.exclude); err == nil {
				t.Error("want error, got nil")
			}
		})
	}
}

// A literal is escaped before it is compiled, so nothing an operator can type
// as a topic or group name can fail to compile.
func TestNewFilterAcceptsAnyLiteral(t *testing.T) {
	for _, entry := range []string{"(unclosed", "a(", "*", "(?P<", "[", "\\", "/", "/one-sided"} {
		if _, err := newFilter([]string{entry}, nil); err != nil {
			t.Errorf("newFilter(%q): %v", entry, err)
		}
	}
}

func TestIsInternalGroup(t *testing.T) {
	tests := []struct {
		id   string
		want bool
	}{
		{"__internal", true},
		{"__consumer_offsets", true},
		{"_schemas", false},
		{"my-app", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isInternalGroup(tt.id); got != tt.want {
			t.Errorf("isInternalGroup(%q) = %v, want %v", tt.id, got, tt.want)
		}
	}
}
