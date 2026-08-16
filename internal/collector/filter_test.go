package collector

import "testing"

func TestFilterAllow(t *testing.T) {
	tests := []struct {
		name    string
		include string
		exclude string
		input   string
		want    bool
	}{
		{name: "no rules allows everything", input: "orders", want: true},
		{name: "include match", include: "^prod\\.", input: "prod.orders", want: true},
		{name: "include miss", include: "^prod\\.", input: "dev.orders", want: false},
		{name: "exclude match", exclude: "-dlq$", input: "orders-dlq", want: false},
		{name: "exclude miss", exclude: "-dlq$", input: "orders", want: true},
		{name: "exclude beats include", include: "^prod\\.", exclude: "-dlq$", input: "prod.orders-dlq", want: false},
		{name: "include and exclude both pass", include: "^prod\\.", exclude: "-dlq$", input: "prod.orders", want: true},
		{name: "unanchored include is a substring match", include: "orders", input: "prod.orders.v2", want: true},
		{name: "internal topic is not special to the regex", include: "^__", input: "__consumer_offsets", want: true},
		{name: "user topic with underscores is not dropped", exclude: "^__consumer_offsets$", input: "__events", want: true},
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

func TestNewFilterRejectsBadRegex(t *testing.T) {
	if _, err := newFilter("(unclosed", ""); err == nil {
		t.Error("want error for malformed include regex")
	}
	if _, err := newFilter("", "(unclosed"); err == nil {
		t.Error("want error for malformed exclude regex")
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
