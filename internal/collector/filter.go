package collector

import (
	"fmt"
	"regexp"
	"strings"

	"kafka-metrics-agent/internal/metrics"
)

// filter is an include+exclude regex pair. A nil *filter allows everything.
type filter struct {
	include *regexp.Regexp
	exclude *regexp.Regexp
}

func newFilter(include, exclude string) (*filter, error) {
	f := new(filter)
	var err error
	if include != "" {
		if f.include, err = regexp.Compile(include); err != nil {
			return nil, fmt.Errorf("compile include regex %q: %w", include, err)
		}
	}
	if exclude != "" {
		if f.exclude, err = regexp.Compile(exclude); err != nil {
			return nil, fmt.Errorf("compile exclude regex %q: %w", exclude, err)
		}
	}
	return f, nil
}

// allow reports whether name survives the pair. An unset include matches
// everything; exclude always wins, so a name matching both is dropped.
//
// The regexes are unanchored on purpose: `orders` matches `prod.orders`, and a
// caller wanting exact matches writes `^orders$`.
func (f *filter) allow(name string) bool {
	if f == nil {
		return true
	}
	if f.include != nil && !f.include.MatchString(name) {
		return false
	}
	if f.exclude != nil && f.exclude.MatchString(name) {
		return false
	}
	return true
}

// isInternalGroup keeps the "__" prefix rule for GROUPS only. Topics carry an
// explicit IsInternal flag in metadata; the group protocol carries nothing
// equivalent, so the prefix convention is all there is.
func isInternalGroup(id string) bool { return strings.HasPrefix(id, "__") }

// selection echoes the filters in force, for the batch to carry.
//
// Nil when nothing narrows the view, so presence alone answers the question a
// backend has to ask before aggregating: is this the whole cluster? A filtered
// entity leaves no trace in the payload -- it is simply absent, with no counter
// saying it ever existed -- so the configuration has to travel with the data or
// the omission is unknowable at the far end.
func (o Options) selection() *metrics.Selection {
	sel := metrics.Selection{
		TopicInclude:          o.TopicIncludeRegex,
		TopicExclude:          o.TopicExcludeRegex,
		GroupInclude:          o.GroupIncludeRegex,
		GroupExclude:          o.GroupExcludeRegex,
		GroupStates:           o.GroupStates,
		IncludeInternalTopics: o.IncludeInternalTopics,
	}
	if sel.TopicInclude == "" && sel.TopicExclude == "" &&
		sel.GroupInclude == "" && sel.GroupExclude == "" &&
		len(sel.GroupStates) == 0 && !sel.IncludeInternalTopics {
		return nil
	}
	return &sel
}
