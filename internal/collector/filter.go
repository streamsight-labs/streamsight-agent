package collector

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/streamsight-labs/streamsight-agent/internal/metrics"
)

// filter is an include+exclude pair of compiled selection lists. A nil *filter,
// and an empty include list, both allow everything.
type filter struct {
	include []*regexp.Regexp
	exclude []*regexp.Regexp
}

func newFilter(include, exclude []string) (*filter, error) {
	f := new(filter)
	var err error
	if f.include, err = compilePatterns(include); err != nil {
		return nil, fmt.Errorf("include: %w", err)
	}
	if f.exclude, err = compilePatterns(exclude); err != nil {
		return nil, fmt.Errorf("exclude: %w", err)
	}
	return f, nil
}

// compilePatterns compiles a selection list, one entry at a time. A nil result
// for an empty list is what makes "unset means everything" fall out of allow's
// loop rather than needing a case of its own.
func compilePatterns(list []string) ([]*regexp.Regexp, error) {
	if len(list) == 0 {
		return nil, nil
	}
	out := make([]*regexp.Regexp, 0, len(list))
	for _, entry := range list {
		re, err := compilePattern(entry)
		if err != nil {
			return nil, fmt.Errorf("entry %q: %w", entry, err)
		}
		out = append(out, re)
	}
	return out, nil
}

// compilePattern turns one list entry into a matcher, borrowing kminion's
// convention: an entry is a literal unless it is wrapped in slashes, in which
// case the text between them is a regular expression.
//
// The literal form is escaped and anchored, so `orders.events` matches the
// topic of exactly that name and nothing else. That escaping is the point of
// the whole convention rather than a detail of it: Kafka names are full of
// dots, `.` is a regex wildcard, and a bare-regex config quietly turns an
// intended exclusion of `orders.events` into one that also drops
// `ordersXevents`. An operator naming a topic they can see in the broker gets
// that topic; nothing they type can accidentally be a metacharacter.
//
// The slash-wrapped form is compiled verbatim and therefore UNANCHORED, exactly
// as written -- `/orders/` matches `prod.orders.v2`. Anchoring a regex is what
// `^` and `$` are for, and someone reaching for the slashes has already said
// they are writing a regex.
//
// An empty body (`//`) is refused. It compiles to a pattern matching
// everything, which is what an empty list already says with no punctuation at
// all, so as a written entry it is far likelier to be a typo or a truncated
// value than an intent. `/.*/` says it deliberately.
func compilePattern(entry string) (*regexp.Regexp, error) {
	if body, ok := regexBody(entry); ok {
		if body == "" {
			return nil, errors.New("empty regex between slashes; write /.*/ to match everything")
		}
		re, err := regexp.Compile(body)
		if err != nil {
			return nil, fmt.Errorf("invalid regex: %w", err)
		}
		return re, nil
	}
	return regexp.Compile("^" + regexp.QuoteMeta(entry) + "$")
}

// regexBody reports whether entry uses the slash-wrapped regex form, returning
// the text between the slashes.
//
// The length check is what keeps a lone `/` a literal: it is a prefix and a
// suffix of itself, and a topic may legitimately be named `/`.
//
// config.validatePattern is the paired definition -- it applies this same rule
// at startup so a malformed entry fails the process rather than the first
// collection cycle. The two must agree on what counts as the regex form.
func regexBody(entry string) (string, bool) {
	if len(entry) >= 2 && strings.HasPrefix(entry, "/") && strings.HasSuffix(entry, "/") {
		return entry[1 : len(entry)-1], true
	}
	return "", false
}

// allow reports whether name survives the pair. An empty include list matches
// everything; a non-empty one is a union, so any single entry admits a name.
// Exclude always wins, so a name matching both is dropped -- the same
// precedence kminion gives its ignore lists, and the only one that lets a broad
// include be narrowed by a specific exclusion.
func (f *filter) allow(name string) bool {
	if f == nil {
		return true
	}
	if len(f.include) > 0 && !matchAny(f.include, name) {
		return false
	}
	return !matchAny(f.exclude, name)
}

func matchAny(res []*regexp.Regexp, name string) bool {
	for _, re := range res {
		if re.MatchString(name) {
			return true
		}
	}
	return false
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
//
// The lists travel exactly as configured, slashes and all, rather than as the
// patterns they compile to. What an operator wrote is what a backend can show
// them and what they can diff against their own config; `^orders\.events$` is
// neither.
func (o Options) selection() *metrics.Selection {
	sel := metrics.Selection{
		TopicInclude:          o.TopicInclude,
		TopicExclude:          o.TopicExclude,
		GroupInclude:          o.GroupInclude,
		GroupExclude:          o.GroupExclude,
		GroupStates:           o.GroupStates,
		IncludeInternalTopics: o.IncludeInternalTopics,
	}
	if len(sel.TopicInclude) == 0 && len(sel.TopicExclude) == 0 &&
		len(sel.GroupInclude) == 0 && len(sel.GroupExclude) == 0 &&
		len(sel.GroupStates) == 0 && !sel.IncludeInternalTopics {
		return nil
	}
	return &sel
}
