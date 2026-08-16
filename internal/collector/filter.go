package collector

import (
	"fmt"
	"regexp"
	"strings"
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
