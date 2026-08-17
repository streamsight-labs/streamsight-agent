package mockingest

import (
	"encoding"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"time"
)

// FieldPaths renders t as a sorted list of every JSON path it can produce, one
// line per field:
//
//	topics[].partitions[].start_offset *int64 nullable
//
// It is the only check in this package that actually freezes the wire contract.
// The strict decode in check.go (DisallowUnknownFields) is inert against this
// repo's agent, because mock and agent are compiled from the same tree and a
// newly added field is accepted by both sides at once. A committed golden of
// this output is what fails when someone adds, renames or retypes a field in
// metrics.Batch — so TestSchemaV1Frozen is not redundant with the strict decode,
// it is the part with teeth.
func FieldPaths(t reflect.Type) []string {
	var out []string
	// The stack breaks self-referential types, which metrics.Batch has none of
	// today but must never hang CI if one arrives.
	walkStruct("", t, &out, map[reflect.Type]bool{})
	sort.Strings(out)
	return out
}

func walkStruct(prefix string, t reflect.Type, out *[]string, stack map[reflect.Type]bool) {
	if t.Kind() != reflect.Struct || stack[t] {
		return
	}
	stack[t] = true
	defer delete(stack, t)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" && !f.Anonymous {
			continue // unexported: encoding/json never emits it
		}

		name, omitempty, skip := jsonName(f)
		if skip {
			continue
		}

		// An embedded struct with no json tag is inlined by encoding/json, so
		// its fields belong to the parent path.
		if name == "" && f.Anonymous {
			inner, _ := unwrap(f.Type)
			walkStruct(prefix, inner, out, stack)
			continue
		}

		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		*out = append(*out, line(path, f.Type, omitempty))

		inner, suffix := unwrap(f.Type)
		if !isLeaf(inner) {
			walkStruct(path+suffix, inner, out, stack)
		}
	}
}

// unwrap strips pointers, slices and maps down to the underlying value type,
// accumulating the path suffix that reaches it: "[]" for a slice element, "{}"
// for a map value.
func unwrap(t reflect.Type) (reflect.Type, string) {
	var suffix string
	for {
		if isLeaf(t) {
			return t, suffix
		}
		switch t.Kind() {
		case reflect.Pointer:
			t = t.Elem()
		case reflect.Slice, reflect.Array:
			suffix += "[]"
			t = t.Elem()
		case reflect.Map:
			suffix += "{}"
			t = t.Elem()
		default:
			return t, suffix
		}
	}
}

func line(path string, t reflect.Type, omitempty bool) string {
	var b strings.Builder
	b.WriteString(path)
	b.WriteByte(' ')
	b.WriteString(t.String())
	// Nullability is semantic, not stylistic: a *int64 becoming an int64
	// silently turns "not known this cycle" into 0.
	if t.Kind() == reflect.Pointer {
		b.WriteString(" nullable")
	}
	if omitempty {
		b.WriteString(" omitempty")
	}
	return b.String()
}

func jsonName(f reflect.StructField) (name string, omitempty, skip bool) {
	tag, ok := f.Tag.Lookup("json")
	if !ok {
		if f.Anonymous {
			return "", false, false
		}
		return f.Name, false, false
	}
	parts := strings.Split(tag, ",")
	if parts[0] == "-" && len(parts) == 1 {
		return "", false, true
	}
	name = parts[0]
	for _, o := range parts[1:] {
		if o == "omitempty" {
			omitempty = true
		}
	}
	if name == "" && !f.Anonymous {
		name = f.Name
	}
	return name, omitempty, false
}

var (
	timeType          = reflect.TypeOf(time.Time{})
	jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

// isLeaf reports whether t marshals as a single JSON scalar and must not be
// descended into. time.Time is a struct with unexported fields that encodes as
// an RFC3339 string; walking it would put runtime internals in the golden.
func isLeaf(t reflect.Type) bool {
	if t == timeType {
		return true
	}
	if t.Implements(jsonMarshalerType) || t.Implements(textMarshalerType) {
		return true
	}
	if reflect.PointerTo(t).Implements(jsonMarshalerType) || reflect.PointerTo(t).Implements(textMarshalerType) {
		return true
	}
	return t.Kind() != reflect.Struct && t.Kind() != reflect.Pointer &&
		t.Kind() != reflect.Slice && t.Kind() != reflect.Array && t.Kind() != reflect.Map
}
