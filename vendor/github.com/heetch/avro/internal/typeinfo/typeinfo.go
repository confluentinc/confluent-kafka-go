package typeinfo

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/heetch/avro/avrotypegen"
)

// Info is the representation of Go types used
// by the analyzer. It can represent a record, a field or a type
// inside one of those.
type Info struct {
	// Type holds the Go type used for this Avro type (or nil for null).
	Type reflect.Type

	// FieldName holds the Avro name of the field.
	FieldName string

	// FieldIndex holds the index of the field if this entry is about
	// a struct field.
	FieldIndex int

	// MakeDefault is a function that returns the default
	// value for a field, or nil if there is no default value.
	MakeDefault func() reflect.Value

	// IsUnion holds whether this info is about a union type
	// (if not, it's about a struct).
	IsUnion bool

	// Entries holds the possible types that can
	// be descended in from this type.
	// For structs (records) this holds an entry
	// for each field; for union types, this holds an
	// entry for each possible type in the union.
	Entries []Info
}

// ForType returns the Info for the given Go type.
func ForType(t reflect.Type) (Info, error) {
	if debugging {
		debugf("Info(%v)", t)
	}
	switch t.Kind() {
	case reflect.Struct:
		info := Info{
			Type:    t,
			Entries: make([]Info, 0, t.NumField()),
		}
		// Note that RecordInfo is defined in such a way that
		// the zero value gives useful defaults for a normal Go
		// value that doesn't return the any RecordInfo - all
		// fields will default to their zero value and the only
		// unions will be pointer types.
		// We don't need to diagnose all bad Go types here - they'll
		// be caught earlier - when we try to determine the Avro schema
		// from the Go type.
		var r avrotypegen.RecordInfo
		if v, ok := reflect.Zero(t).Interface().(avrotypegen.AvroRecord); ok {
			r = v.AvroRecord()
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Anonymous {
				// TODO consider struct embedding.
				// https://github.com/heetch/avro/issues/40
				return Info{}, fmt.Errorf("anonymous fields not supported")
			}
			if shouldOmitField(f) {
				continue
			}
			var required bool
			var makeDefault func() reflect.Value
			var unionInfo avrotypegen.UnionInfo
			if i < len(r.Required) {
				required = r.Required[i]
			}
			if i < len(r.Defaults) {
				if md := r.Defaults[i]; md != nil {
					makeDefault = func() reflect.Value {
						return reflect.ValueOf(md())
					}
				}
			}
			if i < len(r.Unions) {
				unionInfo = r.Unions[i]
			}
			entry := forField(f, required, makeDefault, unionInfo)
			info.Entries = append(info.Entries, entry)
		}
		if debugging {
			debugf("-> record, %d entries", len(info.Entries))
		}
		return info, nil
	default:
		// TODO check for top-level union types too.
		// See https://github.com/heetch/avro/issues/13
		if debugging {
			debugf("-> unknown")
		}
		return Info{
			Type: t,
		}, nil
	}
}

func forField(f reflect.StructField, required bool, makeDefault func() reflect.Value, unionInfo avrotypegen.UnionInfo) Info {
	t := f.Type
	if t.Kind() == reflect.Ptr && len(unionInfo.Union) == 0 {
		// It's a pointer but there's no explicit union entry, which means that
		// the union defaults to ["null", type]
		unionInfo.Union = []avrotypegen.UnionInfo{{
			Type: nil,
		}, {
			Type: reflect.New(t.Elem()).Interface(),
		}}
	}
	// Make an appropriate makeDefault function, even when one isn't explicitly specified.
	switch {
	case required:
		// Keep to the letter of the contract (makeDefault should always
		// be nil in this case anyway).
		makeDefault = nil
	case makeDefault == nil && len(unionInfo.Union) > 0:
		// It's a ["null", T] union - we can infer the default
		// value from the field type. The default value is the
		// zero value of the first member of the union.
		var v reflect.Value
		firstMemberType := unionInfo.Union[0].Type
		if firstMemberType != nil {
			v = reflect.Zero(reflect.TypeOf(firstMemberType).Elem())
		} else {
			v = reflect.Zero(t)
		}
		makeDefault = func() reflect.Value {
			return v
		}
	case makeDefault == nil:
		v := reflect.Zero(t)
		makeDefault = func() reflect.Value {
			return v
		}
	}
	name, _ := JSONFieldName(f)
	info := Info{
		Type:        t,
		FieldIndex:  f.Index[0],
		FieldName:   name,
		MakeDefault: makeDefault,
	}
	setUnionInfo(&info, unionInfo)
	return info
}

func setUnionInfo(info *Info, unionInfo avrotypegen.UnionInfo) {
	if len(unionInfo.Union) == 0 {
		return
	}
	info.IsUnion = true
	info.Entries = make([]Info, len(unionInfo.Union))
	for i, u := range unionInfo.Union {
		var ut reflect.Type
		if u.Type != nil {
			ut = reflect.TypeOf(u.Type).Elem()
		}
		info.Entries[i] = Info{
			Type: ut,
		}
		setUnionInfo(&info.Entries[i], u)
	}
}

func shouldOmitField(f reflect.StructField) bool {
	name, _ := JSONFieldName(f)
	return name == ""
}

// JSONFieldName returns the name that the field will be given
// when marshaled to JSON, or the empty string if
// the field is ignored.
// It also reports whether the field has been qualified with
// the "omitempty" qualifier.
func JSONFieldName(f reflect.StructField) (name string, omitEmpty bool) {
	if f.PkgPath != "" {
		// It's unexported.
		return "", false
	}
	tag := f.Tag.Get("json")
	parts := strings.Split(tag, ",")
	for _, part := range parts[1:] {
		if part == "omitempty" {
			omitEmpty = true
		}
	}
	switch {
	case parts[0] == "":
		return f.Name, omitEmpty
	case parts[0] == "-":
		return "", omitEmpty
	}
	return parts[0], omitEmpty
}

const debugging = false

func debugf(f string, a ...interface{}) {
	if debugging {
		log.Printf(f, a...)
	}
}
