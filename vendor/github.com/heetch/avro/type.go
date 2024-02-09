package avro

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/actgardner/gogen-avro/v10/schema"

	"github.com/heetch/avro/internal/typeinfo"
)

// Type represents an Avro schema type.
type Type struct {
	avroType schema.AvroType
	schema   string
	// We might not usually need the canonical string, so we
	// calculate it lazily and store it in canonical[opts].
	canonical     [RetainAll + 1]string
	canonicalOnce [RetainAll + 1]sync.Once
}

// ParseType parses an Avro schema in the format defined by the Avro
// specification at https://avro.apache.org/docs/current/spec.html.
func ParseType(s string) (*Type, error) {
	avroType, err := typeinfo.ParseSchema(s, nil)
	if err != nil {
		return nil, err
	}
	return &Type{
		schema:   s,
		avroType: avroType,
	}, nil
}

func (t *Type) String() string {
	return t.schema
}

// CanonicalOpts holds a bitmask of options for CanonicalString.
type CanonicalOpts int

const (
	// LeaveDefaults specifies that default values should be retained in
	// the canonicalized schema string.
	RetainDefaults CanonicalOpts = 1 << iota
	RetainLogicalTypes
	RetainAll CanonicalOpts = RetainDefaults | RetainLogicalTypes
)

// CanonicalString returns the canonical string representation of the type,
// as documented here: https://avro.apache.org/docs/1.9.1/spec.html#Transforming+into+Parsing+Canonical+Form
//
// BUG: Unicode characters \u2028 and \u2029 in strings inside the schema are always escaped, contrary to the
// specification above.
func (t *Type) CanonicalString(opts CanonicalOpts) string {
	opts &= RetainAll
	t.canonicalOnce[opts].Do(func() {
		c := &canonicalizer{
			defined: make(map[schema.QualifiedName]bool),
			opts:    opts,
		}
		v := c.canonicalValue(t.avroType)

		// Use a Encoder rather than MarshalJSON directly so that
		// we can disable escaping of HTML metacharacters.
		var buf strings.Builder
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(v); err != nil {
			panic(err)
		}
		t.canonical[opts] = strings.TrimSuffix(buf.String(), "\n")
	})
	return t.canonical[opts]
}

type canonicalizer struct {
	defined map[schema.QualifiedName]bool
	opts    CanonicalOpts
}

// canonicalFields holds all the fields that can be produced as part
// of a canonical schema representation in the order defined
// by the spec.
type canonicalFields struct {
	Name string      `json:"name,omitempty"`
	Type interface{} `json:"type,omitempty"`
	// Note: this is a pointer so that omitempty
	// doesn't omit it when the slice is empty but non-nil.
	Fields  *[]canonicalFields `json:"fields,omitempty"`
	Symbols []string           `json:"symbols,omitempty"`
	Items   interface{}        `json:"items,omitempty"`
	Size    int                `json:"size,omitempty"`
	Values  interface{}        `json:"values,omitempty"`
	// The default field isn't mentioned in the specification, but is
	// important to store in the registry, so we allow it to be
	// kept with the LeaveDefaults option to CanonicalString.
	// TODO the Avro spec doesn't define canonicalization for
	// numeric values, which could be an issue.
	Default interface{} `json:"default,omitempty"`
	// Logical types aren't mentioned in the specification either,
	// but they're important to maintain so that we can potentially
	// guard against data corruption due to changed logical types.
	LogicalType string `json:"logicalType,omitempty"`
	Precision   int    `json:"precision,omitempty"`
	Scale       int    `json:"scale,omitempty"`
}

func (c *canonicalizer) canonicalValue(at schema.AvroType) interface{} {
	s := c.canonicalValue1(at)
	ltype := logicalType(at)
	if (c.opts&RetainLogicalTypes) == 0 || ltype == "" {
		return s
	}
	var r canonicalFields
	switch s := s.(type) {
	case string:
		r = canonicalFields{
			Type: s,
		}
	case canonicalFields:
		r = s
	default:
		panic("unexpected type returned from canonicalValue1")
	}
	r.LogicalType = ltype
	if ltype == "decimal" {
		scale, _ := at.Attribute("scale").(float64)
		r.Scale = int(scale)
		precision, _ := at.Attribute("precision").(float64)
		r.Precision = int(precision)
	}
	return r
}

func (c *canonicalizer) canonicalValue1(at schema.AvroType) interface{} {
	switch at := at.(type) {
	case *schema.ArrayField:
		return canonicalFields{
			Type:  "array",
			Items: c.canonicalValue(at.ItemType()),
		}
	case *schema.BoolField:
		return "boolean"
	case *schema.BytesField:
		return "bytes"
	case *schema.DoubleField:
		return "double"
	case *schema.FloatField:
		return "float"
	case *schema.IntField:
		return "int"
	case *schema.LongField:
		return "long"
	case *schema.NullField:
		return "null"
	case *schema.StringField:
		return "string"
	case *schema.MapField:
		return canonicalFields{
			Type:   "map",
			Values: c.canonicalValue(at.ItemType()),
		}
	case *schema.UnionField:
		cf := make([]interface{}, len(at.ItemTypes()))
		for i, t := range at.ItemTypes() {
			cf[i] = c.canonicalValue(t)
		}
		return cf
	case *schema.Reference:
		if c.defined[at.TypeName] {
			return at.TypeName.String()
		}
		c.defined[at.TypeName] = true
		switch def := at.Def.(type) {
		case *schema.EnumDefinition:
			// TODO enum default
			return canonicalFields{
				Name:    def.AvroName().String(),
				Type:    "enum",
				Symbols: def.Symbols(),
			}
		case *schema.FixedDefinition:
			return canonicalFields{
				Name: def.AvroName().String(),
				Type: "fixed",
				Size: def.SizeBytes(),
			}
		case *schema.RecordDefinition:
			fields := def.Fields()
			cfields := make([]canonicalFields, len(fields))
			cf := canonicalFields{
				Name:   def.AvroName().String(),
				Type:   "record",
				Fields: &cfields,
			}
			for i, f := range fields {
				// It's possible that the order field should be stored in the
				// registry, but it doesn't seem necessary for now, so we
				// omit it.
				cfields[i] = canonicalFields{
					Name: f.Name(),
					Type: c.canonicalValue(f.Type()),
				}
				if f.HasDefault() && (c.opts&RetainDefaults) != 0 {
					v := f.Default()
					if v == nil {
						// The default is nil but we don't want it to be omitted
						// from the resulting JSON, so use a concrete nil type instead.
						v = (*struct{})(nil)
					}
					cfields[i].Default = v
				}
			}
			return cf
		default:
			panic(fmt.Errorf("unknown definition type %T", def))
		}
	default:
		panic(fmt.Errorf("unknown Avro type %T", at))
	}
}

// Name returns the fully qualified Avro Name for the type,
// or the empty string if it's not a definition.
func (t *Type) Name() string {
	ref, ok := t.avroType.(*schema.Reference)
	if !ok {
		return ""
	}
	return ref.TypeName.String()
}
