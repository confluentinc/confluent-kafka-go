package avro

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/schema"
)

// Names represents a namespace that can rename schema names.
// The zero value of a Names is the empty namespace.
type Names struct {
	// renames maps from an original Avro schema fully qualified
	// name to the new name and aliases for that name.
	renames map[string][]string

	// avroTypes is effectively a map[reflect.Type]*Type
	// that holds Avro types for Go types that specify the schema
	// entirely. Go types that don't fully specify a schema must be resolved
	// with respect to a given writer schema and so cannot live in
	// here.
	//
	// If there's an error translating a type, it's stored here as
	// an errorSchema.
	goTypeToAvroType sync.Map
	goTypeToEncoder  sync.Map
}

var builtinTypes = map[string]bool{
	"int":     true,
	"long":    true,
	"string":  true,
	"bytes":   true,
	"float":   true,
	"double":  true,
	"boolean": true,
	"null":    true,
}

// Marshal is like the Marshal function except that names
// in the schema for x are renamed according to names.
func (names *Names) Marshal(x interface{}) ([]byte, *Type, error) {
	return marshalAppend(names, nil, reflect.ValueOf(x))
}

// Rename returns a copy of n that renames oldName to newName
// with the given aliases when a schema is used.
//
// If aliases aren't full names, their namespace will be taken from
// the namespace of newName.
//
// If n already includes a rename for oldName, the old association
// will be overwritten.
//
// The rename only applies to schemas directly - it does not rename
// names already passed to Rename as newName or aliases.
//
// So for example:
//
//	n.Rename("foo", "bar").
//		Rename("bar", "baz").
//		TypeOf(`{"type":"record", "name": "foo", "fields": ...}`)
//
// will return a type with a schema named "bar", not "baz".
//
// Rename panics if oldName is any of the built-in Avro types.
func (n *Names) Rename(oldName string, newName string, newAliases ...string) *Names {
	if builtinTypes[oldName] {
		panic(fmt.Errorf("rename of built-in type %q to %q", oldName, newName))
	}
	n1 := &Names{
		renames: make(map[string][]string),
	}
	for name, names := range n.renames {
		n1.renames[name] = names
	}
	newNames := make([]string, 1+len(newAliases))
	newNames[0] = newName
	copy(newNames[1:], newAliases)
	n1.renames[oldName] = newNames
	return n1
}

// RenameType returns a copy of n that uses the given name
// and aliases for the type of x.
//
// RenameType will panic if TypeOf(x) returns an error or the type doesn't
// represent an Avro named definition (a record, an enum or a fixed Avro type)
//
// If RenameType has already been called for the type of x, the old association will be
// overwritten.
func (n *Names) RenameType(x interface{}, newName string, newAliases ...string) *Names {
	// TODO on the errors below, we could just ignore the Add and return n unchanged.
	// The caller will get an error if they ever try to use the type
	// for encoding or decoding, so maybe that's OK.
	// See https://github.com/heetch/avro/issues/38
	t, err := TypeOf(x)
	if err != nil {
		panic(fmt.Errorf("cannot rename %T to %q: cannot get Avro type: %v", x, newName, err))
	}
	name := t.Name()
	if name == "" {
		panic(fmt.Errorf("cannot rename %T to %q: it does not represent an Avro definition", x, newName))
	}
	return n.Rename(name, newName, newAliases...)
}

// TypeOf is like the TypeOf function except that Avro names
// in x will be translate through the namespace n.
func (n *Names) TypeOf(x interface{}) (*Type, error) {
	return avroTypeOf(n, reflect.TypeOf(x))
}

func (names *Names) renameSchema(at schema.AvroType) interface{} {
	return names.renameSchema1(at, "", make(map[schema.QualifiedName]bool))
}

func (names *Names) renameSchema1(at schema.AvroType, enclosingNamespace string, defined map[schema.QualifiedName]bool) interface{} {
	switch at := at.(type) {
	case *schema.Reference:
		// https://avro.apache.org/docs/1.9.1/spec.html#Aliases
		// "A type alias may be specified either as fully
		// namespace-qualified, or relative to the namespace of
		// the name it is an alias for."
		var qname schema.QualifiedName
		var qaliases []schema.QualifiedName
		if newNames, ok := names.renames[at.TypeName.String()]; ok {
			qname = parser.ParseAvroName("", newNames[0])
			qaliases = make([]schema.QualifiedName, len(newNames)-1)
			for i, alias := range newNames[1:] {
				qaliases[i] = parser.ParseAvroName(qname.Namespace, alias)
			}
		} else {
			qname = at.TypeName
			qaliases = at.Def.Aliases()
		}
		if defined[qname] {
			// It's a reference to a type that's already been
			// defined, so just use the new name.
			return relativeName(enclosingNamespace, qname)
		}
		// References are always represented by a JSON object
		// (a map in Go).
		def := copyOfSchemaObj(at)
		defined[qname] = true
		switch adef := at.Def.(type) {
		case *schema.RecordDefinition:
			fieldDefs := make([]map[string]interface{}, len(adef.Fields()))
			for i, f := range adef.Fields() {
				fieldDef := copyOfSchemaObj(f)
				fieldDef["type"] = names.renameSchema1(f.Type(), qname.Namespace, defined)
				fieldDefs[i] = fieldDef
			}
			def["fields"] = fieldDefs
		case *schema.FixedDefinition:
		case *schema.EnumDefinition:
		default:
			panic(fmt.Errorf("unknown definition type %T", adef))
		}
		delete(def, "namespace")
		def["name"] = relativeName(enclosingNamespace, qname)
		delete(def, "aliases")
		if len(qaliases) > 0 {
			aliases := make([]string, len(qaliases))
			for i, name := range qaliases {
				aliases[i] = relativeName(qname.Namespace, name)
			}
			def["aliases"] = aliases
		}
		return def
	case *schema.UnionField:
		items := make([]interface{}, len(at.ItemTypes()))
		for i, item := range at.ItemTypes() {
			items[i] = names.renameSchema1(item, enclosingNamespace, defined)
		}
		return items
	case *schema.ArrayField:
		obj := copyOfSchemaObj(at)
		obj["items"] = names.renameSchema1(at.ItemType(), enclosingNamespace, defined)
		return obj
	case *schema.MapField:
		obj := copyOfSchemaObj(at)
		obj["values"] = names.renameSchema1(at.ItemType(), enclosingNamespace, defined)
		return obj
	default:
		obj, _ := at.Definition(emptyScope())
		return obj
	}
}

func relativeName(enclosingNamespace string, name schema.QualifiedName) string {
	if name.Namespace == enclosingNamespace {
		return name.Name
	}
	return name.String()
}

func copyOfSchemaObj(at interface{}) map[string]interface{} {
	var obj interface{}
	// Note: at the time of writing there's no way that Definition can
	// return an error.
	switch at := at.(type) {
	case *schema.Field:
		obj, _ = at.Definition(emptyScope())
	case schema.AvroType:
		obj, _ = at.Definition(emptyScope())
	}
	switch obj := obj.(type) {
	case map[string]interface{}:
		obj1 := make(map[string]interface{})
		for name, val := range obj {
			obj1[name] = val
		}
		return obj1
	default:
		panic(fmt.Errorf("unexpected type for Avro definition %T", obj))
	}
}

func emptyScope() map[schema.QualifiedName]interface{} {
	return make(map[schema.QualifiedName]interface{})
}
