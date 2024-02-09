package parser

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	avro "github.com/actgardner/gogen-avro/v10/schema"
)

// Namespace is a mapping of avro.QualifiedNames to their Definitions, used to resolve
// type lookups within a schema.
type Namespace struct {
	Definitions map[avro.QualifiedName]avro.Definition
	Roots       []avro.Definition
	ShortUnions bool
}

func NewNamespace(shortUnions bool) *Namespace {
	return &Namespace{
		Definitions: make(map[avro.QualifiedName]avro.Definition),
		Roots:       make([]avro.Definition, 0),
		ShortUnions: shortUnions,
	}
}

// RegisterDefinition adds a new type definition to the namespace. Returns an error if the type is already defined.
func (n *Namespace) RegisterDefinition(d avro.Definition) error {
	if curDef, ok := n.Definitions[d.AvroName()]; ok {
		if !reflect.DeepEqual(curDef, d) {
			return fmt.Errorf("Conflicting definitions for %v", d.AvroName())
		}
		return nil
	}
	n.Definitions[d.AvroName()] = d
	n.Roots = append(n.Roots, d)

	for _, alias := range d.Aliases() {
		if existing, ok := n.Definitions[alias]; ok {
			return fmt.Errorf("Alias for %q is %q, but %q is already aliased with that name", d.AvroName(), alias, existing.AvroName())
		}
		n.Definitions[alias] = d
	}
	return nil
}

// ParseAvroName parses a name according to the Avro spec:
//   - If the name contains a dot ('.'), the last part is the name and the rest is the namespace
//   - Otherwise, the enclosing namespace is used
func ParseAvroName(enclosing, name string) avro.QualifiedName {
	lastIndex := strings.LastIndex(name, ".")
	if lastIndex != -1 {
		enclosing = name[:lastIndex]
	}
	return avro.QualifiedName{enclosing, name[lastIndex+1:]}
}

// TypeForSchema accepts an Avro schema as a JSON string, decode it and return the AvroType defined at the top level:
//    - a single record definition (JSON map)
//    - a union of multiple types (JSON array)
//    - an already-defined type (JSON string)
// The Avro type defined at the top level and all the type definitions beneath it will also be added to this Namespace.
func (n *Namespace) TypeForSchema(schemaJson []byte) (avro.AvroType, error) {
	var schema interface{}
	if err := json.Unmarshal(schemaJson, &schema); err != nil {
		return nil, err
	}

	field, err := n.decodeTypeDefinition("topLevel", "", schema)
	if err != nil {
		return nil, err
	}

	n.Roots = append(n.Roots, &avro.FileRoot{field})

	return field, nil
}

func (n *Namespace) decodeTypeDefinition(name, namespace string, schema interface{}) (avro.AvroType, error) {
	switch schema.(type) {
	case string:
		typeStr := schema.(string)
		return n.getTypeByName(namespace, typeStr, schema), nil

	case []interface{}:
		return n.decodeUnionDefinition(name, namespace, schema.([]interface{}))

	case map[string]interface{}:
		return n.decodeComplexDefinition(name, namespace, schema.(map[string]interface{}))

	}

	return nil, NewWrongMapValueTypeError("type", "array, string, map", schema)
}

// Given a map representing a record definition, validate the definition and build the RecordDefinition struct.
func (n *Namespace) decodeRecordDefinition(namespace string, schemaMap map[string]interface{}) (avro.Definition, error) {
	typeStr, err := getMapString(schemaMap, "type")
	if err != nil {
		return nil, err
	}

	if typeStr != "record" {
		return nil, fmt.Errorf("Type of record must be 'record'")
	}

	name, err := avroNameForDefinition(schemaMap, namespace)
	if err != nil {
		return nil, err
	}

	var rDocString string
	if rDoc, ok := schemaMap["doc"]; ok {
		rDocString, ok = rDoc.(string)
		if !ok {
			return nil, NewWrongMapValueTypeError("doc", "string", rDoc)
		}
	}

	fieldList, err := getMapArray(schemaMap, "fields")
	if err != nil {
		return nil, err
	}

	decodedFields := make([]*avro.Field, 0)
	for i, f := range fieldList {
		field, ok := f.(map[string]interface{})
		if !ok {
			return nil, NewWrongMapValueTypeError("fields", "map[]", field)
		}

		fieldName, err := getMapString(field, "name")
		if err != nil {
			return nil, err
		}

		t, ok := field["type"]
		if !ok {
			return nil, NewRequiredMapKeyError("type")
		}

		fieldType, err := n.decodeTypeDefinition(fieldName, name.Namespace, t)
		if err != nil {
			return nil, err
		}

		var docString string
		if doc, ok := field["doc"]; ok {
			docString, ok = doc.(string)
			if !ok {
				return nil, NewWrongMapValueTypeError("doc", "string", doc)
			}
		}

		var fieldTags string
		if tags, ok := field["golang.tags"]; ok {
			fieldTags, ok = tags.(string)
			if !ok {
				return nil, NewWrongMapValueTypeError("golang.tags", "string", tags)
			}
		}

		var fieldAliases []string
		if aliases, ok := field["aliases"]; ok {
			aliasList, ok := aliases.([]interface{})
			if !ok {
				return nil, NewWrongMapValueTypeError("aliases", "[]string", aliases)
			}

			for _, aliasVal := range aliasList {
				aliasStr, ok := aliasVal.(string)
				if !ok {
					return nil, NewWrongMapValueTypeError("aliases", "[]string", aliases)
				}
				fieldAliases = append(fieldAliases, aliasStr)
			}
		}

		def, hasDef := field["default"]
		fieldStruct := avro.NewField(fieldName, fieldType, def, hasDef, fieldAliases, docString, field, i, fieldTags)

		decodedFields = append(decodedFields, fieldStruct)
	}

	aliases, err := parseAliases(schemaMap, name.Namespace)
	if err != nil {
		return nil, err
	}

	return avro.NewRecordDefinition(name, aliases, decodedFields, rDocString, schemaMap), nil
}

// decodeEnumDefinition accepts a namespace and a map representing an enum definition,
// it validates the definition and build the EnumDefinition struct.
func (n *Namespace) decodeEnumDefinition(namespace string, schemaMap map[string]interface{}) (avro.Definition, error) {
	typeStr, err := getMapString(schemaMap, "type")
	if err != nil {
		return nil, err
	}

	if typeStr != "enum" {
		return nil, fmt.Errorf("Type of enum must be 'enum'")
	}

	name, err := avroNameForDefinition(schemaMap, namespace)
	if err != nil {
		return nil, err
	}

	symbolSlice, err := getMapArray(schemaMap, "symbols")
	if err != nil {
		return nil, err
	}

	var defaultStr string
	if dv, ok := schemaMap["default"]; ok {
		defaultStr, ok = dv.(string)
		if !ok {
			return nil, NewWrongMapValueTypeError("default", "string", dv)
		}
	}

	symbolStr, ok := interfaceSliceToStringSlice(symbolSlice)
	if !ok {
		return nil, fmt.Errorf("'symbols' must be an array of strings")
	}

	aliases, err := parseAliases(schemaMap, namespace)
	if err != nil {
		return nil, err
	}

	var docString string
	if doc, ok := schemaMap["doc"]; ok {
		if docString, ok = doc.(string); !ok {
			return nil, fmt.Errorf("'doc' must be a string")
		}
	}

	return avro.NewEnumDefinition(name, aliases, symbolStr, docString, defaultStr, schemaMap), nil
}

// decodeFixedDefinition accepts a namespace and a map representing a fixed definition,
// it validates the definition and build the FixedDefinition struct.
func (n *Namespace) decodeFixedDefinition(namespace string, schemaMap map[string]interface{}) (avro.Definition, error) {
	typeStr, err := getMapString(schemaMap, "type")
	if err != nil {
		return nil, err
	}

	if typeStr != "fixed" {
		return nil, fmt.Errorf("Type of fixed must be 'fixed'")
	}

	name, err := avroNameForDefinition(schemaMap, namespace)
	if err != nil {
		return nil, err
	}

	sizeBytes, err := getMapFloat(schemaMap, "size")
	if err != nil {
		return nil, err
	}

	aliases, err := parseAliases(schemaMap, name.Namespace)
	if err != nil {
		return nil, err
	}

	return avro.NewFixedDefinition(name, aliases, int(sizeBytes), schemaMap), nil
}

func (n *Namespace) decodeUnionDefinition(name, namespace string, fieldList []interface{}) (avro.AvroType, error) {
	unionFields := make([]avro.AvroType, 0)
	for _, f := range fieldList {
		fieldDef, err := n.decodeTypeDefinition(name, namespace, f)
		if err != nil {
			return nil, err
		}

		unionFields = append(unionFields, fieldDef)
	}

	if n.ShortUnions {
		name += "Union"
	} else {
		name = ""
	}
	return avro.NewUnionField(name, unionFields, fieldList), nil
}

func (n *Namespace) decodeComplexDefinition(name, namespace string, typeMap map[string]interface{}) (avro.AvroType, error) {
	typeStr, err := getMapString(typeMap, "type")
	if err != nil {
		return nil, err
	}
	switch typeStr {
	case "array":
		items, ok := typeMap["items"]
		if !ok {
			return nil, NewRequiredMapKeyError("items")
		}

		fieldType, err := n.decodeTypeDefinition(name, namespace, items)
		if err != nil {
			return nil, err
		}

		return avro.NewArrayField(fieldType, typeMap), nil

	case "map":
		values, ok := typeMap["values"]
		if !ok {
			return nil, NewRequiredMapKeyError("values")
		}

		fieldType, err := n.decodeTypeDefinition(name, namespace, values)
		if err != nil {
			return nil, err
		}

		return avro.NewMapField(fieldType, typeMap), nil

	case "enum":
		definition, err := n.decodeEnumDefinition(namespace, typeMap)
		if err != nil {
			return nil, err
		}

		err = n.RegisterDefinition(definition)
		if err != nil {
			return nil, err
		}
		return avro.NewReference(definition.AvroName()), nil

	case "fixed":
		definition, err := n.decodeFixedDefinition(namespace, typeMap)
		if err != nil {
			return nil, err
		}

		err = n.RegisterDefinition(definition)
		if err != nil {
			return nil, err
		}

		return avro.NewReference(definition.AvroName()), nil

	case "record":
		definition, err := n.decodeRecordDefinition(namespace, typeMap)
		if err != nil {
			return nil, err
		}

		err = n.RegisterDefinition(definition)
		if err != nil {
			return nil, err
		}

		return avro.NewReference(definition.AvroName()), nil

	default:
		// If the type isn't a special case, it's a primitive or a reference to an existing type
		return n.getTypeByName(namespace, typeStr, typeMap), nil
	}
}

func (n *Namespace) getTypeByName(namespace string, typeStr string, definition interface{}) avro.AvroType {
	switch typeStr {
	case "int":
		return avro.NewIntField(definition)

	case "long":
		return avro.NewLongField(definition)

	case "float":
		return avro.NewFloatField(definition)

	case "double":
		return avro.NewDoubleField(definition)

	case "boolean":
		return avro.NewBoolField(definition)

	case "bytes":
		return avro.NewBytesField(definition)

	case "string":
		return avro.NewStringField(definition)

	case "null":
		return avro.NewNullField(definition)
	}

	return avro.NewReference(ParseAvroName(namespace, typeStr))
}

// parseAliases parses out all the aliases from a definition map - returns an empty slice if no aliases exist.
// Returns an error if the aliases key exists but the value isn't a list of strings.
func parseAliases(objectMap map[string]interface{}, namespace string) ([]avro.QualifiedName, error) {
	aliases, ok := objectMap["aliases"]
	if !ok {
		return make([]avro.QualifiedName, 0), nil
	}

	aliasList, ok := aliases.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Field aliases expected to be array, got %v", aliases)
	}

	qualifiedAliases := make([]avro.QualifiedName, 0, len(aliasList))

	for _, alias := range aliasList {
		aliasString, ok := alias.(string)
		if !ok {
			return nil, fmt.Errorf("Field aliases expected to be array of strings, got %v", aliases)
		}
		qualifiedAliases = append(qualifiedAliases, ParseAvroName(namespace, aliasString))
	}
	return qualifiedAliases, nil
}

// avroNameForDefinition returns the fully qualified name from the
// schema definition represented by schemaMap. From the specification:
//
//	In record, enum and fixed definitions, the fullname is
//	determined in one of the following ways:
//
//	- A name and namespace are both specified. For example, one
//	might use "name": "X", "namespace": "org.foo" to indicate the
//	fullname org.foo.X.
//
//	- A fullname is specified. If the name specified contains a
//	dot, then it is assumed to be a fullname, and any namespace
//	also specified is ignored. For example, use "name":
//	"org.foo.X" to indicate the fullname org.foo.X.
//
//	- A name only is specified, i.e., a name that contains no
//	dots. In this case the namespace is taken from the most
//	tightly enclosing schema or protocol. For example, if "name":
//	"X" is specified, and this occurs within a field of the record
//	definition of org.foo.Y, then the fullname is org.foo.X. If
//	there is no enclosing namespace then the null namespace is
//	used.
func avroNameForDefinition(schemaMap map[string]interface{}, enclosing string) (avro.QualifiedName, error) {
	name, err := getMapString(schemaMap, "name")
	if err != nil {
		return avro.QualifiedName{}, err
	}
	var namespace string
	if _, ok := schemaMap["namespace"]; ok {
		namespace, err = getMapString(schemaMap, "namespace")
		if err != nil {
			return avro.QualifiedName{}, err
		}
	}
	if namespace != "" {
		enclosing = namespace
	}
	return ParseAvroName(enclosing, name), nil
}
