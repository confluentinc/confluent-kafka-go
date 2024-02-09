// Compiler has methods to generate GADGT VM bytecode from Avro schemas
package compiler

import (
	"github.com/actgardner/gogen-avro/v10/parser"
	"github.com/actgardner/gogen-avro/v10/resolver"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/actgardner/gogen-avro/v10/vm"
)

// Given two Avro schemas, compile them into a program which can read the data
// written by `writer` and store it in the structs generated for `reader`.
// If you're reading records from an OCF you can use the New<RecordType>Reader()
// method that's generated for you, which will parse the schemas automatically.
func CompileSchemaBytes(writer, reader []byte, opts ...Option) (*vm.Program, error) {
	readerType, err := ParseSchema(reader)
	if err != nil {
		return nil, err
	}

	writerType, err := ParseSchema(writer)
	if err != nil {
		return nil, err
	}

	return Compile(writerType, readerType, opts...)
}

func ParseSchema(s []byte) (schema.AvroType, error) {
	ns := parser.NewNamespace(false)
	sType, err := ns.TypeForSchema(s)
	if err != nil {
		return nil, err
	}

	for _, def := range ns.Roots {
		if err := resolver.ResolveDefinition(def, ns.Definitions); err != nil {
			return nil, err
		}
	}
	return sType, nil
}

// Given two parsed Avro schemas, compile them into a program which can read the data
// written by `writer` and store it in the structs generated for `reader`.
func Compile(writer, reader schema.AvroType, opts ...Option) (*vm.Program, error) {
	log("Compile()\n writer:\n %v\n---\nreader: %v\n---\n", writer, reader)

	program := &irProgram{
		methods:       make(map[string]*irMethod),
		errors:        make([]string, 0),
		allowLaxNames: false,
	}

	for _, opt := range opts {
		opt(program)
	}

	program.main = newIRMethod("main", program)

	err := program.main.compileType(writer, reader)
	if err != nil {
		return nil, err
	}

	log("%v", program)
	compiled, err := program.CompileToVM()
	log("%v", compiled)
	return compiled, err
}
