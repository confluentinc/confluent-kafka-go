package avro

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/actgardner/gogen-avro/v10/vm"
	gouuid "github.com/google/uuid"

	"github.com/heetch/avro/internal/typeinfo"
)

var (
	timeType     = reflect.TypeOf(time.Time{})
	durationType = reflect.TypeOf(time.Duration(0))
	byteType     = reflect.TypeOf(byte(0))
	uuidType     = reflect.TypeOf(gouuid.UUID{})
)

type decodeProgram struct {
	vm.Program

	// enter holds an entry for each Enter instruction in the
	// program, indexed by pc, that gets a value that
	// can be assigned to for the given index.
	// It reports whether the returned value is a reference
	// directly into the target value (for example when
	// the target is a struct type).
	enter []enterFunc
	// makeDefault holds an entry for each SetDefault instruction
	// in the program, indexed by pc, that gets the default
	// value for a field.
	makeDefault []func() reflect.Value

	readerType *Type
}

type analyzer struct {
	prog        *vm.Program
	pcInfo      []pcInfo
	enter       []enterFunc
	makeDefault []func() reflect.Value
}

// enterFunc is used to "enter" a field or union value.
// It's passed the outer value and returns the inner value
// and also reports whether the inner value is a direct
// reference to a part of the outer one.
type enterFunc = func(reflect.Value) (reflect.Value, bool)

type pcInfo struct {
	// path holds the descent path into the type for an instruction
	// in the program. It has an entry for each Enter
	// (record field or union), AppendArray or AppendMap
	// instruction encountered when executing the VM up
	// until the instruction.
	path []pathElem

	// traces holds the set of call stacks we've found that lead
	// to this instruction.
	traces [][]int
}

func (info *pcInfo) addTrace(stack []int) bool {
	for _, st := range info.traces {
		if stackeq(st, stack) {
			return false
		}
	}
	info.traces = append(info.traces, append([]int(nil), stack...))
	return true
}

func stackeq(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type pathElem struct {
	// ftype holds the type of the value for this element.
	ftype reflect.Type
	// info holds the type info for the element.
	info typeinfo.Info
	// avroType holds the corresponding Avro type
	// that we're looking at.
	avroType schema.AvroType
}

// compileDecoder returns a decoder program to decode into values of the given type
// Avro values encoded with the given writer schema.
func compileDecoder(names *Names, t reflect.Type, writerType *Type) (*decodeProgram, error) {
	// First determine the schema for the type.
	readerType, err := avroTypeOf(names, t)
	if err != nil {
		return nil, fmt.Errorf("cannot determine schema for %s: %v", t, err)
	}
	if debugging {
		debugf("compiling:\nwriter type: %s\nreader type: %s\n", writerType, readerType)
	}
	prog, err := compiler.Compile(writerType.avroType, readerType.avroType, compiler.AllowLaxNames())
	if err != nil {
		return nil, fmt.Errorf("cannot create decoder: %v", err)
	}
	prog1, err := analyzeProgramTypes(prog, t, readerType.avroType)
	if err != nil {
		return nil, fmt.Errorf("analysis failed: %v", err)
	}
	prog1.readerType = readerType
	return prog1, nil
}

// analyzeProgramTypes analyses the given program with
// respect to the given type (the program must have been generated for that
// type) and returns a program with a populated "enter" field allowing
// the VM to correctly create union and field values for Enter instructions.
func analyzeProgramTypes(prog *vm.Program, t reflect.Type, readerType schema.AvroType) (*decodeProgram, error) {
	a := &analyzer{
		prog:        prog,
		pcInfo:      make([]pcInfo, len(prog.Instructions)),
		enter:       make([]enterFunc, len(prog.Instructions)),
		makeDefault: make([]func() reflect.Value, len(prog.Instructions)),
	}
	if debugging {
		debugf("analyze %d instructions; type %s\n%s {", len(prog.Instructions), t, prog)
	}
	defer debugf("}")
	info, err := typeinfo.ForType(t)
	if err != nil {
		return nil, err
	}
	if err := a.eval([]int{0}, nil, []pathElem{{
		ftype:    t,
		info:     info,
		avroType: readerType,
	}}); err != nil {
		return nil, fmt.Errorf("eval: %v", err)
	}
	prog1 := &decodeProgram{
		Program:     *prog,
		enter:       a.enter,
		makeDefault: a.makeDefault,
	}
	// Sanity check that all Enter and SetDefault
	// instructions have associated info.
	for i, inst := range prog.Instructions {
		switch inst.Op {
		case vm.Enter:
			if prog1.enter[i] == nil {
				return nil, fmt.Errorf("enter not set; pc %d; instruction %v", i, inst)
			}
		case vm.SetDefault:
			if prog1.makeDefault[i] == nil {
				return nil, fmt.Errorf("makeDefault not set; pc %d; instruction %v", i, inst)
			}
		}
	}
	return prog1, nil
}

// eval runs a limited evaluation of the program to determine the appropriate
// action to take for each Enter and SetDefault instruction.
// The stack holds the program counter stack; calls holds the
// called PCs (the start of each function in the stack) and path
// holds info on the Go type at each level of the enter/exit stack.
//
// When the analysis is complete, we should have reached all
// instructions in the program.
func (a *analyzer) eval(stack []int, calls []int, path []pathElem) (retErr error) {
	if debugging {
		debugf("analyzer.eval %v; path %s{", stack, pathStr(path))
	}
	defer func() {
		if debugging {
			debugf("} -> %v", retErr)
		}
	}()
	for {
		pc := stack[len(stack)-1]
		if pc >= len(a.prog.Instructions) {
			break
		}
		if a.pcInfo[pc].path == nil {
			// Update the type info for the current PC with a copy
			// of the current path.
			a.pcInfo[pc].path = append(a.pcInfo[pc].path, path...)
		} else {
			if debugging {
				debugf("already evaluated instruction %d", pc)
			}
			// Sanity-check our assumptions about the VM:
			// we should always be inside the same path if
			// we're at the same PC.
			if !equalPathRef(path, a.pcInfo[pc].path) {
				return fmt.Errorf("type mismatch (\n\tprevious %s\n\tnew %s\n)", pathStr(a.pcInfo[pc].path), pathStr(path))
			}
			if !a.pcInfo[pc].addTrace(stack) {
				// We've been exactly here before,
				// so we can stop analysing.
				return nil
			}
		}

		elem := path[len(path)-1]
		if debugging {
			debugf("exec %d: %v; ftype %v", pc, a.prog.Instructions[pc], elem.ftype)
		}
		switch inst := a.prog.Instructions[pc]; inst.Op {
		case vm.Set:
			if elem.info.IsUnion {
				if debugging {
					debugf("patching Set to Nop")
				}
				// Set on a union type is just to set the type of the union,
				// which is implicit with the next Enter, so we want to just
				// ignore the instruction, so replace it with a jump to the next instruction,
				// as there's no vm.Nop available.
				a.prog.Instructions[pc] = vm.Instruction{
					Op:      vm.Jump,
					Operand: pc + 1,
				}
				break
			}
			// TODO: sanity-check that if it's Set(Bytes), the previous
			// instruction was Read(Bytes) (i.e. frame.Bytes hasn't been invalidated).
			if !canAssignVMType(inst.Operand, elem.ftype) {
				return fmt.Errorf("cannot assign %v to %s", operandString(inst.Operand), elem.ftype)
			}
		case vm.Enter:
			index := inst.Operand
			if debugging {
				debugf("enter %d -> %v, %d entries", index, elem.info.Type, len(elem.info.Entries))
			}
			enterf, newElem, err := enter(elem, index)
			if err != nil {
				return fmt.Errorf("cannot enter: %v", err)
			}
			path = append(path, newElem)
			a.enter[pc] = enterf
		case vm.AppendArray:
			if elem.ftype.Kind() != reflect.Slice {
				return fmt.Errorf("cannot append to %T", elem.ftype)
			}
			newElem, err := enterContainer(elem)
			if err != nil {
				return fmt.Errorf("cannot enter array: %v", err)
			}
			path = append(path, newElem)
			if debugging {
				debugf("append array enter -> %v", elem.ftype.Elem())
			}
		case vm.AppendMap:
			if elem.ftype.Kind() != reflect.Map {
				return fmt.Errorf("cannot append to %T", elem.ftype)
			}
			if elem.ftype.Key().Kind() != reflect.String {
				return fmt.Errorf("invalid key type for map %s", elem.ftype)
			}
			newElem, err := enterContainer(elem)
			if err != nil {
				return fmt.Errorf("cannot enter map: %v", err)
			}
			path = append(path, newElem)
		case vm.Exit:
			if len(path) == 0 {
				return fmt.Errorf("unbalanced exit")
			}
			path = path[:len(path)-1]
		case vm.SetExitNull:
			// Do nothing as Null value is treated in enter* functions

		case vm.SetDefault:
			index := inst.Operand
			if index >= len(elem.info.Entries) {
				return fmt.Errorf("set-default index out of bounds; pc %d; type %s", pc, elem.ftype)
			}
			info := elem.info.Entries[index]
			if info.MakeDefault == nil {
				return fmt.Errorf("no default info found at index %d at %v", index, pathStr(path))
			}
			a.makeDefault[pc] = info.MakeDefault
		case vm.Call:
			found := false
			for _, pc := range calls {
				if pc == inst.Operand {
					// We've already called this in the current stack, so
					// it's a recursive call, so ignore it.
					found = true
					break
				}
			}
			if !found {
				calls = append(calls, inst.Operand)
				stack = append(stack, inst.Operand-1)
			}
		case vm.Return:
			if len(stack) == 0 {
				return fmt.Errorf("empty stack")
			}
			stack = stack[:len(stack)-1]
			calls = calls[:len(calls)-1]
		case vm.CondJump:
			if debugging {
				debugf("split {")
			}
			// Execute one path of the condition with a forked
			// version of the state before carrying on with the
			// current execution flow.
			stack1 := make([]int, len(stack), cap(stack))
			copy(stack1, stack)
			stack1[len(stack1)-1] = inst.Operand
			calls1 := make([]int, len(calls))
			copy(calls1, calls)
			path1 := make([]pathElem, len(path), cap(path))
			copy(path1, path)
			if err := a.eval(stack1, calls1, path1); err != nil {
				return err
			}
			if debugging {
				debugf("}")
			}
		case vm.Jump:
			stack[len(stack)-1] = inst.Operand - 1
		case vm.EvalGreater,
			vm.EvalEqual,
			vm.SetInt,
			vm.SetLong,
			vm.AddLong,
			vm.MultLong,
			vm.PushLoop,
			vm.PopLoop,
			vm.Read,
			vm.HintSize:
			// We don't care about any of these instructions because
			// they can't influence the types that we're traversing.
		case vm.Halt:
			return nil
		default:
			return fmt.Errorf("unknown instruction %v", inst.Op)
		}
		stack[len(stack)-1]++
	}
	return nil
}

// enter returns an enter function and the new path element
// resulting from an Enter into the given path element at
// the given index.
//
// The enter function will be used to execute the Enter instruction
// at decode time - it takes the value being decoded into
// and returns the new value to decode into and also reports
// whether the new value is a reference into the original
// value (if not, it will need to be copied into the original value).
func enter(elem pathElem, index int) (enterFunc, pathElem, error) {
	var entryType schema.AvroType
	var info typeinfo.Info
	switch at := elem.avroType.(type) {
	case *schema.UnionField:
		itemTypes := at.ItemTypes()
		if len(elem.info.Entries) != len(itemTypes) {
			return nil, pathElem{}, fmt.Errorf("union type mismatch")
		}
		if index >= len(elem.info.Entries) {
			return nil, pathElem{}, fmt.Errorf("union index out of bounds")
		}

		entryType = itemTypes[index]
		info = elem.info.Entries[index]
	case *schema.Reference:
		switch def := at.Def.(type) {
		case *schema.RecordDefinition:
			fields := def.Fields()
			if index >= len(fields) {
				return nil, pathElem{}, fmt.Errorf("field index out of bounds (%d/%d)", index, len(fields))
			}
			field := fields[index]
			// The reader type might not exactly match the
			// entries inferred from the Go type because
			// of external types (external types are allowed
			// to add and reorder fields without breaking the
			// API), so search through the struct fields, looking
			// for a field that matches the Avro field.
			info1, ok := entryByName(elem.info.Entries, field.Name())
			if !ok {
				return nil, pathElem{}, fmt.Errorf("could not find entry for field %q in %v", field.Name(), elem.ftype)
			}
			info = info1
			entryType = field.Type()
		default:
			return nil, pathElem{}, fmt.Errorf("unexpected Enter on Avro definition %T", def)
		}
	default:
		return nil, pathElem{}, fmt.Errorf("unexpected Enter on Avro type %T", at)
	}
	if info.Type == nil {
		// Special case for the nil type. Return
		// a zero value that will never be used.
		return func(v reflect.Value) (reflect.Value, bool) {
			return reflect.Value{}, true
		}, pathElem{}, nil
	}
	if len(info.Entries) == 0 {
		// The type itself might contribute information.
		info1, err := typeinfo.ForType(info.Type)
		if err != nil {
			return nil, pathElem{}, fmt.Errorf("cannot get info for %s: %v", info.Type, err)
		}
		info1.FieldIndex = info.FieldIndex
		info = info1
	}
	newElem := pathElem{
		ftype:    info.Type,
		info:     info,
		avroType: entryType,
	}
	var enter func(v reflect.Value) (reflect.Value, bool)
	switch elem.ftype.Kind() {
	case reflect.Struct:
		fieldIndex := info.FieldIndex
		enter = func(v reflect.Value) (reflect.Value, bool) {
			debugf("entering field %d in type %v", fieldIndex, v.Type())
			return v.Field(fieldIndex), true
		}
	case reflect.Interface:
		enter = func(v reflect.Value) (reflect.Value, bool) {
			return reflect.New(info.Type).Elem(), false
		}
	case reflect.Ptr:
		if len(elem.info.Entries) != 2 {
			return nil, pathElem{}, fmt.Errorf("pointer type without a two-member union")
		}
		enter = func(v reflect.Value) (reflect.Value, bool) {
			inner := reflect.New(info.Type)
			v.Set(inner)
			return inner.Elem(), true
		}
	default:
		return nil, pathElem{}, fmt.Errorf("unexpected type %v for Enter", elem.ftype)
	}
	return enter, newElem, nil
}

// enterContainer returns the path element resulting
// from descending into a map or array container
// represented by elem.
func enterContainer(elem pathElem) (pathElem, error) {
	type container interface {
		ItemType() schema.AvroType
	}
	elem1 := pathElem{
		ftype:    elem.ftype.Elem(),
		info:     elem.info,
		avroType: elem.avroType.(container).ItemType(),
	}
	if len(elem1.info.Entries) == 0 {
		// The type itself might contribute information.
		info, err := typeinfo.ForType(elem1.ftype)
		if err != nil {
			return pathElem{}, fmt.Errorf("cannot get info for %s: %v", info.Type, err)
		}
		elem1.info = info
	}
	return elem1, nil
}

func entryByName(entries []typeinfo.Info, fieldName string) (typeinfo.Info, bool) {
	for _, entry := range entries {
		if entry.FieldName == fieldName {
			return entry, true
		}
	}
	return typeinfo.Info{}, false
}

func canAssignVMType(operand int, dstType reflect.Type) bool {
	// Note: the logic in this switch reflects the Set logic in the decoder.eval method.
	dstKind := dstType.Kind()
	switch operand {
	case vm.Null:
		return true
	case vm.Boolean:
		return dstKind == reflect.Bool
	case vm.Int, vm.Long:
		return dstType == timeType || dstType == durationType || reflect.Int <= dstKind && dstKind <= reflect.Int64
	case vm.Float, vm.Double:
		return dstKind == reflect.Float64 || dstKind == reflect.Float32
	case vm.Bytes:
		if dstKind == reflect.Array {
			return dstType.Elem() == byteType
		}
		return dstKind == reflect.Slice && dstType.Elem() == byteType
	case vm.String:
		return dstKind == reflect.String || dstType == uuidType
	default:
		return false
	}
}

func equalPathRef(p1, p2 []pathElem) bool {
	if len(p1) == 0 || len(p2) == 0 {
		return len(p1) == len(p2)
	}
	return p1[len(p1)-1].ftype == p2[len(p2)-1].ftype
}

func pathStr(ps []pathElem) string {
	var buf strings.Builder
	buf.WriteString("{")
	for i, p := range ps {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s", p.ftype)
	}
	buf.WriteString("}")
	return buf.String()
}

var operandStrings = []string{
	vm.Unused:     "unused",
	vm.Null:       "null",
	vm.Boolean:    "boolean",
	vm.Int:        "int",
	vm.Long:       "long",
	vm.Float:      "float",
	vm.Double:     "double",
	vm.Bytes:      "bytes",
	vm.String:     "string",
	vm.UnionElem:  "unionelem",
	vm.UnusedLong: "unusedlong",
}

func operandString(op int) string {
	if op < 0 || op >= len(operandStrings) {
		return fmt.Sprintf("unknown%d", op)
	}
	return operandStrings[op]
}

const debugging = false

func debugf(f string, a ...interface{}) {
	if debugging {
		log.Printf(f, a...)
	}
}
