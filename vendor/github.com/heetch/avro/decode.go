package avro

import (
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/actgardner/gogen-avro/v10/vm"
	gouuid "github.com/google/uuid"
)

// Unmarshal unmarshals the given Avro-encoded binary data, which must
// have been written with Avro type described by wType,
// into x, which must be a pointer to a struct type.
//
// The reader type used is TypeOf(*x), and
// must be compatible with wType according to the
// rules described here:
// https://avro.apache.org/docs/current/spec.html#Schema+Resolution
//
// Unmarshal returns the reader type.
func Unmarshal(data []byte, x interface{}, wType *Type) (*Type, error) {
	return globalNames.Unmarshal(data, x, wType)
}

// Unmarshal is like the Unmarshal function except that names
// in the schema for x are renamed according to names.
func (names *Names) Unmarshal(data []byte, x interface{}, wType *Type) (*Type, error) {
	v := reflect.ValueOf(x)
	t := v.Type()
	if t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("destination is not a pointer %s", t)
	}
	prog, err := compileDecoder(names, t.Elem(), wType)
	if err != nil {
		return nil, err
	}
	v = v.Elem()
	return unmarshal(nil, data, prog, v)
}

// stackFrame represents the registers that are mutated by the VM interpreter.
type stackFrame struct {
	Boolean   bool
	Int       int64
	Float     float64
	Bytes     []byte
	String    string
	Condition bool
}

type decoder struct {
	pc      int
	program *decodeProgram

	// buf holds bytes read from r to be consumed
	// by the decoder. The unconsumed bytes are
	// in d.buf[d.scan:].
	buf     []byte
	scan    int
	r       io.Reader
	readErr error
}

type decodeError struct {
	err error
}

// unmarshal unmarshals Avro binary data from r and writes it to target
// following the given program.
func unmarshal(r io.Reader, buf []byte, prog *decodeProgram, target reflect.Value) (_ *Type, err error) {
	if debugging {
		debugf("unmarshal %x into %s", buf, target.Type())
	}
	defer func() {
		switch panicErr := recover().(type) {
		case *decodeError:
			err = panicErr.err
		case nil:
		default:
			panic(panicErr)
		}
	}()
	d := decoder{
		r:       r,
		program: prog,
	}
	if r == nil {
		d.buf = buf
		d.readErr = io.EOF
	} else {
		d.buf = make([]byte, 0, bufSize)
	}
	d.eval(target)
	return prog.readerType, nil
}

func (d *decoder) eval(target reflect.Value) {
	if debugging {
		if target.IsValid() {
			debugf("eval %s", target.Type())
		} else {
			debugf("eval nil")
		}
		defer debugf("}")
	}
	var frame stackFrame
	for ; d.pc < len(d.program.Instructions); d.pc++ {
		if debugging {
			debugf("x %d: %v", d.pc, d.program.Instructions[d.pc])
		}
		switch inst := d.program.Instructions[d.pc]; inst.Op {
		case vm.Read:
			switch inst.Operand {
			case vm.Null:
			case vm.Boolean:
				frame.Boolean = d.readBool()
			case vm.Int:
				// TODO bounds check. See https://github.com/heetch/avro/issues/33.
				frame.Int = d.readLong()
			case vm.Long:
				frame.Int = d.readLong()
			case vm.UnusedLong:
				d.readLong()
			case vm.Float:
				frame.Float = d.readFloat()
			case vm.Double:
				frame.Float = d.readDouble()
			case vm.Bytes:
				frame.Bytes = d.readBytes()
			case vm.String:
				frame.String = d.readString()
			default:
				frame.Bytes = d.readFixed(inst.Operand - 11)
			}
		case vm.Set:
			if debugging {
				debugf("%v on %s", inst, target.Type())
			}
			switch inst.Operand {
			case vm.Null:
			case vm.Boolean:
				target.SetBool(frame.Boolean)
			case vm.Long:
				// TODO support timestamp-millis.
				// Unfortunately we can't tell whether the instruction
				// is setting milliseconds or microseconds. We'll need
				// need more information from the VM to be able to
				// do that, so support only timestamp-micros for now.
				// See https://github.com/heetch/avro/issues/3
				switch target.Type() {
				case timeType:
					// timestamp-micros
					target.Set(reflect.ValueOf(time.Unix(frame.Int/1e6, frame.Int%1e6*1e3)))
				case durationType:
					// duration-nanos
					target.Set(reflect.ValueOf(time.Duration(frame.Int)))
				default:
					target.SetInt(frame.Int)
				}
			case vm.Int:
				target.SetInt(frame.Int)
			case vm.Float, vm.Double:
				target.SetFloat(frame.Float)
			case vm.Bytes:
				if target.Kind() == reflect.Array {
					n := reflect.Copy(target, reflect.ValueOf(frame.Bytes))
					if n != len(frame.Bytes) {
						d.error(fmt.Errorf("copied too little"))
					}
				} else {
					data := make([]byte, len(frame.Bytes))
					copy(data, frame.Bytes)
					target.SetBytes(data)
				}
			case vm.String:
				if target.Type() == uuidType {
					if frame.String == "" {
						// We produce the empty string "" when encoding zero UUID value,
						// so allow it when decoding too.
						target.Set(reflect.ValueOf(gouuid.UUID{}))
						break
					}
					val, err := gouuid.Parse(frame.String)
					if err != nil {
						d.error(fmt.Errorf("invalid UUID in Avro encoding: %w", err))
					} else {
						target.Set(reflect.ValueOf(val))
					}
					break
				}
				target.SetString(frame.String)
			}
		case vm.SetDefault:
			if d.program.makeDefault[d.pc] == nil {
				panic(fmt.Errorf("no makeDefault at PC %d; prog %p", d.pc, &d.program.makeDefault[0]))
			}
			v := d.program.makeDefault[d.pc]()
			target.Field(inst.Operand).Set(v)
		case vm.Enter:
			val, isRef := d.program.enter[d.pc](target)
			if debugging {
				debugf("enter %d -> %#v (isRef %v) {", inst.Operand, val, isRef)
			}
			d.pc++
			d.eval(val)
			if !isRef {
				target.Set(val)
			}
		case vm.Exit:
			if debugging {
				debugf("}")
			}
			return
		case vm.AppendArray:
			target.Set(reflect.Append(target, reflect.Zero(target.Type().Elem())))
			d.pc++
			d.eval(target.Index(target.Len() - 1))
		case vm.AppendMap:
			d.pc++
			elem := reflect.New(target.Type().Elem()).Elem()
			d.eval(elem)
			if target.IsNil() {
				// TODO we'd like to encode (null | map) by using a nil
				// map value, but because we're only making the map
				// when we append the first element, all empty maps
				// will also be nil. Perhaps when SetLong is called on the
				// union type, we should create the map.
				// The same applies to slices.
				// See https://github.com/heetch/avro/issues/19
				target.Set(reflect.MakeMap(target.Type()))
			}
			target.SetMapIndex(reflect.ValueOf(frame.String), elem)
		case vm.Call:
			curr := d.pc
			d.pc = inst.Operand
			d.eval(target)
			d.pc = curr
		case vm.SetExitNull:
			// This is a no-op by now as it's handled by isRef
		case vm.HintSize:
			// This is a performance improvement to put a capacity to slice
			if target.Kind() == reflect.Slice && target.IsZero() {
				target.Set(reflect.MakeSlice(target.Type(), 0, inst.Operand))
				if debugging {
					debugf("putting cap %d to %+v", inst.Operand, target)
				}
			} // It's also used for Maps but it's no-op here, see AppendMap for details.
		case vm.Return:
			return
		case vm.Jump:
			d.pc = inst.Operand - 1
		case vm.EvalGreater:
			frame.Condition = frame.Int > int64(inst.Operand)
		case vm.EvalEqual:
			frame.Condition = frame.Int == int64(inst.Operand)
		case vm.CondJump:
			if frame.Condition {
				d.pc = inst.Operand - 1
			}
		case vm.AddLong:
			frame.Int += int64(inst.Operand)
		case vm.SetLong:
			frame.Int = int64(inst.Operand)
		case vm.SetInt:
			frame.Int = int64(inst.Operand)
		case vm.MultLong:
			frame.Int *= int64(inst.Operand)
		case vm.PushLoop:
			loop := frame.Int
			d.pc++
			d.eval(target)
			frame.Int = loop
		case vm.PopLoop:
			return
		case vm.Halt:
			if inst.Operand == 0 {
				// This doesn't actually halt, but it doesn't seem to matter.
				return
			}
			d.error(fmt.Errorf("runtime error: %v, frame: %v, pc: %v", d.program.Errors[inst.Operand-1], frame, d.pc))
		default:
			d.error(fmt.Errorf("unknown instruction %v", d.program.Instructions[d.pc]))
		}
	}
}

func (d *decoder) error(err error) {
	panic(&decodeError{
		err: err,
	})
}
