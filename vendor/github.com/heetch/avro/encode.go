package avro

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/actgardner/gogen-avro/v10/schema"
	gouuid "github.com/google/uuid"

	"github.com/heetch/avro/internal/typeinfo"
)

// Set to true for deterministic output.
const sortMapKeys = false

type encoderInfo struct {
	encode   encoderFunc
	avroType *Type
}

// Marshal encodes x as a message using the Avro binary
// encoding, using TypeOf(x) as the Avro type for marshaling.
//
// Marshal returns the encoded data and the actual type that
// was used for marshaling.
//
// See https://avro.apache.org/docs/current/spec.html#binary_encoding
func Marshal(x interface{}) ([]byte, *Type, error) {
	return globalNames.Marshal(x)
}

func marshalAppend(names *Names, buf []byte, xv reflect.Value) (_ []byte, _ *Type, marshalErr error) {
	avroType, enc := typeEncoder(names, xv.Type())
	e := &encodeState{
		Buffer: bytes.NewBuffer(buf),
	}
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(*encodeError); ok {
				marshalErr = err.err
			} else {
				panic(r)
			}
		}
	}()
	enc(e, xv)
	return e.Bytes(), avroType, nil
}

func typeEncoder(names *Names, t reflect.Type) (*Type, encoderFunc) {
	// Note: since a Go type can't encode as more than one definition,
	// we can use a purely Go-type-based cache.
	enc0, ok := names.goTypeToEncoder.Load(t)
	if ok {
		info := enc0.(*encoderInfo)
		return info.avroType, info.encode
	}
	at, err := avroTypeOf(names, t)
	if err != nil {
		return nil, errorEncoder(err)
	}
	b := &encoderBuilder{
		names:        names,
		typeEncoders: make(map[reflect.Type]encoderFunc),
	}
	enc := b.typeEncoder(at.avroType, t, typeinfo.Info{})
	names.goTypeToEncoder.LoadOrStore(t, &encoderInfo{
		avroType: at,
		encode:   enc,
	})
	return at, enc
}

type encodeState struct {
	*bytes.Buffer
	scratch [64]byte
}

// error aborts the encoding by panicking with err wrapped in encodeError.
func (e *encodeState) error(err error) {
	panic(&encodeError{err})
}

func errorEncoder(err error) encoderFunc {
	return func(e *encodeState, v reflect.Value) {
		e.error(err)
	}
}

type encodeError struct {
	err error
}

type encoderFunc func(e *encodeState, v reflect.Value)

type encoderBuilder struct {
	names        *Names
	typeEncoders map[reflect.Type]encoderFunc
}

// typeEncoder returns an encoder that encodes values of type t according
// to the Avro type at.
func (b *encoderBuilder) typeEncoder(at schema.AvroType, t reflect.Type, info typeinfo.Info) encoderFunc {
	if enc := b.typeEncoders[t]; enc != nil {
		return enc
	}
	switch at := at.(type) {
	case *schema.Reference:
		switch def := at.Def.(type) {
		case *schema.RecordDefinition:
			if t.Kind() != reflect.Struct {
				return errorEncoder(fmt.Errorf("expected struct"))
			}
			if len(info.Entries) == 0 {
				// The type itself might contribute information.
				info1, err := typeinfo.ForType(t)
				if err != nil {
					return errorEncoder(fmt.Errorf("cannot get info for %s: %v", info.Type, err))
				}
				info = info1
			}
			// To avoid an infinite loop on recursive types, make an
			// entry in the type-encoder map which will use the real
			// encoder function when after it's determined. This
			// indirect function will only be used for recursive
			// types.
			//
			// Credit to encoding/json for this idiom. We don't need
			// the sync.WaitGroup that encoding/json uses because
			// we're not concurrently populating a shared cache
			// here.
			var enc encoderFunc
			b.typeEncoders[t] = func(e *encodeState, v reflect.Value) {
				enc(e, v)
			}
			fieldEncoders := make([]encoderFunc, len(def.Fields()))
			indexes := make([]int, len(def.Fields()))
			for i, f := range def.Fields() {
				fieldInfo, ok := entryByName(info.Entries, f.Name())
				if !ok {
					return errorEncoder(fmt.Errorf("field %q not found in %s", f.Name(), t))
				}
				fieldIndex := fieldInfo.FieldIndex
				fieldEncoders[i] = b.typeEncoder(f.Type(), t.Field(fieldIndex).Type, info.Entries[i])
				indexes[i] = fieldIndex
			}
			enc = structEncoder{
				fieldEncoders: fieldEncoders,
				fieldIndexes:  indexes,
			}.encode
			return enc
		case *schema.EnumDefinition:
			return longEncoder
		case *schema.FixedDefinition:
			return fixedEncoder{def.SizeBytes()}.encode
		default:
			return errorEncoder(fmt.Errorf("unknown definition type %T", def))
		}
	case *schema.UnionField:
		atypes := at.ItemTypes()
		switch t.Kind() {
		case reflect.Ptr:
			// It's a union of null and one other type, represented by a Go pointer.
			if len(atypes) != 2 {
				return errorEncoder(fmt.Errorf("unexpected item type count in union"))
			}
			switch {
			case info.Entries[0].Type == nil:
				return ptrUnionEncoder{
					indexes:    [2]byte{0, 1},
					encodeElem: b.typeEncoder(atypes[1], info.Entries[1].Type, info.Entries[1]),
				}.encode
			case info.Entries[1].Type == nil:
				return ptrUnionEncoder{
					indexes:    [2]byte{1, 0},
					encodeElem: b.typeEncoder(atypes[0], info.Entries[0].Type, info.Entries[0]),
				}.encode
			default:
				return errorEncoder(fmt.Errorf("unexpected types in union"))
			}
		case reflect.Interface:
			enc := unionEncoder{
				nullIndex: -1,
				choices:   make([]unionEncoderChoice, len(info.Entries)),
			}
			for i, entry := range info.Entries {
				if entry.Type == nil {
					enc.nullIndex = i
				} else {
					enc.choices[i] = unionEncoderChoice{
						typ: entry.Type,
						enc: b.typeEncoder(atypes[i], entry.Type, entry),
					}
				}
			}
			return enc.encode
		default:
			return errorEncoder(fmt.Errorf("union type is not pointer or interface"))
		}
	case *schema.MapField:
		return mapEncoder{b.typeEncoder(at.ItemType(), t.Elem(), info)}.encode
	case *schema.ArrayField:
		return arrayEncoder{b.typeEncoder(at.ItemType(), t.Elem(), info)}.encode
	case *schema.BoolField:
		return boolEncoder
	case *schema.BytesField:
		return bytesEncoder
	case *schema.DoubleField:
		return doubleEncoder
	case *schema.FloatField:
		return floatEncoder
	case *schema.IntField:
		return longEncoder
	case *schema.NullField:
		return nullEncoder
	case *schema.LongField:
		switch t {
		case timeType:
			if lt := logicalType(at); lt == timestampMicros {
				return timestampMicrosEncoder
			} else {
				// TODO timestamp-millis support.
				return errorEncoder(fmt.Errorf("cannot encode time.Time as long with logical type %q", lt))
			}
		case durationType:
			if lt := logicalType(at); lt == durationNanos {
				return durationNanosEncoder
			} else {
				return errorEncoder(fmt.Errorf("cannot encode %t as long with logical type %q", t, lt))
			}
		default:
			return longEncoder
		}
	case *schema.StringField:
		if t == uuidType {
			if lt := logicalType(at); lt == uuid {
				return uuidEncoder
			} else {
				return errorEncoder(fmt.Errorf("cannot encode %v as string with logical type %q", t, lt))
			}
		}
		return stringEncoder
	default:
		return errorEncoder(fmt.Errorf("unknown avro schema type %T", at))
	}
}

func logicalType(t schema.AvroType) string {
	s, _ := t.Attribute("logicalType").(string)
	return s
}

func timestampMillisEncoder(e *encodeState, v reflect.Value) {
	t := v.Interface().(time.Time)
	if t.IsZero() {
		e.writeLong(0)
	} else {
		e.writeLong(t.Unix()*1e3 + int64(t.Nanosecond())/int64(time.Millisecond))
	}
}

func timestampMicrosEncoder(e *encodeState, v reflect.Value) {
	t := v.Interface().(time.Time)
	if t.IsZero() {
		e.writeLong(0)
	} else {
		e.writeLong(t.Unix()*1e6 + int64(t.Nanosecond())/int64(time.Microsecond))
	}
}

func uuidEncoder(e *encodeState, v reflect.Value) {
	if v.IsZero() {
		e.writeLong(int64(0))
		e.WriteString("")
	} else {
		t := v.Interface().(gouuid.UUID)
		s := t.String()
		e.writeLong(int64(len(s)))
		e.WriteString(s)
	}
}

func durationNanosEncoder(e *encodeState, v reflect.Value) {
	d := v.Interface().(time.Duration)
	e.writeLong(d.Nanoseconds())
}

type fixedEncoder struct {
	size int
}

func (fe fixedEncoder) encode(e *encodeState, v reflect.Value) {
	if v.CanAddr() {
		e.Write(v.Slice(0, fe.size).Bytes())
	} else {
		// TODO use a sync.Pool?
		buf := make([]byte, fe.size)
		reflect.Copy(reflect.ValueOf(buf), v)
		e.Write(buf)
	}
}

type mapEncoder struct {
	encodeElem encoderFunc
}

func (me mapEncoder) encode(e *encodeState, v reflect.Value) {
	n := v.Len()
	e.writeLong(int64(n))
	if n == 0 {
		return
	}
	if sortMapKeys {
		keys := make([]string, 0, n)
		for iter := v.MapRange(); iter.Next(); {
			keys = append(keys, iter.Key().String())
		}
		sort.Strings(keys)
		for _, k := range keys {
			kv := reflect.ValueOf(k)
			stringEncoder(e, kv)
			me.encodeElem(e, v.MapIndex(kv))
		}
	} else {
		for iter := v.MapRange(); iter.Next(); {
			stringEncoder(e, iter.Key())
			me.encodeElem(e, iter.Value())
		}
	}
	e.writeLong(0)
}

type arrayEncoder struct {
	encodeElem encoderFunc
}

func (ae arrayEncoder) encode(e *encodeState, v reflect.Value) {
	n := v.Len()
	e.writeLong(int64(n))
	if n == 0 {
		return
	}
	for i := 0; i < n; i++ {
		ae.encodeElem(e, v.Index(i))
	}
	e.writeLong(0)
}

func boolEncoder(e *encodeState, v reflect.Value) {
	if v.Bool() {
		e.WriteByte(1)
	} else {
		e.WriteByte(0)
	}
}

func nullEncoder(e *encodeState, v reflect.Value) {
}

func longEncoder(e *encodeState, v reflect.Value) {
	e.writeLong(v.Int())
}

func (e *encodeState) writeLong(x int64) {
	n := binary.PutVarint(e.scratch[:], x)
	e.Write(e.scratch[:n])
}

func floatEncoder(e *encodeState, v reflect.Value) {
	binary.LittleEndian.PutUint32(e.scratch[:], math.Float32bits(float32(v.Float())))
	e.Write(e.scratch[:4])
}

func doubleEncoder(e *encodeState, v reflect.Value) {
	binary.LittleEndian.PutUint64(e.scratch[:], math.Float64bits(v.Float()))
	e.Write(e.scratch[:8])
}

func bytesEncoder(e *encodeState, v reflect.Value) {
	data := v.Bytes()
	e.writeLong(int64(len(data)))
	e.Write(data)
}

func stringEncoder(e *encodeState, v reflect.Value) {
	s := v.String()
	e.writeLong(int64(len(s)))
	e.WriteString(s)
}

type structEncoder struct {
	fieldIndexes  []int
	fieldEncoders []encoderFunc
}

func (se structEncoder) encode(e *encodeState, v reflect.Value) {
	for i, index := range se.fieldIndexes {
		se.fieldEncoders[i](e, v.Field(index))
	}
}

type unionEncoderChoice struct {
	typ reflect.Type
	enc encoderFunc
}

type unionEncoder struct {
	// nullIndex holds the union index of the null alternative,
	// or -1 if there is none.
	nullIndex int
	// use a slice because unions are usually small and
	// a linear traversal is faster then.
	choices []unionEncoderChoice
}

func (ue unionEncoder) encode(e *encodeState, v reflect.Value) {
	if v.IsNil() {
		if ue.nullIndex != -1 {
			e.writeLong(int64(ue.nullIndex))
			return
		}
		e.error(fmt.Errorf("nil value not allowed"))
	}
	v = v.Elem()
	vt := v.Type()
	for i, choice := range ue.choices {
		if choice.typ == vt {
			e.writeLong(int64(i))
			choice.enc(e, v)
			return
		}
	}
	e.error(fmt.Errorf("unknown type for union %s", vt))
}

type ptrUnionEncoder struct {
	indexes    [2]byte
	encodeElem encoderFunc
}

func (pe ptrUnionEncoder) encode(e *encodeState, v reflect.Value) {
	if v.IsNil() {
		e.writeLong(int64(pe.indexes[0]))
		return
	}
	e.writeLong(int64(pe.indexes[1]))
	pe.encodeElem(e, v.Elem())
}
