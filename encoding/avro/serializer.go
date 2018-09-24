package avro

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rnpridgeon/avro"
	"io"
	"sync"
	"reflect"
)

// DatumWriter wrapper
type datumWriter struct {
	metadata []byte
	avro.DatumWriter
}

// DaturmReader wrapper
type datumReader struct {
	avro.DatumReader
}

// Serializer implements the Serializer interface enabling objects to be encoded in the Avro BinaryFormat
type Serializer struct {
	sync.Mutex
	datumWriters map[int]*datumWriter
	avroSerde
}

// NewAvroSerializer returns a new Serializer instance.
// Configuration values should be provided in the Kafka [Producer|Consumer]'s ConfigMap prefixed with "schema.registry".
// The Kafka [Producer|Consumer] instance will initialize this Serializer with a call to Configure
func NewSerializer() kafka.Serializer {
	return &Serializer{
		datumWriters: make(map[int]*datumWriter),
		avroSerde:   avroSerde{},
	}
}

// Serialize returns the binary representation of datum as described in the Confluent Schema Registry docs.
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
func (s *Serializer) Serialize(msg *kafka.Message) (error) {
	var datum interface{}

	if s.IsKey {
		datum = msg.KeyObject
	} else  {
		datum = msg.ValueObject
	}

	// Extract schema from datum
	schema, err := getSchema(datum)
	if err != nil {
		return kafka.NewError(kafka.ErrInvalidArg, err)
	}

	id, err := s.client.Register(s.getSubject(msg.TopicPartition.Topic, schema), schema)
	if err != nil {
		return kafka.NewError(kafka.ErrInvalidArg, err)
	}

	// Get datumWriter from cache identified by the Schema ID
	// If the datumWriter is not available create one
	s.Lock()
	writer, ok := s.datumWriters[id]
	if !ok {
		// prepare buffer with Confluent Schema Registry metadata
		metaBuffer := make([]byte, schemaID + 1)
		metaBuffer [0] = magicByte
		writeID(metaBuffer[1:], id)

		writer = &datumWriter{
			metaBuffer,
			avro.NewDatumWriter(schema),
		}

		s.datumWriters[id] = writer
	}
	s.Unlock()

	buffer := make([]byte, len(writer.metadata))
	copy(buffer, writer.metadata)
	writeBuffer := bytes.NewBuffer(buffer)

	writer.Write(datum, avro.NewBinaryEncoder(writeBuffer))
	if s.IsKey {
		msg.Key = writeBuffer.Bytes()
	} else  {
		msg.Value = writeBuffer.Bytes()
	}

	return kafka.NewSerializationError(s.IsKey,err)
}

// gteSchema returns the schema from the provided datum
func getSchema(datum interface{}) (Schema, error) {
	switch datum.(type) {
	case Record:
		return datum.(Record).Schema(), nil
	case nil:
		return &avro.NullSchema{}, nil
	case bool:
		return &avro.BooleanSchema{}, nil
	case int:
		return nil, errors.New("integer precision must be specified, use int32 or int64 instead")
	case int32:
		return &avro.IntSchema{}, nil
	case int64:
		return &avro.LongSchema{}, nil
	case float32:
		return &avro.FloatSchema{}, nil
	case float64:
		return &avro.DoubleSchema{}, nil
	case string:
		return &avro.StringSchema{}, nil
	case []byte:
		return &avro.BytesSchema{}, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", datum)
	}
}

// writeID adds the schema ID to the buffer.
// Complete wire format documentation can be found in the Confluent Schema Registry Docs
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format.
func writeID(buffer []byte, value int) {
	// Skip magic byte position
	binary.BigEndian.PutUint32(buffer, uint32(value))

}

//setNewFunc returns a function for creating new instances of type i
func setNewFunc(s Schema) func()Schema{
	t := reflect.TypeOf(s)
	return func()Schema{
		return reflect.Zero(t).Interface().(Schema)
	}
}

// Deserializer implements the Deserializer interface enabling objects to be decoded from the Avro BinaryFormat
type Deserializer struct {
	sync.Mutex
	newFunc func()Schema
	reader map[int32]*datumReader
	avroSerde
}

// NewDeserializer returns a deserializer for type t.
// t must one of SpecificRecord, GenericRecord or an Avro Primitive i.e. avro.String
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
func NewDeserializer(s Schema) kafka.Deserializer {
	d := &Deserializer{
		newFunc: setNewFunc(s),
		avroSerde:    avroSerde{},
	}
	return d
}

// NewDeserializer returns a deserializer for type t.
// t must one of SpecificRecord, GenericRecord or an Avro Primitive i.e. avro.String
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
func NewGenericDeserializer() kafka.Deserializer {
	d := &Deserializer{
		reader: make(map[int32]*datumReader),
		avroSerde:    avroSerde{},
	}
	return d
}

// validate advances the reader 1 byte then validates the SR components of the buffer
func validate(r *bytes.Buffer) bool {
	magic, _ := r.ReadByte()
	return r.Len() >= schemaID && magic == magicByte
}

// extractID reads the schema id into an int32 pointer
func extractID(r io.Reader, id *int32) error {
	return binary.Read(r, binary.BigEndian, id)
}

// fetchSchema extracts the schema id from reader r then queries the schema registry returning the resulting schema
func (d *Deserializer) fetchSchema(r io.Reader, id *int32) (Schema, error) {
	if err := extractID(r, id); err != nil {
		return nil, err
	}

	return d.client.GetByID(int(*id))
}

// getValue reads an avro type into a go type
func getValue(schema Schema, decoder avro.Decoder, reader avro.DatumReader) (interface{}, error) {
	switch schema.Type() {
	case avro.String:
		return decoder.ReadString()
	case avro.Bytes:
		return decoder.ReadBytes()
	case avro.Int:
		return decoder.ReadInt()
	case avro.Long:
		return decoder.ReadLong()
	case avro.Float:
		return decoder.ReadFloat()
	case avro.Double:
		return decoder.ReadDouble()
	case avro.Boolean:
		return decoder.ReadBoolean()
	case avro.Null:
		return decoder.ReadNull()
	default:
		record := NewGenericRecord(schema)
		return record, reader.Read(record, decoder)
	}
	return nil, nil
}

// get reader returns the reader identified by id
func (d *Deserializer) getReader(id int32, schema Schema) (avro.DatumReader) {
	reader, ok := d.reader[id]
	if !ok {
		reader = &datumReader{avro.NewDatumReader(schema)}
		d.reader[id] = reader
	}
	return reader
}

// Deserialize reads the contents of msg and returns the configured type
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
func (d *Deserializer) Deserialize(msg *kafka.Message) error {
	var buffer *bytes.Buffer

	if d.IsKey {
		buffer = bytes.NewBuffer(msg.Key)
	} else {
		buffer = bytes.NewBuffer(msg.Value)
	}

	if !validate(buffer) {
		return kafka.NewDeserializationError(d.IsKey, fmt.Errorf("serialization error: invalid message"))
	}

	var id int32
	schema, err := d.fetchSchema(buffer, &id)
	if err != nil {
		return kafka.NewDeserializationError(d.IsKey, err)
	}

	reader := d.getReader(id, schema)
	value, err := getValue(schema, avro.NewBinaryDecoder(buffer.Bytes()), reader)

	if d.IsKey {
		msg.KeyObject = value
	} else {
		msg.ValueObject = value
	}

	return kafka.NewDeserializationError(d.IsKey, err)
}
