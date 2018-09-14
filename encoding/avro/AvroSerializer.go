package avro

import (
	"github.com/rnpridgeon/avro"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

type Schema interface {
	avro.Schema
}

type AvroRecord interface {
	avro.AvroRecord
}

type GenericAvroRecord interface {
	AvroRecord
	Set(name string, value interface{})
}

// NewGenericRecord returns a new generic Avro record
func NewGenericRecord(schema Schema)(GenericAvroRecord) {
	return avro.NewGenericRecord(schema)
}

type datumWriter struct {
	 metadata []byte
	 avro.DatumWriter
}

type datumReader struct {
	avro.DatumReader
}

type AvroSerializer struct {
	datumWriter map[int]*datumWriter
	*avroSerde
}

type AvroDeserializer struct {
	datumReader map[int32]*datumReader
	*avroSerde
}

// NewAvroSerializer provides a unconfigured AvroSerializer
// *Note* Configure should be handled in the Producer
func NewAvroSerializer() (kafka.Serializer) {
	return &AvroSerializer{
		datumWriter: make(map[int]*datumWriter),
		avroSerde: &avroSerde{},
	}
}

// writeID adds the schema ID to the buffer
func writeID(buffer []byte, value int) {
	binary.BigEndian.PutUint32(buffer, uint32(value))
}

// getSchema returns the datum schema based on its type
func getSchema(datum interface{}) (Schema, error) {
	switch datum.(type) {
	case AvroRecord:
		return datum.(avro.AvroRecord).Schema(), nil
	case nil:
		return &avro.NullSchema{}, nil
	case bool:
		return &avro.BooleanSchema{}, nil
	case int:
		return nil, fmt.Errorf("Illegal type int, only int32 an int64 datums are supported")
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
		return nil, fmt.Errorf("unsupported type %T ", datum)
	}
}

// Serialize writes converts datum to the schema registry wire format
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
func (s *AvroSerializer) Serialize(topic *string, datum interface{}) ([]byte, error) {
	datumSchema, err := getSchema(datum)
	if err != nil {
		return nil, err
	}

	stringSchema := datumSchema.String()
	id, err := s.client.Register(s.getSubject(topic, datumSchema), &stringSchema)
	if err != nil {
		return nil, err
	}

	s.Lock()
	writer, ok := s.datumWriter[id]
	if !ok {
		buffer := make([]byte, schemaId+1)
		buffer[0] = magicByte
		writeID(buffer[1:], id)

		writer = &datumWriter{
			buffer,
			avro.NewDatumWriter(datumSchema),
		}

		s.datumWriter[id] = writer
	}
	s.Unlock()

	writeBuffer := new(bytes.Buffer)
	_, err = writeBuffer.Write(writer.metadata[:])
	if err != nil {
		return nil, err
	}

	err = writer.Write(datum, avro.NewBinaryEncoder(writeBuffer))

	return writeBuffer.Bytes(), err
}

// NewAvroDeserializer provides a unconfigured AvroDeserializer
// *Note* Configure should be handled in the Producer
func NewAvroDeserializer() (kafka.Deserializer) {
	return &AvroDeserializer{
		datumReader: make(map[int32]*datumReader),
		avroSerde: &avroSerde{},
	}
}

// valid performs a sanity check on the buffer
func valid(r *bytes.Buffer) bool {
	magic, _ := r.ReadByte()
	return r.Len() >= schemaId && magic == magicByte
}

// extractID reads the schema id into an int32 pointer
func extractID(r io.Reader, id *int32) (error) {
	binary.Read(r, binary.BigEndian, id)
	return nil
}

func (d *AvroDeserializer) extractSchema(r io.Reader, id *int32) (Schema, error) {
	if err := extractID(r, id); err != nil {
		return nil, err
	}
	schema, err := d.client.GetByID(int(*id))
	if err != nil {
		return nil, err
	}

	return Parse(*schema)
}

// get reader returns the reader identified by id
func (d *AvroDeserializer) getReader(id int32, schema Schema) (avro.DatumReader) {
	reader, ok := d.datumReader[id]
	if !ok {
		reader = &datumReader{avro.NewDatumReader(schema)}
		d.datumReader[id] = reader
	}
	return reader
}

func (d *AvroDeserializer) decode(reader avro.DatumReader, schema Schema, payload []byte) (interface{}, error) {
	record := avro.NewGenericRecord(schema)
	return record, reader.Read(record, avro.NewBinaryDecoder(payload))
}

func getValue(schema Schema, decoder avro.Decoder, reader avro.DatumReader) (interface{}, error) {
	switch schema.Type(){
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
		record := avro.NewGenericRecord(schema)
		return record, reader.Read(record,decoder)
	}
}
// Deserialize writes converts datum to the schema registry wire format
// https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
func (d *AvroDeserializer) Deserialize(topic *string, data []byte) (interface{}, error) {
	buffer := bytes.NewBuffer(data)
	if !valid(buffer) {
		log.Printf("%v", buffer.Bytes())
		return nil, fmt.Errorf("serialization error: invalid message")
	}

	var id int32
	schema, err := d.extractSchema(buffer, &id)

	if err != nil {
		return nil, err
	}

	d.Lock()
	reader := d.getReader(id, schema)
	d.Unlock()

	return getValue(schema, avro.NewBinaryDecoder(buffer.Bytes()), reader)
}
