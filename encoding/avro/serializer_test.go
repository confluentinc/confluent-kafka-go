package avro

import (
	"testing"
	"bytes"
	"reflect"
)

var testBuf = []byte{0x0, 0x0, 0x0, 0x0, 0x2A}

func TestSerializerIsValid(t *testing.T) {
	if validate(bytes.NewBuffer(testBuf)) {
		return
	}
	t.Logf("%v", testBuf)
	t.FailNow()
}

func TestSerializerWriteId(t *testing.T) {
	buf := make([]byte, 5)
	writeID(buf, 42)
	var ID int32
	extractID(bytes.NewReader(buf[1:]), &ID)
	if reflect.DeepEqual(buf, testBuf) {
		return
	}
	t.FailNow()
}

func TestSerializerExtractID(t *testing.T) {
	var ID int32
	buf := bytes.NewReader(testBuf)
	// Move cursor past Magic Byte
	buf.ReadByte()
	extractID(buf, &ID)

	if ID == 42 {
		return
	}
	t.FailNow()
}

