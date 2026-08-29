/**
 * Copyright 2024 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package serde

import (
	"testing"
)

func TestGUID(t *testing.T) {
	MaybeFail = InitFailFunc(t)

	schemaID := SchemaID{
		SchemaType: "AVRO",
	}
	input := []byte{
		0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
		0xa8, 0x02, 0xe2,
	}
	schemaID.FromBytes(input)
	guid := schemaID.GUID.String()
	MaybeFail("same guids", Expect(guid, "89791762-2336-4186-9674-299b90a802e2"))

	output, err := schemaID.GUIDToBytes()
	MaybeFail("same bytes", err, Expect(output, input))
}

func TestID(t *testing.T) {
	MaybeFail = InitFailFunc(t)

	schemaID := SchemaID{
		SchemaType: "AVRO",
	}
	input := []byte{
		0x00, 0x00, 0x00, 0x00, 0x01,
	}
	schemaID.FromBytes(input)
	id := schemaID.ID
	MaybeFail("same ids", Expect(id, 1))

	output, err := schemaID.IDToBytes()
	MaybeFail("same bytes", err, Expect(output, input))
}

func TestGUIDWithMessageIndex(t *testing.T) {
	MaybeFail = InitFailFunc(t)

	schemaID := SchemaID{
		SchemaType: "PROTOBUF",
	}
	input := []byte{
		0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
		0xa8, 0x02, 0xe2, 0x06, 0x02, 0x04, 0x06,
	}
	schemaID.FromBytes(input)
	guid := schemaID.GUID.String()
	MaybeFail("same guids", Expect(guid, "89791762-2336-4186-9674-299b90a802e2"))

	msgIndexes := schemaID.MessageIndexes
	MaybeFail("same message indexes", Expect(msgIndexes, []int{1, 2, 3}))

	output, err := schemaID.GUIDToBytes()
	MaybeFail("same bytes", err, Expect(output, input))
}

func TestIdWithMessageIndexes(t *testing.T) {
	MaybeFail = InitFailFunc(t)

	schemaID := SchemaID{
		SchemaType: "PROTOBUF",
	}
	input := []byte{
		0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x02, 0x04, 0x06,
	}
	schemaID.FromBytes(input)
	id := schemaID.ID
	MaybeFail("same ids", Expect(id, 1))

	msgIndexes := schemaID.MessageIndexes
	MaybeFail("same message indexes", Expect(msgIndexes, []int{1, 2, 3}))

	output, err := schemaID.IDToBytes()
	MaybeFail("same bytes", err, Expect(output, input))
}

func TestFromBytesTruncatedPrefix(t *testing.T) {
	// A truncated wire format prefix must return an error rather than panic:
	// FromBytes used to slice the schema id or guid before checking that those
	// bytes were present, so a record value holding only a magic byte took down
	// the caller instead of failing deserialization.
	guid := []byte{
		0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
		0xa8, 0x02, 0xe2,
	}
	tests := []struct {
		name    string
		payload []byte
	}{
		{"empty payload", []byte{}},
		{"magic byte v0 only", []byte{MagicByteV0}},
		{"partial id", []byte{MagicByteV0, 0x00, 0x00}},
		{"one byte short of an id", []byte{MagicByteV0, 0x00, 0x00, 0x00}},
		{"magic byte v1 only", []byte{MagicByteV1}},
		{"partial guid", append([]byte{MagicByteV1}, guid[:8]...)},
		{"one byte short of a guid", append([]byte{MagicByteV1}, guid[:15]...)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("FromBytes panicked on %v: %v", tc.payload, r)
				}
			}()

			schemaID := SchemaID{SchemaType: "AVRO"}
			if _, err := schemaID.FromBytes(tc.payload); err == nil {
				t.Errorf("expected an error for %v, got nil", tc.payload)
			}
		})
	}
}

func TestGetSchemaTruncatedPrefix(t *testing.T) {
	// The deprecated GetSchema path carries its own copy of the prefix parsing
	// and needs the same guard; the legacy avro deserializer reaches it with an
	// ordinary zero length record value.
	tests := []struct {
		name    string
		payload []byte
	}{
		{"empty payload", []byte{}},
		{"magic byte only", []byte{MagicByteV0}},
		{"one byte short of an id", []byte{MagicByteV0, 0x00, 0x00, 0x00}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("GetSchema panicked on %v: %v", tc.payload, r)
				}
			}()

			deser := BaseDeserializer{}
			if _, err := deser.GetSchema("topic1", tc.payload); err == nil {
				t.Errorf("expected an error for %v, got nil", tc.payload)
			}
		})
	}
}

func TestReadMessageIndexes(t *testing.T) {
	MaybeFail = InitFailFunc(t)

	// count=0 shorthand: 1 byte consumed, indexes=[0]
	bytesRead, indexes, err := readMessageIndexes([]byte{0x00, 0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f})
	MaybeFail("zero count", err, Expect(indexes, []int{0}), Expect(bytesRead, 1))

	// 0x09 = raw 9, zigzag = -5 (e.g. field 1, wire type 1 in protobuf): negative count
	// should return an error indicating message indexes are absent or malformed
	_, _, err = readMessageIndexes([]byte{0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	if err == nil || err.Error() != "message indexes are absent or malformed" {
		t.Errorf("negative count: expected error 'message indexes are absent or malformed', got %v", err)
	}

	// 0x0a = count 5 (field 1, wire type 2 in protobuf), 0x05 = raw 5, zigzag = -3:
	// positive count but first index is negative — should return an error
	_, _, err = readMessageIndexes([]byte{0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f})
	if err == nil || err.Error() != "message indexes are absent or malformed" {
		t.Errorf("negative first index: expected error 'message indexes are absent or malformed', got %v", err)
	}

	// valid positive indexes: 0x06 = count 3, then indexes 1, 2, 3 (zigzag: 0x02 0x04 0x06)
	bytesRead, indexes, err = readMessageIndexes([]byte{0x06, 0x02, 0x04, 0x06})
	MaybeFail("valid indexes", err, Expect(indexes, []int{1, 2, 3}), Expect(bytesRead, 4))

	// empty payload — should return an error indicating message indexes are absent or malformed
	_, _, err = readMessageIndexes([]byte{})
	if err == nil || err.Error() != "message indexes are absent or malformed" {
		t.Errorf("empty payload: expected error 'message indexes are absent or malformed', got %v", err)
	}
}
