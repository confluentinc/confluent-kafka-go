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
