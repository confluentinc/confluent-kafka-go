/**
 * Copyright 2026 Confluent Inc.
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

package protobuf

import (
	"fmt"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// TestBuiltinConfluentTypeImports covers the two confluent value types as built-in imports,
// in both spellings. The canonical path is confluent/type/... - what the Java client registers
// and what ProtobufSchema declares - and the generated descriptors used to be named
// confluent/types/... after the directory this client needs because `type` is a Go keyword, so
// only the plural spelling resolved and a Java-registered schema failed. The descriptors are
// canonical now and the plural name is kept as a read alias, so schemas registered either way
// keep loading.
func TestBuiltinConfluentTypeImports(t *testing.T) {
	tmpl := `syntax = "proto3";
package test;
import "%s";
message M { confluent.type.%s f = 1; }
`
	cases := []struct{ path, message, want string }{
		{"confluent/type/decimal.proto", "Decimal", "confluent.type.Decimal"},
		{"confluent/type/variant.proto", "Variant", "confluent.type.Variant"},
		// The plural spelling stays readable for schemas registered before the rename. Both
		// files declare `package confluent.type`, so either resolves to the same message.
		{"confluent/types/decimal.proto", "Decimal", "confluent.type.Decimal"},
		{"confluent/types/variant.proto", "Variant", "confluent.type.Variant"},
	}
	for _, tc := range cases {
		info := schemaregistry.SchemaInfo{Schema: fmt.Sprintf(tmpl, tc.path, tc.message)}
		fd, err := parseFileDesc(nil, info)
		if err != nil {
			t.Errorf("import %q: %v", tc.path, err)
			continue
		}
		got := fd.GetMessageTypes()[0].GetFields()[0].GetMessageType().GetFullyQualifiedName()
		if got != tc.want {
			t.Errorf("import %q: field type = %q, want %q", tc.path, got, tc.want)
		}
	}
}

// TestUnknownDependencyNamesItself pins the accessor change: an import nothing provides has to
// report itself. Before this, the accessor answered an unknown file with an empty one and the
// failure landed on whatever referred to it - "unknown type confluent.type.Nope", naming the
// field rather than the import that was missing.
func TestUnknownDependencyNamesItself(t *testing.T) {
	tmpl := `syntax = "proto3";
package test;
import "%s";
message M { confluent.type.%s f = 1; }
`
	for _, tc := range []struct{ path, message string }{
		{"confluent/type/nope.proto", "Nope"},
		{"confluent/types/nope.proto", "Nope"},
	} {
		info := schemaregistry.SchemaInfo{Schema: fmt.Sprintf(tmpl, tc.path, tc.message)}
		_, err := parseFileDesc(nil, info)
		if err == nil {
			t.Errorf("import %q resolved, want an error", tc.path)
			continue
		}
		if want := "dependency " + tc.path + " not found"; !strings.Contains(err.Error(), want) {
			t.Errorf("import %q: error = %q, want it to contain %q", tc.path, err, want)
		}
	}
}
