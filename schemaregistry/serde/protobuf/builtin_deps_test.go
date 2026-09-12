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

// TestBuiltinConfluentTypeImports covers the confluent value types as built-in imports. The
// canonical path is confluent/type/... - what the Java client registers and what ProtobufSchema
// declares - and the generated descriptors used to be named confluent/types/... after the
// directory this client needs because `type` is a Go keyword, so only the plural spelling
// resolved and a Java-registered schema failed. The descriptors are canonical now, and
// decimal's old path is served by a public-import stub so schemas registered either way keep
// loading. Variant has no such stub: it had not shipped under the old path, so nothing can be
// importing it - see TestUnknownDependencyNamesItself.
func TestBuiltinConfluentTypeImports(t *testing.T) {
	tmpl := `syntax = "proto3";
package test;
import "%s";
message M { confluent.type.%s f = 1; }
`
	cases := []struct{ path, message, want string }{
		{"confluent/type/decimal.proto", "Decimal", "confluent.type.Decimal"},
		{"confluent/type/variant.proto", "Variant", "confluent.type.Variant"},
		// Decimal's old path, served by the stub. It declares nothing and publicly imports the
		// canonical file, so the symbol resolves through it to the same confluent.type.Decimal.
		{"confluent/types/decimal.proto", "Decimal", "confluent.type.Decimal"},
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
		// Variant never shipped under the old path, so it deliberately has no stub. Pinned
		// here so adding one is a deliberate act rather than a copy of decimal's row.
		{"confluent/types/variant.proto", "Variant"},
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
