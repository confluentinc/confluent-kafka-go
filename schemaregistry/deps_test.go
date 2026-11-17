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

package schemaregistry

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNoKafkaDependency verifies that the Schema Registry client does not
// depend on the kafka package, which requires CGo, so that it can be built
// with CGO_ENABLED=0.
func TestNoKafkaDependency(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go command not found")
	}

	out, err := exec.Command(goBin, "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps failed: %v\n%s", err, out)
	}

	const kafkaPkg = "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	for dep := range strings.FieldsSeq(string(out)) {
		if dep == kafkaPkg {
			t.Fatalf("schemaregistry must not depend on %s, as it requires CGo", kafkaPkg)
		}
	}
}
