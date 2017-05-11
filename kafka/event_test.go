/**
 * Copyright 2016 Confluent Inc.
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

package kafka

import (
	"testing"
)

// TestEventAPIs dry-tests the public event related APIs, no broker is needed.
func TestEventAPIs(t *testing.T) {
	assignedPartitions := AssignedPartitions{}
	t.Logf("%s\n", assignedPartitions.String())

	revokedPartitions := RevokedPartitions{}
	t.Logf("%s\n", revokedPartitions.String())

	topic := "test"
	partition := PartitionEOF{Topic: &topic}
	t.Logf("%s\n", partition.String())

	partition = PartitionEOF{}
	t.Logf("%s\n", partition.String())

	committedOffsets := OffsetsCommitted{}
	t.Logf("%s\n", committedOffsets.String())

	stats := Stats{"{\"name\": \"Producer-1\"}"}
	t.Logf("Stats: %s\n", stats.String())
}
