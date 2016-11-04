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

//Test TimestampType
func TestTimestampType(t *testing.T) {
	timestampMap := map[TimestampType]string{TimestampCreateTime: "CreateTime",
		TimestampLogAppendTime: "LogAppendTime",
		TimestampNotAvailable:  "NotAvailable"}
	for ts, desc := range timestampMap {
		if ts.String() != desc {
			t.Errorf("Wrong timestamp description for %s, expected %s\n", desc, ts.String())
		}
	}
}
