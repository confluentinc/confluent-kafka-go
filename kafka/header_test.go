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

// TestHeader tests the Header type
func TestHeader(t *testing.T) {

	hdr := Header{"MyHdr1", []byte("a string")}
	if hdr.String() != "MyHdr1=\"a string\"" {
		t.Errorf("Unexpected: %s", hdr.String())
	}

	hdr = Header{"MyHdr2", []byte("a longer string that will be truncated right here <-- so you wont see this part.")}
	if hdr.String() != "MyHdr2=\"a longer string that will be truncated right here \"(30 more bytes)" {
		t.Errorf("Unexpected: %s", hdr.String())
	}

	hdr = Header{"MyHdr3", []byte{1, 2, 3, 4}}
	if hdr.String() != "MyHdr3=\"\\x01\\x02\\x03\\x04\"" {
		t.Errorf("Unexpected: %s", hdr.String())
	}

}
