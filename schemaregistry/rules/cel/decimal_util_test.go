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

package cel

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/apd/v3"
)

func mustDecimal(t *testing.T, s string) *apd.Decimal {
	t.Helper()
	d, _, err := apd.NewFromString(s)
	if err != nil {
		t.Fatalf("apd.NewFromString(%q): %v", s, err)
	}
	return d
}

func TestDecimalProtoRoundTrip(t *testing.T) {
	for _, s := range []string{"12.34", "0", "-7.5", "100", "1.50", "0.001", "-0.0000000001"} {
		d := mustDecimal(t, s)
		proto, err := decimalToProto(d)
		if err != nil {
			t.Fatalf("decimalToProto(%s): %v", s, err)
		}
		back, err := decimalFromProto(proto)
		if err != nil {
			t.Fatalf("decimalFromProto for %s: %v", s, err)
		}
		if back.Cmp(d) != 0 {
			t.Errorf("round-trip mismatch for %s: got %s", s, back.Text('f'))
		}
		// Scale is preserved through the proto (1.50 stays 1.50, not 1.5).
		if -proto.Scale != d.Exponent {
			t.Errorf("scale mismatch for %s: proto scale %d, apd exponent %d", s, proto.Scale, d.Exponent)
		}
	}
}

func TestDecimalProtoKnownWireForm(t *testing.T) {
	// 12.34 = unscaled 1234 (0x04D2) at scale 2.
	proto, err := decimalToProto(mustDecimal(t, "12.34"))
	if err != nil {
		t.Fatalf("decimalToProto: %v", err)
	}
	if !bytes.Equal(proto.Value, []byte{0x04, 0xd2}) || proto.Scale != 2 {
		t.Errorf("expected value=[04 d2] scale=2, got value=% x scale=%d", proto.Value, proto.Scale)
	}
}

// TestDecimalToProtoSetsPrecision covers confluent.type.Decimal's precision field, which is the
// unscaled value's digit count -- what BigDecimal.precision() reports and what the JVM's
// ProtobufResultWriter writes (m.put("precision", dec.precision())). This writer left it unset,
// so the same computed decimal serialized differently here than on the JVM, and differently
// from this client's own protobuf serde path, which has always set it.
func TestDecimalToProtoSetsPrecision(t *testing.T) {
	cases := []struct {
		in        string
		precision uint32
		scale     int32
	}{
		{"12.34", 4, 2},
		{"12.3400", 6, 4}, // trailing zeros are digits, so precision is 6 not 4
		{"1E+3", 1, -3},   // unscaled 1 at a negative scale
		{"0.00", 1, 2},    // zero has precision 1, as BigDecimal reports it
		{"100", 3, 0},
		{"-12.34", 4, 2}, // the sign is not a digit
	}
	for _, tc := range cases {
		d, _, err := apd.NewFromString(tc.in)
		if err != nil {
			t.Fatalf("parse %s: %v", tc.in, err)
		}
		got, err := decimalToProto(d)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if got.Precision != tc.precision || got.Scale != tc.scale {
			t.Errorf("decimalToProto(%s) = (precision %d, scale %d), want (%d, %d)",
				tc.in, got.Precision, got.Scale, tc.precision, tc.scale)
		}
	}
}
