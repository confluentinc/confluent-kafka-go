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
	"math/big"
	"testing"
	"time"

	prototypes "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// evalBool runs a boolean CEL rule against value and fails the test on error.
func evalBool(t *testing.T, expr string, value interface{}) bool {
	t.Helper()
	result, err := NewValidator().Execute(rule(expr), nil, value)
	if err != nil {
		t.Fatalf("expr %q: unexpected error: %v", expr, err)
	}
	b, ok := result.(bool)
	if !ok {
		t.Fatalf("expr %q: expected bool result, got %T (%v)", expr, result, result)
	}
	return b
}

func TestDecimalOperators(t *testing.T) {
	cases := []struct {
		expr     string
		expected bool
	}{
		{`decimals.gt(decimal("12.34"), decimal("10.00"))`, true},
		{`decimals.lt(decimal("12.34"), decimal("10.00"))`, false},
		{`decimals.eq(decimal("1.50"), decimal("1.5"))`, true},
		{`decimals.eq(decimals.add(decimal("12.34"), decimal("1.66")), decimal("14.00"))`, true},
		{`decimals.eq(decimals.sub(decimal("5"), decimal("3")), decimal("2"))`, true},
		{`decimals.eq(decimals.mul(decimal("1.5"), decimal("2")), decimal("3.0"))`, true},
		{`decimals.eq(decimals.mod(decimal("10"), decimal("3")), decimal("1"))`, true},
		{`decimals.eq(decimals.sqrt(decimal("144")), decimal("12"))`, true},
		{`decimals.eq(decimals.greatest(decimal("2.5"), decimal("9.99")), decimal("9.99"))`, true},
		{`decimals.eq(decimals.least(decimal("2.5"), decimal("9.99")), decimal("2.5"))`, true},
		{`decimals.eq(decimals.neg(decimal("2.5")), decimal("-2.5"))`, true},
		{`decimals.eq(decimals.abs(decimal("-2.5")), decimal("2.5"))`, true},
		{`decimals.sign(decimal("-2.5")) == -1`, true},
		{`double(decimal("100.50")) == 100.5`, true},
	}
	for _, c := range cases {
		if got := evalBool(t, c.expr, 1); got != c.expected {
			t.Errorf("expr %q: expected %v, got %v", c.expr, c.expected, got)
		}
	}
}

func TestDecimalStringForms(t *testing.T) {
	cases := []struct {
		expr     string
		expected string
	}{
		// Division: exact terminates, non-terminating rounds to 38 significant digits HALF_UP.
		{`string(decimals.div(decimal("1"), decimal("8")))`, "0.125"},
		{`string(decimals.div(decimal("12"), decimal("1")))`, "12"},
		{`string(decimals.div(decimal("1"), decimal("99")))`, "0.010101010101010101010101010101010101010"},
		{`string(decimals.div(decimal("2"), decimal("3")))`, "0.66666666666666666666666666666666666667"},
		{`string(decimals.sqrt(decimal("2")))`, "1.4142135623730950488016887242096980786"},
		// Exact div/sqrt adopt Java BigDecimal's preferred scale rather than over-stripping
		// trailing zeros: 6.0/3 -> "2.0" (not "2"), keeping parity with the other clients.
		{`string(decimals.div(decimal("6.0"), decimal("3")))`, "2.0"},
		{`string(decimals.div(decimal("10.00"), decimal("2")))`, "5.00"},
		{`string(decimals.div(decimal("2000"), decimal("10")))`, "200"},
		{`string(decimals.div(decimal("-6.0"), decimal("3")))`, "-2.0"},
		{`string(decimals.sqrt(decimal("4.00")))`, "2.0"},
		{`string(decimals.sqrt(decimal("100.0000")))`, "10.00"},
		{`string(decimals.sqrt(decimal("144")))`, "12"},
		// Rounding family (Flink-aligned).
		{`string(decimals.round(decimal("2.567"), 2))`, "2.57"},
		{`string(decimals.trunc(decimal("2.567"), 2))`, "2.56"},
		{`string(decimals.floor(decimal("2.9")))`, "2"},
		{`string(decimals.ceil(decimal("2.1")))`, "3"},
		// string(Decimal) is plain (never scientific), scale preserved from the literal.
		{`string(decimal("1.50"))`, "1.50"},
		// add/sub/mul are exact and keep their natural scales (unaffected by the div/sqrt
		// preferred-scale fix): add/sub -> max input scale, mul -> sum of input scales.
		{`string(decimals.add(decimal("1.50"), decimal("2.50")))`, "4.00"},
		{`string(decimals.sub(decimal("5.00"), decimal("3.0")))`, "2.00"},
		{`string(decimals.mul(decimal("1.5"), decimal("2.0")))`, "3.00"},
	}
	for _, c := range cases {
		expr := c.expr + ` == "` + c.expected + `"`
		if !evalBool(t, expr, 1) {
			// Re-run to surface the actual string in the failure message.
			got, _ := NewValidator().Execute(rule(c.expr), nil, 1)
			t.Errorf("expr %q: expected %q, got %q", c.expr, c.expected, got)
		}
	}
}

// TestDecimalScaleOutOfInt32Range verifies that a scale argument outside the int32 range is
// rejected with a CEL error rather than silently narrowed to the low 32 bits, matching
// Java's requireIntScale (Math.toIntExact). Covers decimals.round/trunc and decimal(bytes,
// scale). 3000000000 > math.MaxInt32; naive int32() truncation would wrap it to a small,
// wrong scale.
func TestDecimalScaleOutOfInt32Range(t *testing.T) {
	exprs := []string{
		`decimals.round(decimal("1.5"), 3000000000)`,
		`decimals.trunc(decimal("1.5"), 3000000000)`,
		`decimals.round(decimal("1.5"), -3000000000)`,
		`decimal(b"\x04\xd2", 3000000000)`,
	}
	for _, e := range exprs {
		if _, err := NewValidator().Execute(rule(e), nil, 1); err == nil {
			t.Errorf("expr %q: expected an error for an out-of-int32 scale, got nil", e)
		}
	}
}

func TestDecimalFromBytesAndScale(t *testing.T) {
	// 12.34 = unscaled 1234 (0x04D2) at scale 2.
	if !evalBool(t, `decimals.eq(decimal(b"\x04\xd2", 2), decimal("12.34"))`, 1) {
		t.Errorf("decimal(bytes, scale) did not equal decimal(\"12.34\")")
	}
}

// ---- Marshalling: the four schema-side shapes into CEL ----

func TestProtoConfluentTypeDecimalIntoCel(t *testing.T) {
	// A confluent.type.Decimal message: 12.34 = unscaled 1234 (0x04D2) at scale 2.
	dec := &prototypes.Decimal{Value: []byte{0x04, 0xd2}, Scale: 2}
	if !evalBool(t, `decimals.gt(decimal(this), decimal("10.00"))`, dec) {
		t.Errorf("proto confluent.type.Decimal did not marshal into CEL correctly")
	}
}

func TestProtoWktTimestampIntoCel(t *testing.T) {
	ts := timestamppb.New(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if !evalBool(t, `this < now`, ts) {
		t.Errorf("proto WKT Timestamp did not marshal into CEL as a timestamp")
	}
}

func TestAvroLogicalDecimalIntoCel(t *testing.T) {
	// Avro's decimal logical type decodes to *big.Rat (hamba). 12.34 = 1234/100.
	type decimalRecord struct {
		Amount *big.Rat `avro:"amount"`
	}
	rec := decimalRecord{Amount: new(big.Rat).SetFrac64(1234, 100)}
	if !evalBool(t, `decimals.gt(decimal(this.amount), decimal("10.00"))`, rec) {
		t.Errorf("avro logical decimal (*big.Rat) did not marshal into CEL correctly")
	}
}

func TestAvroLogicalTimestampIntoCel(t *testing.T) {
	// Avro's timestamp logical types decode to time.Time (hamba, UTC).
	type tsRecord struct {
		Ts time.Time `avro:"ts"`
	}
	rec := tsRecord{Ts: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}
	if !evalBool(t, `timestamp.of(this.ts) < now`, rec) {
		t.Errorf("avro logical timestamp (time.Time) did not marshal into CEL correctly")
	}
}
