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
	"math"
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
// TestDecimalLargeMagnitudeRounding is a regression test for the rounding family and mod
// silently returning NaN for values whose digit count exceeds 38. Java uses
// BigDecimal.setScale / BigDecimal.remainder (both exact, unlimited precision), so the
// results must be exact regardless of magnitude. Previously these routed through a
// precision-38 apd context, so apd's Quantize/Rem set the result to NaN (and, with
// Traps==0, returned no error), and string(...) yielded "NaN".
func TestDecimalLargeMagnitudeRounding(t *testing.T) {
	// A 39-digit integer — one more digit than the old precision-38 cap.
	const big39 = "123456789012345678901234567890123456789"
	cases := []struct {
		expr     string
		expected string
	}{
		{`string(decimals.floor(decimal("` + big39 + `")))`, big39},
		{`string(decimals.ceil(decimal("` + big39 + `")))`, big39},
		{`string(decimals.round(decimal("` + big39 + `")))`, big39},
		// A 39-digit integer part plus a fractional half exercises the quantize path
		// (rather than the trunc no-op) at >38 digits.
		{`string(decimals.trunc(decimal("` + big39 + `.5")))`, big39},
		// HALF_UP on the .5 rounds the 39-digit integer up by one.
		{`string(decimals.round(decimal("` + big39 + `.5")))`, "123456789012345678901234567890123456790"},
		// mod with a 40-digit dividend: 10^40 mod 3 == 1 (10 ≡ 1 mod 3).
		{`string(decimals.mod(decimal("1e40"), decimal("3")))`, "1"},
	}
	for _, c := range cases {
		expr := c.expr + ` == "` + c.expected + `"`
		if !evalBool(t, expr, 1) {
			got, _ := NewValidator().Execute(rule(c.expr), nil, 1)
			t.Errorf("expr %q: expected %q, got %q", c.expr, c.expected, got)
		}
	}
}

// TestDecimalRejectsNonFinite is a regression test for decimal() accepting NaN/Infinity
// special values that Java rejects. Java's new BigDecimal(String) and
// BigDecimal.valueOf(double) both throw NumberFormatException on non-finite inputs; apd's
// NewFromString instead parses "NaN"/"Infinity"/"inf"/"sNaN" into special-value decimals
// with no error. decimal(...) must surface a CEL error for these.
func TestDecimalRejectsNonFinite(t *testing.T) {
	// String arm: these all parse into special-value apd decimals, so they must error.
	for _, e := range []string{
		`decimal("NaN")`,
		`decimal("Infinity")`,
		`decimal("-inf")`,
		`decimal("sNaN")`,
	} {
		if _, err := NewValidator().Execute(rule(e), nil, 1); err == nil {
			t.Errorf("expr %q: expected an error for a non-finite decimal, got nil", e)
		}
	}
	// Double arm: a NaN/Inf float64 flowing through decimal(this) must error too.
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := NewValidator().Execute(rule(`decimal(this)`), nil, v); err == nil {
			t.Errorf("decimal(this) with %v: expected an error for a non-finite double, got nil", v)
		}
	}
	// Finite values (including scientific notation and finite doubles) must still
	// succeed and round-trip.
	finite := []struct {
		expr     string
		expected string
	}{
		{`string(decimal("1e40"))`, "10000000000000000000000000000000000000000"},
		{`string(decimal("123.45"))`, "123.45"},
		{`string(decimal(1.5))`, "1.5"},
	}
	for _, c := range finite {
		expr := c.expr + ` == "` + c.expected + `"`
		if !evalBool(t, expr, 1) {
			got, err := NewValidator().Execute(rule(c.expr), nil, 1)
			t.Errorf("expr %q: expected %q, got %q (err=%v)", c.expr, c.expected, got, err)
		}
	}
	// "-0" is finite and must not be rejected (apd keeps negative zero as "-0").
	if _, err := NewValidator().Execute(rule(`string(decimal("-0"))`), nil, 1); err != nil {
		t.Errorf(`decimal("-0"): expected no error for finite negative zero, got %v`, err)
	}
}

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
