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
	"fmt"
	"math"
	"strings"
	"testing"

	typepb "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/confluent/type"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/variant"
)

// A JSON document exercising objects, arrays, an explicit null, and nesting.
const variantDoc = `{"name":"alice","age":30,"explicit":null,"nested":{"x":1},"scores":[10,20,30]}`

// buildVariantMsg builds a standalone Variant with append and returns it in the
// map[string]interface{} shape (metadata/value byte entries) that variant(this) accepts.
func buildVariantMsg(t *testing.T, append func(*variant.Builder) error) map[string]interface{} {
	t.Helper()
	vb := variant.NewBuilder()
	if err := append(vb); err != nil {
		t.Fatalf("append: %v", err)
	}
	v, err := vb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return map[string]interface{}{"value": v.StandaloneValueBytes(), "metadata": v.MetadataBytes()}
}

// TestVariantAsTimestampNanos verifies variants.as('timestamp') on NANOS variants
// (nanoseconds since epoch): sub-microsecond precision is preserved and negative epoch
// values floor toward negative infinity, matching Java's variantGetTimestamp ->
// fromEpochNanos (Math.floorDiv/floorMod). Go's time.Time holds nanoseconds, so parity is
// exact. The micros case guards the unchanged TIMESTAMP_TZ path.
func TestVariantAsTimestampNanos(t *testing.T) {
	cases := []struct {
		name     string
		build    func(*variant.Builder) error
		expected string // RFC 3339 literal used as the CEL oracle
	}{
		// Positive, sub-microsecond: the trailing 123 ns must survive (old code truncated
		// to micros and lost it).
		{"nanos_tz_positive_submicro",
			func(vb *variant.Builder) error { return vb.AppendTimestampNanosTz(1_000_000_123) },
			"1970-01-01T00:00:01.000000123Z"},
		// Negative single nanosecond: floor gives 999999999 ns before epoch. Old code did
		// raw/1000 == 0 (trunc toward zero) and produced the epoch instead.
		{"nanos_ntz_negative_one",
			func(vb *variant.Builder) error { return vb.AppendTimestampNanosNtz(-1) },
			"1969-12-31T23:59:59.999999999Z"},
		// Negative, not a whole microsecond: old raw/1000 truncated toward zero (-1) instead
		// of flooring, and dropped the extra nanosecond.
		{"nanos_tz_negative_nonwhole_micro",
			func(vb *variant.Builder) error { return vb.AppendTimestampNanosTz(-1001) },
			"1969-12-31T23:59:59.999998999Z"},
		// Regression: the micros (non-NANOS) path is unchanged.
		{"micros_tz_regression",
			func(vb *variant.Builder) error { return vb.AppendTimestampTz(1_000_000) },
			"1970-01-01T00:00:01Z"},
	}
	for _, tc := range cases {
		msg := buildVariantMsg(t, tc.build)
		expr := `variants.as(variant(this), 'timestamp') == timestamp("` + tc.expected + `")`
		if !evalBool(t, expr, msg) {
			got, _ := NewValidator().Execute(rule(`variants.as(variant(this), 'timestamp')`), nil, msg)
			t.Errorf("%s: expected %s, got %v", tc.name, tc.expected, got)
		}
	}
}

func TestVariantFunctions(t *testing.T) {
	cases := []struct {
		expr     string
		expected bool
	}{
		{"variants.type(variants.parseJson(this)) == 'object'", true},
		{"variants.as(variants.field(variants.parseJson(this), 'name'), 'string') == 'alice'", true},
		{"variants.as(variants.field(variants.parseJson(this), 'age'), 'int') == 30", true},
		// A missing field is CEL null (absent); an explicit JSON null is a present variant-null.
		{"variants.field(variants.parseJson(this), 'missing') == null", true},
		{"variants.isNull(variants.field(variants.parseJson(this), 'explicit'))", true},
		{"!variants.isNull(variants.field(variants.parseJson(this), 'missing'))", true},
		{"variants.as(variants.path(variants.parseJson(this), '$.nested.x'), 'int') == 1", true},
		{"variants.as(variants.index(variants.field(variants.parseJson(this), 'scores'), 2), 'int') == 30", true},
		// tryAs returns CEL null on a type mismatch (age is an int, not a string).
		{"variants.tryAs(variants.field(variants.parseJson(this), 'age'), 'string') == null", true},
		{`variants.toJson(variants.field(variants.parseJson(this), 'nested')) == '{"x":1}'`, true},
		// variant(null) passes CEL null through (aligns with the Java reference), both
		// directly and composed with a navigation accessor over an absent field.
		{"variant(null) == null", true},
		{"variants.field(variant(variants.field(variants.parseJson(this), 'missing')), 'k') == null", true},
	}
	for _, tc := range cases {
		if got := evalBool(t, tc.expr, variantDoc); got != tc.expected {
			t.Errorf("expr %q = %v, want %v", tc.expr, got, tc.expected)
		}
	}
}

// TestVariantEqualityIsOverTheEncoding pins variant `==` as equality of the encoding. It used
// to be buffer identity - sameBytes compared &a[0], not the contents - so two separately built
// variants over the same document came back unequal. Sound but incomplete: equal bytes mean
// equal values, but one value has many encodings.
func TestVariantEqualityIsOverTheEncoding(t *testing.T) {
	cases := []struct {
		expr     string
		expected bool
	}{
		{`variants.parseJson("1") == variants.parseJson("1")`, true},
		{`variants.parseJson("1") != variants.parseJson("1")`, false},
		{`variants.parseJson("{}") == variants.parseJson("{}")`, true},
		{`variants.parseJson('{"a":1}') == variants.parseJson('{"a":1}')`, true},
		{`variants.parseJson("1") == variants.parseJson("2")`, false},
		// Incomplete, as documented: an int and a double are two encodings.
		{`variants.parseJson("1") == variants.parseJson("1.0")`, false},
		// Containers recurse with the same equality.
		{`[variants.parseJson("1")] == [variants.parseJson("1")]`, true},
		{`[variants.parseJson("1")] == [variants.parseJson("2")]`, false},
		// Navigation: the same position in an identical parent.
		{`variants.field(variants.parseJson('{"a":1}'), "a") == ` +
			`variants.field(variants.parseJson('{"a":1}'), "a")`, true},
		// A field holding 1 is not the standalone variant 1: it carries its parent's
		// metadata dictionary, which is part of the comparison.
		{`variants.field(variants.parseJson('{"a":1}'), "a") == variants.parseJson("1")`, false},
		// And the receiver of the whole document, reached two ways, is the same variant.
		{`variants.parseJson(this) == variants.parseJson(this)`, true},
	}
	for _, tc := range cases {
		if got := evalBool(t, tc.expr, variantDoc); got != tc.expected {
			t.Errorf("expr %q = %v, want %v", tc.expr, got, tc.expected)
		}
	}
}

// TestVariantAsTimestampIsRangeChecked pins the CEL timestamp range on variant extraction.
// A variant timestamp spans the whole int64 range while a CEL timestamp is 0001-9999, so an
// out-of-range value is reachable from data; it used to be built anyway, leaving an invalid
// instant that could not be rendered but could still be compared - `< now` answered a
// confident false for a value that is not a time. Refused now, and routed through the
// as/tryAs split so a rule can guard, matching the reference's variantGetTimestamp.
func TestVariantAsTimestampIsRangeChecked(t *testing.T) {
	const maxMicros = int64(253402300799)*1_000_000 + 999_999
	const minMicros = int64(-62135596800) * 1_000_000

	// The boundaries are inside the range, and tryAs answers a timestamp there.
	for _, micros := range []int64{0, maxMicros, minMicros} {
		msg := buildVariantMsg(t, func(b *variant.Builder) error {
			return b.AppendTimestampTz(micros)
		})
		expr := `variants.as(this, "timestamp") == variants.as(this, "timestamp")`
		if got := evalBool(t, expr, msg); !got {
			t.Errorf("micros %d: %s = false, want true", micros, expr)
		}
		if got := evalBool(t, `variants.tryAs(this, "timestamp") == null`, msg); got {
			t.Errorf("micros %d: tryAs answered null for an in-range value", micros)
		}
	}

	// Out of range: as errors and names the range, tryAs answers CEL null.
	for _, micros := range []int64{math.MaxInt64, math.MinInt64, maxMicros + 1_000_000} {
		msg := buildVariantMsg(t, func(b *variant.Builder) error {
			return b.AppendTimestampTz(micros)
		})
		_, err := evalRaw(t, `variants.as(this, "timestamp") != null`, msg)
		if err == nil {
			t.Errorf("micros %d: variants.as should have failed", micros)
		} else if !strings.Contains(err.Error(), "is outside 0001-01-01T00:00:00Z") {
			t.Errorf("micros %d: error should name the range, got %v", micros, err)
		}
		if got := evalBool(t, `variants.tryAs(this, "timestamp") == null`, msg); !got {
			t.Errorf("micros %d: tryAs should answer null", micros)
		}
	}
}

// TestVariantPathNonASCIIIdentifier covers an unquoted path identifier containing a
// multi-byte UTF-8 character. The parser used to test `rune(path[pos])` - a single byte -
// and a UTF-8 lead byte casts into Latin-1 (0xC3 -> 'Ã'), which unicode.IsLetter accepts;
// the identifier then ended at the continuation byte, so `$.é` failed with
// "unexpected character '©'", naming a character absent from the input. Java accepts
// these (it iterates UTF-16 chars, and Character.isLetter('é') is true), as do every
// other client.
//
// The quoted-form case is the control: it always worked, which is what localises a
// regression to the identifier scanner rather than to the walk.
func TestVariantPathNonASCIIIdentifier(t *testing.T) {
	const doc = `{"é":1,"中":2,"éx":4,"plain":3}`
	cases := []struct {
		expr     string
		expected bool
	}{
		{"variants.as(variants.path(variants.parseJson(this), '$.é'), 'int') == 1", true},
		{"variants.as(variants.path(variants.parseJson(this), '$.中'), 'int') == 2", true},
		// A multi-byte character followed by ASCII: proves the scanner advances by the
		// rune's width rather than by one byte.
		{"variants.as(variants.path(variants.parseJson(this), '$.éx'), 'int') == 4", true},
		// Control: the quoted form was never affected.
		{`variants.as(variants.path(variants.parseJson(this), '$["é"]'), 'int') == 1`, true},
		{"variants.as(variants.path(variants.parseJson(this), '$.plain'), 'int') == 3", true},
	}
	for _, tc := range cases {
		if got := evalBool(t, tc.expr, doc); got != tc.expected {
			t.Errorf("expr %q = %v, want %v", tc.expr, got, tc.expected)
		}
	}
}

// TestVariantTryParseJsonSoftFailure verifies that empty or whitespace-only input
// to variants.tryParseJson yields CEL null (a soft failure) rather than crashing
// or leaking an error.
func TestVariantTryParseJsonSoftFailure(t *testing.T) {
	exprs := []string{
		"variants.tryParseJson('') == null",
		"variants.tryParseJson('   ') == null",
		"variants.tryParseJson('\\t\\n') == null",
	}
	for _, expr := range exprs {
		if !evalBool(t, expr, variantDoc) {
			t.Errorf("expr %q did not evaluate to true (expected CEL null)", expr)
		}
	}
}

// TestVariantNonFiniteToJsonThroughCel verifies the non-finite bareword contract
// end-to-end through the CEL layer: parseJson of an overflow literal renders back
// as the Infinity bareword via variants.toJson.
func TestVariantNonFiniteToJsonThroughCel(t *testing.T) {
	cases := []string{
		"variants.toJson(variants.parseJson('1e400')) == 'Infinity'",
		"variants.toJson(variants.parseJson('-1e400')) == '-Infinity'",
		// The bare tokens, which Go's encoding/json rejects outright and ParseJSON rewrites.
		// Java (Jackson), Python, C# and Rust all accept these, so Go must too.
		"variants.type(variants.parseJson('NaN')) == 'double'",
		"variants.type(variants.parseJson('Infinity')) == 'double'",
		"variants.type(variants.parseJson('-Infinity')) == 'double'",
		"variants.toJson(variants.parseJson('NaN')) == 'NaN'",
		"variants.toJson(variants.parseJson('Infinity')) == 'Infinity'",
		"variants.toJson(variants.parseJson('-Infinity')) == '-Infinity'",
		`variants.toJson(variants.parseJson('{"a":NaN}')) == '{"a":NaN}'`,
		`variants.toJson(variants.parseJson('[NaN,Infinity,-Infinity]')) == '[NaN,Infinity,-Infinity]'`,
		`variants.type(variants.field(variants.parseJson('{"a":NaN}'), 'a')) == 'double'`,
		// A bareword is a successful parse, not a soft failure.
		"variants.tryParseJson('NaN') != null",
		// Spelling and case are exact, matching Jackson, so these stay soft failures.
		"variants.tryParseJson('nan') == null",
		"variants.tryParseJson('INFINITY') == null",
	}
	for _, expr := range cases {
		if !evalBool(t, expr, variantDoc) {
			t.Errorf("expr %q did not evaluate to true", expr)
		}
	}
}

// A confluent.type.Variant proto message bound as `this`; variant(dyn) unwraps it.
func TestProtoConfluentTypeVariantIntoCel(t *testing.T) {
	pv, err := variant.ParseJSON(variantDoc)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	msg := &typepb.Variant{Metadata: pv.MetadataBytes(), Value: pv.ValueBytes()}
	if !evalBool(t, "variants.as(variants.field(variant(this), 'age'), 'int') == 30", msg) {
		t.Errorf("proto confluent.type.Variant did not marshal into CEL correctly")
	}
}

// TestVariantIsNullCoercesBareReceiver pins that variants.isNull coerces its receiver like every
// other accessor. It is declared over dyn, so a bare variant field reaches it. A bare *object*
// cannot catch a missing coercion - isNull on an object is false either way - so only a variant
// that is itself null discriminates.
func TestVariantIsNullCoercesBareReceiver(t *testing.T) {
	for _, tc := range []struct {
		json     string
		expected bool
	}{{"null", true}, {"5", false}} {
		pv, err := variant.ParseJSON(tc.json)
		if err != nil {
			t.Fatalf("ParseJSON(%s): %v", tc.json, err)
		}
		msg := &typepb.Variant{Metadata: pv.MetadataBytes(), Value: pv.ValueBytes()}
		for _, expr := range []string{
			"variants.isNull(this)",
			// The wrapped form has always worked and must keep working.
			"variants.isNull(variant(this))",
		} {
			if got := evalBool(t, expr, msg); got != tc.expected {
				t.Errorf("%s on %s: expected %v, got %v", expr, tc.json, tc.expected, got)
			}
		}
	}
}

// TestVariantNeedsNoConstructor is the cross-client parity test: a variant value is usable with
// the variants.* accessors with **no variant(...) call**, in both formats, and the wrapped form
// keeps working alongside it. The accessors are declared over cel.DynType and coerce inside, so
// they take whatever the decoder produced — a proto confluent.type.Variant message, or the map
// an Avro variant record decodes to.
func TestVariantNeedsNoConstructor(t *testing.T) {
	pv, err := variant.ParseJSON(`{"name":"alice","age":30}`)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	type holder struct {
		Data map[string]interface{} `avro:"data"`
	}
	subjects := map[string]interface{}{
		// Protobuf: a confluent.type.Variant message bound as `this`.
		"proto": &typepb.Variant{Metadata: pv.MetadataBytes(), Value: pv.ValueBytes()},
		// Avro: a variant record, which decodes generically to a metadata/value map.
		"avro": holder{Data: map[string]interface{}{
			"metadata": pv.MetadataBytes(),
			"value":    pv.ValueBytes(),
		}},
	}
	for kind, subject := range subjects {
		// `this` for the proto message, `this.data` for the Avro holder's field.
		self := "this"
		if kind == "avro" {
			self = "this.data"
		}
		for _, tmpl := range []string{
			// Bare: no constructor call.
			"variants.type(%s) == 'object'",
			"variants.as(variants.field(%s, 'name'), 'string') == 'alice'",
			"variants.as(variants.path(%s, '$.age'), 'int') == 30",
			// The wrapped form must keep working (variant(...) re-entry).
			"variants.as(variants.field(variant(%s), 'name'), 'string') == 'alice'",
			// A missing key is CEL null, not an error.
			"variants.field(%s, 'nope') == null",
		} {
			expr := fmt.Sprintf(tmpl, self)
			if !evalBool(t, expr, subject) {
				t.Errorf("%s: %s was false", kind, expr)
			}
		}
		// Negative control.
		expr := fmt.Sprintf("variants.as(variants.field(%s, 'name'), 'string') == 'bob'", self)
		if evalBool(t, expr, subject) {
			t.Errorf("%s: %s should have been false", kind, expr)
		}
	}
}

// An Avro variant field decodes (generically) to a map with metadata/value byte entries;
// variant(this.data) accepts it, mirroring the Python client's map handling.
func TestAvroVariantMapIntoCel(t *testing.T) {
	pv, err := variant.ParseJSON(`{"name":"alice","age":30}`)
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	type holder struct {
		Data map[string]interface{} `avro:"data"`
	}
	rec := holder{Data: map[string]interface{}{
		"metadata": pv.MetadataBytes(),
		"value":    pv.ValueBytes(),
	}}
	if !evalBool(t, "variants.as(variants.field(variant(this.data), 'name'), 'string') == 'alice'", rec) {
		t.Errorf("avro variant map did not marshal into CEL correctly")
	}
}

// TestAbsentVariantReadsAsNull pins that an *absent* variant - a Protobuf field left unset, or
// an Avro variant record whose byte fields are empty - carries no metadata, so there is nothing
// to read. It reads as CEL null and every accessor propagates that, rather than variant.New
// accepting it and a later read failing on a metadata version byte that isn't there.
func TestAbsentVariantReadsAsNull(t *testing.T) {
	exprs := []string{
		"variants.type(this) == null",
		// isNull is false, not an error: an absent variant is not a JSON null.
		"!variants.isNull(this)",
		"variants.field(this, 'name') == null",
		"variants.path(this, '$.name') == null",
		"variants.toJson(this) == null",
		// The explicit constructor reports it as CEL null too, like variant(null).
		"variant(this) == null",
	}
	// Both decode shapes: the Protobuf message, and the map an Avro variant record yields.
	subjects := map[string]interface{}{
		"proto": &typepb.Variant{Metadata: []byte{}, Value: []byte{}},
		"avro":  map[string]interface{}{"metadata": []byte{}, "value": []byte{}},
	}
	for name, subject := range subjects {
		for _, expr := range exprs {
			if !evalBool(t, expr, subject) {
				t.Errorf("%s (%s): expected true", expr, name)
			}
		}
	}
}

// TestExplicitNullVariantIsNotAbsent pins that absent stays distinguishable from a variant that
// genuinely holds JSON null: the former is CEL null, the latter a present variant of type NULL.
func TestExplicitNullVariantIsNotAbsent(t *testing.T) {
	pv, err := variant.ParseJSON("null")
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	msg := &typepb.Variant{Metadata: pv.MetadataBytes(), Value: pv.ValueBytes()}
	if !evalBool(t, "variants.isNull(this)", msg) {
		t.Errorf("an explicit JSON null must report isNull")
	}
	if !evalBool(t, "variants.type(this) != null", msg) {
		t.Errorf("an explicit JSON null is a present variant, so its type is not CEL null")
	}
}
