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
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/overloads"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

// Flink-style decimal precisions for the two-argument timestamp constructor.
const (
	precisionSeconds = 0
	precisionMillis  = 3
	precisionMicros  = 6
	precisionNanos   = 9
)

// The CEL timestamp range, 0001-01-01T00:00:00Z..9999-12-31T23:59:59.999999999Z, in epoch
// seconds. Same bounds as the reference's TimestampUtils.MIN/MAX_EPOCH_SECOND.
const (
	minEpochSecond = -62135596800
	maxEpochSecond = 253402300799
)

// timestampOptions adds one overload to the *standard* timestamp constructor, rather than a
// timestamp.of namespace of our own: timestamp(int, int), an epoch value at a decimal
// precision. cel-go merges the declaration into the stdlib function, so timestamp(string),
// timestamp(int) (epoch seconds) and timestamp(timestamp) all keep their stdlib bindings.
//
// Nothing is needed for the one-argument non-int cases: cel-go's type adapter already maps
// time.Time and *timestamppb.Timestamp to CEL's timestamp, so an Avro or Protobuf timestamp
// field satisfies the stdlib identity overload with no wrapper at all.
func timestampOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("timestamp",
			cel.Overload("timestamp_int_int", []*cel.Type{cel.IntType, cel.IntType}, cel.TimestampType,
				cel.BinaryBinding(fromEpochPrecision)),
		),
		// Replaces the standard string(timestamp), which DefaultEnv excludes at the overload
		// level. See formatTimestamp for why.
		cel.Function(overloads.TypeConvertString,
			cel.Overload(overloads.TimestampToString, []*cel.Type{cel.TimestampType}, cel.StringType,
				cel.UnaryBinding(func(v ref.Val) ref.Val {
					t, ok := v.Value().(time.Time)
					if !ok {
						return types.NewErr("string: not a timestamp")
					}
					return types.String(formatTimestamp(t))
				})),
		),
	}
}

// formatTimestamp renders a timestamp the way every other client's string(...) does.
//
// cel-go's builtin formats with time.RFC3339Nano, which strips trailing zeros from the
// fractional second: an instant at .100 rendered as ".1Z" and .500 as ".5Z", where Java, C++ and
// JS all give ".100Z" and ".500Z". The value was never wrong - only its rendering - which made it
// a silent divergence rather than an error.
//
// The fraction is emitted in whole 3-digit groups, matching protobuf's Timestamps.toString (the
// Java reference): none when it is zero, then 3, 6 or 9 digits for a value that is a whole
// millisecond, microsecond, or neither. Always rendered in UTC with a Z suffix, as Java does.
func formatTimestamp(t time.Time) string {
	utc := t.UTC()
	text := utc.Format("2006-01-02T15:04:05")
	switch nanos := utc.Nanosecond(); {
	case nanos == 0:
	case nanos%1e6 == 0:
		text += fmt.Sprintf(".%03d", nanos/1e6)
	case nanos%1e3 == 0:
		text += fmt.Sprintf(".%06d", nanos/1e3)
	default:
		text += fmt.Sprintf(".%09d", nanos)
	}
	return text + "Z"
}

// fromEpochPrecision builds a timestamp from an epoch numeric value at a decimal precision.
// Precisions outside {0, 3, 6, 9} are rejected rather than generalized to "any p means 10^-p":
// with the unit a number rather than a name, that check is the only thing between a typo and a
// silently wrong instant.
func fromEpochPrecision(v, precision ref.Val) ref.Val {
	val, ok := v.Value().(int64)
	if !ok {
		return types.NewErr("timestamp: the epoch value must be an int")
	}
	p, ok := precision.Value().(int64)
	if !ok {
		return types.NewErr("timestamp: the precision must be an int")
	}
	var perSecond int64
	switch p {
	case precisionSeconds:
		perSecond = 1
	case precisionMillis:
		perSecond = 1_000
	case precisionMicros:
		perSecond = 1_000_000
	case precisionNanos:
		perSecond = 1_000_000_000
	default:
		return types.NewErr(
			"timestamp: unknown precision %d; expected 0 (seconds), 3 (millis), 6 (micros) or 9 (nanos)", p)
	}
	// Range-checked before construction, not after, and on the epoch rather than on the
	// resulting year. Building types.Timestamp directly skips the bound cel-go applies to its
	// own timestamp() conversions, and time.Unix overflows silently past it: measured,
	// timestamp(253402300800, 0) rendered as 10000-01-01T00:00:00Z, timestamp(-62135596801, 0)
	// as year 0, and timestamp(-9223372036854775807, 0) as 292277026596-12-04 - a *positive*
	// year from a far-past epoch, the sign lost in the wrap. The reference refuses all of them
	// in TimestampUtils.instantOfEpoch, which is what this mirrors.
	seconds := floorDiv(val, perSecond)
	if seconds < minEpochSecond || seconds > maxEpochSecond {
		return types.NewErr(
			"timestamp: out of range: %d seconds since the epoch is outside "+
				"0001-01-01T00:00:00Z..9999-12-31T23:59:59.999999999Z", seconds)
	}
	nanos := (val - seconds*perSecond) * (1_000_000_000 / perSecond)
	return types.Timestamp{Time: time.Unix(seconds, nanos).UTC()}
}

// floorDiv rounds toward negative infinity, so a pre-epoch value keeps a non-negative
// sub-second remainder - matching the reference's Math.floorDiv.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
