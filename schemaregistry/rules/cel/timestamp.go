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
	"time"

	"cel.dev/cel-go/cel"
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
	}
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
	var t time.Time
	switch p {
	case precisionSeconds:
		t = time.Unix(val, 0)
	case precisionMillis:
		t = time.UnixMilli(val)
	case precisionMicros:
		t = time.UnixMicro(val)
	case precisionNanos:
		t = time.Unix(0, val)
	default:
		return types.NewErr(
			"timestamp: unknown precision %d; expected 0 (seconds), 3 (millis), 6 (micros) or 9 (nanos)", p)
	}
	return types.Timestamp{Time: t.UTC()}
}
