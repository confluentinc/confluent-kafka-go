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

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	unitMillis  = "millis"
	unitMicros  = "micros"
	unitNanos   = "nanos"
	unitSeconds = "seconds"
)

// timestampOptions declares timestamp.of, mirroring the other clients. The result uses CEL's
// built-in timestamp type (backed by time.Time), so no new type is introduced.
func timestampOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("timestamp.of",
			cel.Overload("timestamp_of_dyn", []*cel.Type{cel.DynType}, cel.TimestampType,
				cel.UnaryBinding(toTimestamp)),
			cel.Overload("timestamp_of_int_string", []*cel.Type{cel.IntType, cel.StringType}, cel.TimestampType,
				cel.BinaryBinding(fromEpoch)),
		),
	}
}

// toTimestamp is the runtime dispatch backing timestamp.of(dyn). It accepts the shapes
// Avro (time.Time) and Protobuf (google.protobuf.Timestamp) decoders produce, plus RFC 3339
// strings. A raw int lacks a unit and must use timestamp.of(value, unit).
func toTimestamp(v ref.Val) ref.Val {
	switch x := v.Value().(type) {
	case time.Time:
		return types.Timestamp{Time: x}
	case *timestamppb.Timestamp:
		return types.Timestamp{Time: x.AsTime()}
	case string:
		t, err := time.Parse(time.RFC3339Nano, x)
		if err != nil {
			return types.NewErr("timestamp.of: cannot parse %q as RFC 3339: %v", x, err)
		}
		return types.Timestamp{Time: t}
	case int64:
		return types.NewErr("timestamp.of: raw int needs a unit; use " +
			"timestamp.of(value, \"millis\"|\"micros\"|\"nanos\"|\"seconds\")")
	default:
		return types.NewErr("timestamp.of: cannot convert %s to Timestamp", v.Type().TypeName())
	}
}

// fromEpoch builds a timestamp from an epoch numeric value plus a unit string.
func fromEpoch(v, unit ref.Val) ref.Val {
	val, ok := v.Value().(int64)
	if !ok {
		return types.NewErr("timestamp.of: value must be an int")
	}
	unitStr, ok := unit.Value().(string)
	if !ok {
		return types.NewErr("timestamp.of: unit must be a string")
	}
	var t time.Time
	switch unitStr {
	case unitMillis:
		t = time.UnixMilli(val)
	case unitMicros:
		t = time.UnixMicro(val)
	case unitNanos:
		t = time.Unix(0, val)
	case unitSeconds:
		t = time.Unix(val, 0)
	default:
		return types.NewErr("timestamp.of: unknown unit %q; expected millis, micros, nanos, seconds", unitStr)
	}
	return types.Timestamp{Time: t.UTC()}
}
