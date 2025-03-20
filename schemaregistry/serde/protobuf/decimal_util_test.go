/**
 * Copyright 2025 Confluent Inc.
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

package protobuf

import (
	"math/big"
	"reflect"
	"testing"
)

func TestDecimalConversion(t *testing.T) {
	tests := []struct {
		name  string
		input *big.Rat
		scale int32
	}{
		{
			name:  "1st value",
			input: big.NewRat(0, 1),
			scale: 0,
		},
		{
			name:  "2nd value",
			input: big.NewRat(101, 100),
			scale: 2,
		},
		{
			name:  "3rd value",
			input: big.NewRat(123456789123456789, 100),
			scale: 2,
		},
		{
			name:  "4th value",
			input: big.NewRat(1234, 1),
			scale: 0,
		},
		{
			name:  "5h value",
			input: big.NewRat(12345, 10),
			scale: 1,
		},
		{
			name:  "6th value",
			input: big.NewRat(-0, 1),
			scale: 0,
		},
		{
			name:  "7th value",
			input: big.NewRat(-101, 100),
			scale: 2,
		},
		{
			name:  "8th value",
			input: big.NewRat(-123456789123456789, 100),
			scale: 2,
		},
		{
			name:  "9th value",
			input: big.NewRat(-1234, 1),
			scale: 0,
		},
		{
			name:  "10th value",
			input: big.NewRat(-12345, 10),
			scale: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, err := BigRatToDecimal(test.input, test.scale)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			result, err := DecimalToBigRat(converted)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(test.input, result) {
				t.Fatalf("not equal: input %v, output %v", test.input, result)
			}
		})
	}
}
