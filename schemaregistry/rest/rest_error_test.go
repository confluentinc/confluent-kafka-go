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

package rest

import "testing"

func TestHasStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      Error
		status   int
		expected bool
	}{
		{
			name:     "status and refining error code",
			err:      Error{Status: 404, Code: 40470},
			status:   404,
			expected: true,
		},
		{
			name:     "status without a usable error code",
			err:      Error{Status: 404, Code: -1},
			status:   404,
			expected: true,
		},
		{
			name:     "error code alone",
			err:      Error{Code: 40470},
			status:   404,
			expected: true,
		},
		{
			name:     "error code equal to the status",
			err:      Error{Code: 404},
			status:   404,
			expected: true,
		},
		{
			name:     "conflict",
			err:      Error{Status: 409, Code: 40971},
			status:   409,
			expected: true,
		},
		{
			name:     "different status",
			err:      Error{Status: 500, Code: 50070},
			status:   404,
			expected: false,
		},
		{
			name:     "status disagreeing with the error code is authoritative",
			err:      Error{Status: 502, Code: 40470},
			status:   404,
			expected: false,
		},
		{
			name:     "status disagreeing with the error code still matches itself",
			err:      Error{Status: 502, Code: 40470},
			status:   502,
			expected: true,
		},
		{
			name:     "error code that is neither a status nor a refined status",
			err:      Error{Code: 4045},
			status:   404,
			expected: false,
		},
		{
			name:     "no status and no error code",
			err:      Error{},
			status:   404,
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.err.HasStatus(test.status); got != test.expected {
				t.Errorf("HasStatus(%d) on %+v = %v, expected %v",
					test.status, test.err, got, test.expected)
			}
		})
	}
}
