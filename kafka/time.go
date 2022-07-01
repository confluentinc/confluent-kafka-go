/**
 * Copyright 2019 Confluent Inc.
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

import "C"

import (
	"context"
	"time"
)

const (
	cTimeoutInfinite = C.int(-1) // Blocks indefinitely until completion.
	cTimeoutNoWait   = C.int(0)  // Returns immediately without blocking.
)

// cTimeoutFromContext returns the remaining time after which work done on behalf of this context
// should be canceled, in milliseconds.
//
// If no deadline/timeout is set, or if the timeout does not fit in an int32, it returns
// cTimeoutInfinite;
// If there is no time left in this context, it returns cTimeoutNoWait.
func cTimeoutFromContext(ctx context.Context) C.int {
	if ctx == nil {
		return cTimeoutInfinite
	}
	timeout, hasTimeout := timeout(ctx)
	if !hasTimeout {
		return cTimeoutInfinite
	}
	if timeout <= 0 {
		return cTimeoutNoWait
	}

	timeoutMs := int64(timeout / time.Millisecond)
	if int64(int32(timeoutMs)) < timeoutMs {
		return cTimeoutInfinite
	}

	return C.int(timeoutMs)
}
