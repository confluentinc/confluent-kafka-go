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

import (
	"context"
	"time"
)

// Timeout returns the remaining time after which work done on behalf of this context should be
// canceled, or ok==false if no deadline/timeout is set.
func timeout(ctx context.Context) (timeout time.Duration, ok bool) {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline.Sub(time.Now()), true
	}
	return 0, false
}
