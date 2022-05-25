/**
 * Copyright 2020 Confluent Inc.
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

package main

// Utility that outputs the librdkafka version and link info,
// used by CI jobs.

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)


func main () {
	vnum, vstr := kafka.LibraryVersion()
	fmt.Printf("LibraryVersion: %s (0x%x)\n", vstr, vnum)
	fmt.Printf("LinkInfo:       %s\n", kafka.LibrdkafkaLinkInfo)

}
