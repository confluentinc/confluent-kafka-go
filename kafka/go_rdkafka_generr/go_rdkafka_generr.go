// confluent-kafka-go internal tool to generate error constants from librdkafka
package main

/**
 * Copyright 2016 Confluent Inc.
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

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {

	outfile := os.Args[1]

	f, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	kafka.WriteErrorCodes(f)
}
