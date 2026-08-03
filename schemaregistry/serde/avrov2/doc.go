/**
 * Copyright 2022 Confluent Inc.
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

// Package avrov2 provides Avro serialization and deserialization for
// Confluent Schema Registry, backed by github.com/hamba/avro/v2.
//
// github.com/hamba/avro/v2 has been archived upstream. New code should
// prefer package avrov3, which uses Confluent's maintained fork
// (github.com/confluentinc/confluent-avro-go/v2). avrov2 remains
// available for backwards compatibility.
//
// Maintainer note: avrov3 was forked verbatim from this package with
// the underlying Avro library swapped. The two packages are kept
// structurally identical (same exported API, same internal logic).
// Any bug fix here should be applied to avrov3, and vice versa; any
// divergence must be documented inline with the rationale.
package avrov2
