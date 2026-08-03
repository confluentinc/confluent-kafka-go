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

// Package avrov3 provides Avro serialization and deserialization for
// Confluent Schema Registry, backed by
// github.com/confluentinc/confluent-avro-go/v2.
//
// The github.com/hamba/avro/v2 library used by package avrov2 is now
// archived. From this point onwards, avrov3 will use confluent-avro-go,
// Confluent's maintained fork of hamba/avro. Existing users of avrov2
// may continue using it; new code should prefer avrov3.
//
// Maintainer note: this package was forked verbatim from serde/avrov2
// with the underlying Avro library swapped from github.com/hamba/avro/v2
// to github.com/confluentinc/confluent-avro-go/v2. The two packages are
// kept structurally identical (same exported API, same internal logic).
// Any bug fix to one should be applied to the other; any avrov3-specific
// divergence must be documented inline with the rationale.
package avrov3
