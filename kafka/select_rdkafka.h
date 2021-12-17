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

// This file uses a preprocessor macro defined by the various build_*.go
// files to determine whether to import the bundled librdkafka header, or
// the system one.
// This is needed because cgo will automatically add -I. to the include
// path, so <librdkafka/rdkafka.h> would find a bundled header instead of
// the system one if it were called librdkafka/rdkafka.h instead of
// librdkafka_vendor/rdkafka.h

#ifdef USE_VENDORED_LIBRDKAFKA
#include "librdkafka_vendor/rdkafka.h"
#include "librdkafka_vendor/rdkafka_mock.h"
#else
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>
#endif
