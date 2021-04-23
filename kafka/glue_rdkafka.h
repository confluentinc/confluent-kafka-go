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
#pragma once


/**
 * Glue between Go, Cgo and librdkafka
 */


/**
 * Temporary C to Go header representation
 */
typedef struct tmphdr_s {
  const char *key;
  const void *val;   // producer: malloc()ed by Go code if size > 0
                     // consumer: owned by librdkafka
  ssize_t     size;
} tmphdr_t;



/**
 * @struct This is a glue struct used by the C code in this client to
 *         effectively map fields from a librdkafka rd_kafka_message_t
 *         to something usable in Go with as few CGo calls as possible.
 */
typedef struct glue_msg_s {
  rd_kafka_message_t *msg;
  rd_kafka_timestamp_type_t tstype;
  int64_t   ts;
  tmphdr_t *tmphdrs;
  size_t    tmphdrsCnt;
  int8_t    want_hdrs;  /**< If true, copy headers */
} glue_msg_t;
