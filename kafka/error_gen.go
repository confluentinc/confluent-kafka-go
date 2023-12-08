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

package kafka

// Automatically generate error codes from librdkafka
// See README for instructions
//go:generate $GOPATH/bin/go_rdkafka_generr generated_errors.go

/*
#include <stdlib.h>
#include "select_rdkafka.h"

static const char *errdesc_to_string (const struct rd_kafka_err_desc *ed, int idx) {
   return ed[idx].name;
}

static const char *errdesc_to_desc (const struct rd_kafka_err_desc *ed, int idx) {
   return ed[idx].desc;
}
*/
import "C"

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// camelCase transforms a snake_case string to camelCase.
func camelCase(s string) string {
	ret := ""
	for _, v := range strings.Split(s, "_") {
		if len(v) == 0 {
			continue
		}
		ret += strings.ToUpper((string)(v[0])) + strings.ToLower(v[1:])
	}
	return ret
}

// WriteErrorCodes writes Go error code constants to file from the
// librdkafka error codes.
// This function is not intended for public use.
func WriteErrorCodes(f *os.File) {
	f.WriteString("package kafka\n\n")
	now := time.Now()
	f.WriteString(fmt.Sprintf("// Copyright 2016-%d Confluent Inc.\n", now.Year()))
	f.WriteString(fmt.Sprintf("// AUTOMATICALLY GENERATED ON %v USING librdkafka %s\n",
		now, C.GoString(C.rd_kafka_version_str())))

	var errdescs *C.struct_rd_kafka_err_desc
	var csize C.size_t
	C.rd_kafka_get_err_descs(&errdescs, &csize)

	f.WriteString(`
/*
#include "select_rdkafka.h"
*/
import "C"

// ErrorCode is the integer representation of local and broker error codes
type ErrorCode int

// String returns a human readable representation of an error code
func (c ErrorCode) String() string {
	return C.GoString(C.rd_kafka_err2str(C.rd_kafka_resp_err_t(c)))
}

const (
`)

	for i := 0; i < int(csize); i++ {
		orig := C.GoString(C.errdesc_to_string(errdescs, C.int(i)))
		if len(orig) == 0 {
			continue
		}
		desc := C.GoString(C.errdesc_to_desc(errdescs, C.int(i)))
		if len(desc) == 0 {
			continue
		}

		errname := "Err" + camelCase(orig)

		// Special handling to please golint
		// Eof -> EOF
		// Id -> ID
		errname = strings.Replace(errname, "Eof", "EOF", -1)
		errname = strings.Replace(errname, "Id", "ID", -1)

		f.WriteString(fmt.Sprintf("\t// %s %s\n", errname, desc))
		f.WriteString(fmt.Sprintf("\t%s ErrorCode = C.RD_KAFKA_RESP_ERR_%s\n",
			errname, orig))
	}

	f.WriteString(")\n")

}
