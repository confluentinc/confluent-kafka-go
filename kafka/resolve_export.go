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

package kafka

// The preamble of a file with //export directives may only hold
// declarations, so the C side of the resolver lives in resolve.go.

/*
#include <stdint.h>
*/
import "C"

import "runtime/cgo"

// ckgoResolveGo calls the ResolveCallback behind h on behalf of
// ckgo_resolve_cb, from a librdkafka thread. On success it either sets
// toNode and toService to C strings the caller frees, or sets useDefault.
// A panic in the callback must not unwind into librdkafka's C frames, so it
// is recovered and reported as a lookup failure.
//
//export ckgoResolveGo
func ckgoResolveGo(h C.uintptr_t, node, service *C.char, toNode, toService **C.char, useDefault *C.int) (code C.int) {
	defer func() {
		if recover() != nil {
			*useDefault = 0
			code = resolveErrorCode(errResolvePanic)
		}
	}()

	cb := cgo.Handle(h).Value().(ResolveCallback)
	port := C.GoString(service)
	addrHost, addrPort, err := cb(C.GoString(node), port)
	if err != nil {
		return resolveErrorCode(err)
	}
	if addrHost == "" {
		*useDefault = 1
		return 0
	}
	if addrPort == "" {
		addrPort = port
	}
	*toNode = C.CString(addrHost)
	*toService = C.CString(addrPort)
	return 0
}
