package kafka

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

// Automatically generate error codes from librdkafka
// See README for instructions
//go:generate $GOPATH/bin/go_rdkafka_generr generated_errors.go

/*
#include <stdlib.h>
#include "select_rdkafka.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// Error provides a Kafka-specific error container
type Error struct {
	code             ErrorCode
	str              string
	fatal            bool
	retriable        bool
	txnRequiresAbort bool
}

func newError(code C.rd_kafka_resp_err_t) (err Error) {
	return Error{code: ErrorCode(code)}
}

// NewError creates a new Error.
func NewError(code ErrorCode, str string, fatal bool) (err Error) {
	return Error{code: code, str: str, fatal: fatal}
}

func newErrorFromString(code ErrorCode, str string) (err Error) {
	return Error{code: code, str: str}
}

func newErrorFromCString(code C.rd_kafka_resp_err_t, cstr *C.char) (err Error) {
	var str string
	if cstr != nil {
		str = C.GoString(cstr)
	} else {
		str = ""
	}
	return Error{code: ErrorCode(code), str: str}
}

func newCErrorFromString(code C.rd_kafka_resp_err_t, str string) (err Error) {
	return newErrorFromString(ErrorCode(code), str)
}

// newErrorFromCError creates a new Error instance and destroys
// the passed cError.
func newErrorFromCErrorDestroy(cError *C.rd_kafka_error_t) Error {
	defer C.rd_kafka_error_destroy(cError)
	return Error{
		code:             ErrorCode(C.rd_kafka_error_code(cError)),
		str:              C.GoString(C.rd_kafka_error_string(cError)),
		fatal:            cint2bool(C.rd_kafka_error_is_fatal(cError)),
		retriable:        cint2bool(C.rd_kafka_error_is_retriable(cError)),
		txnRequiresAbort: cint2bool(C.rd_kafka_error_txn_requires_abort(cError)),
	}
}

// Error returns a human readable representation of an Error
// Same as Error.String()
func (e Error) Error() string {
	return e.String()
}

// String returns a human readable representation of an Error
func (e Error) String() string {
	var errstr string
	if len(e.str) > 0 {
		errstr = e.str
	} else {
		errstr = e.code.String()
	}

	if e.IsFatal() {
		return fmt.Sprintf("Fatal error: %s", errstr)
	}

	return errstr
}

// Code returns the ErrorCode of an Error
func (e Error) Code() ErrorCode {
	return e.code
}

// IsFatal returns true if the error is a fatal error.
// A fatal error indicates the client instance is no longer operable and
// should be terminated. Typical causes include non-recoverable
// idempotent producer errors.
func (e Error) IsFatal() bool {
	return e.fatal
}

// IsRetriable returns true if the operation that caused this error
// may be retried.
// This flag is currently only set by the Transactional producer API.
func (e Error) IsRetriable() bool {
	return e.retriable
}

// TxnRequiresAbort returns true if the error is an abortable transaction error
// that requires the application to abort the current transaction with
// AbortTransaction() and start a new transaction with BeginTransaction()
// if it wishes to proceed with transactional operations.
// This flag is only set by the Transactional producer API.
func (e Error) TxnRequiresAbort() bool {
	return e.txnRequiresAbort
}

// getFatalError returns an Error object if the client instance has raised a fatal error, else nil.
func getFatalError(H Handle) error {
	cErrstr := (*C.char)(C.malloc(C.size_t(512)))
	defer C.free(unsafe.Pointer(cErrstr))

	cErr := C.rd_kafka_fatal_error(H.gethandle().rk, cErrstr, 512)
	if int(cErr) == 0 {
		return nil
	}

	err := newErrorFromCString(cErr, cErrstr)
	err.fatal = true

	return err
}

// testFatalError triggers a fatal error in the underlying client.
// This is to be used strictly for testing purposes.
func testFatalError(H Handle, code ErrorCode, str string) ErrorCode {
	return ErrorCode(C.rd_kafka_test_fatal_error(H.gethandle().rk, C.rd_kafka_resp_err_t(code), C.CString(str)))
}
