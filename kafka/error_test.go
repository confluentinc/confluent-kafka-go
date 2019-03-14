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
	"strings"
	"testing"
)

//TestFatalError tests fatal errors
func TestFatalError(t *testing.T) {
	fatalErr := newErrorFromString(ErrOutOfOrderSequenceNumber, "Testing a fatal error")
	fatalErr.fatal = true
	if !fatalErr.IsFatal() {
		t.Errorf("Expected IsFatal() to return true for %v", fatalErr)
	}
	t.Logf("%v", fatalErr)

	normalErr := newErrorFromString(ErrOutOfOrderSequenceNumber, "Testing a non-fatal error")
	if normalErr.IsFatal() {
		t.Errorf("Expected IsFatal() to return false for %v", normalErr)
	}
	t.Logf("%v", normalErr)
}

//TestFatalErrorClient tests fatal errors using a client instance
func TestFatalErrorClient(t *testing.T) {
	p, err := NewProducer(&ConfigMap{})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}

	err = p.GetFatalError()
	if err != nil {
		t.Fatalf("Expected no fatal error, got %s", err)
	}

	errstr := "A_FATAL_ERROR_TEST"
	p.TestFatalError(ErrInvalidTimestamp, errstr)

	err = p.GetFatalError()
	if err == nil {
		t.Fatalf("Expected a fatal error, got nil")
	}

	if err.(Error).Code() != ErrInvalidTimestamp ||
		!strings.HasSuffix(err.(Error).String(), errstr) {
		t.Fatalf("Expected ErrInvalidTimestamp and %s suffix, got %v: %s",
			errstr, err.(Error).Code(), err.(Error).String())
	}

	if !err.(Error).IsFatal() {
		t.Fatalf("Expected error to be fatal")
	}

	p.Close()
}
