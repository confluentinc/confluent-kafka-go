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

package kafka

import (
	"testing"
)

//Test LibraryVersion()
func TestLibraryVersion(t *testing.T) {
	ver, verstr := LibraryVersion()
	if ver >= 0x00090200 {
		t.Logf("Library version %d: %s\n", ver, verstr)
	} else {
		t.Errorf("Unexpected Library version %d: %s\n", ver, verstr)
	}
}

//Test Offset APIs
func TestOffsetAPIs(t *testing.T) {
	offsets := []Offset{OffsetBeginning, OffsetEnd, OffsetInvalid, OffsetStored, 1001}
	for _, offset := range offsets {
		t.Logf("Offset: %s\n", offset.String())
	}

	// test known offset strings
	testOffsets := map[string]Offset{"beginning": OffsetBeginning,
		"earliest": OffsetBeginning,
		"end":      OffsetEnd,
		"latest":   OffsetEnd,
		"unset":    OffsetInvalid,
		"invalid":  OffsetInvalid,
		"stored":   OffsetStored}

	for key, expectedOffset := range testOffsets {
		offset, err := NewOffset(key)
		if err != nil {
			t.Errorf("Cannot create offset for %s, error: %s\n", key, err)
		} else {
			if offset != expectedOffset {
				t.Errorf("Offset does not equal expected: %s != %s\n", offset, expectedOffset)
			}
		}
	}

	// test numeric string conversion
	offset, err := NewOffset("10")
	if err != nil {
		t.Errorf("Cannot create offset for 10, error: %s\n", err)
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test integer offset
	var intOffset = 10
	offset, err = NewOffset(intOffset)
	if err != nil {
		t.Errorf("Cannot create offset for int 10, Error: %s\n", err)
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test int64 offset
	var int64Offset int64 = 10
	offset, err = NewOffset(int64Offset)
	if err != nil {
		t.Errorf("Cannot create offset for int64 10, Error: %s \n", err)
	} else {
		if offset != Offset(10) {
			t.Errorf("Offset does not equal expected: %s != %s\n", offset, Offset(10))
		}
	}

	// test invalid string offset
	invalidOffsetString := "what is this offset"
	offset, err = NewOffset(invalidOffsetString)
	if err == nil {
		t.Errorf("Expected error for this string offset. Error: %s\n", err)
	} else if offset != Offset(0) {
		t.Errorf("Expected offset (%v), got (%v)\n", Offset(0), offset)
	}
	t.Logf("Offset for string (%s): %v\n", invalidOffsetString, offset)

	// test double offset
	doubleOffset := 12.15
	offset, err = NewOffset(doubleOffset)
	if err == nil {
		t.Errorf("Expected error for this double offset: %f. Error: %s\n", doubleOffset, err)
	} else if offset != OffsetInvalid {
		t.Errorf("Expected offset (%v), got (%v)\n", OffsetInvalid, offset)
	}
	t.Logf("Offset for double (%f): %v\n", doubleOffset, offset)

	// test change offset via Set()
	offset, err = NewOffset("beginning")
	if err != nil {
		t.Errorf("Cannot create offset for 'beginning'. Error: %s\n", err)
	}

	// test change to a logical offset
	err = offset.Set("latest")
	if err != nil {
		t.Errorf("Cannot set offset to 'latest'. Error: %s \n", err)
	} else if offset != OffsetEnd {
		t.Errorf("Failed to change offset. Expect (%v), got (%v)\n", OffsetEnd, offset)
	}

	// test change to an integer offset
	err = offset.Set(int(10))
	if err != nil {
		t.Errorf("Cannot set offset to (%v). Error: %s \n", 10, err)
	} else if offset != 10 {
		t.Errorf("Failed to change offset. Expect (%v), got (%v)\n", 10, offset)
	}

	// test OffsetTail()
	tail := OffsetTail(offset)
	t.Logf("offset tail %v\n", tail)

}
