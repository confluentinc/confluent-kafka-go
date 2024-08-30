/**
 * Copyright 2024 Confluent Inc.
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

package serde

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
)

func TestWildCardMatcher(t *testing.T) {
	maybeFail = initFailFunc(t)

	maybeFail(expect(match("", "Foo"), false))
	maybeFail(expect(match("Foo", ""), false))
	maybeFail(expect(match("", ""), true))
	maybeFail(expect(match("Foo", "Foo"), true))
	maybeFail(expect(match("", "*"), true))
	maybeFail(expect(match("", "?"), false))
	maybeFail(expect(match("Foo", "Fo*"), true))
	maybeFail(expect(match("Foo", "Fo?"), true))
	maybeFail(expect(match("Foo Bar and Catflag", "Fo*"), true))
	maybeFail(expect(match("New Bookmarks", "N?w ?o?k??r?s"), true))
	maybeFail(expect(match("Foo Bar and Catflag", "Fo*"), true))
	maybeFail(expect(match("Foo", "Bar"), false))
	maybeFail(expect(match("Foo Bar Foo", "F*o Bar*"), true))
	maybeFail(expect(match("Adobe Acrobat Installer", "Ad*er"), true))
	maybeFail(expect(match("Foo", "*Foo"), true))
	maybeFail(expect(match("BarFoo", "*Foo"), true))
	maybeFail(expect(match("Foo", "Foo*"), true))
	maybeFail(expect(match("FooBar", "Foo*"), true))
	maybeFail(expect(match("FOO", "*Foo"), false))
	maybeFail(expect(match("BARFOO", "*Foo"), false))
	maybeFail(expect(match("FOO", "Foo*"), false))
	maybeFail(expect(match("FOOBAR", "Foo*"), false))
	maybeFail(expect(match("eve", "eve*"), true))
	maybeFail(expect(match("alice.bob.eve", "a*.bob.eve"), true))
	maybeFail(expect(match("alice.bob.eve", "a*.bob.e*"), true))
	maybeFail(expect(match("alice.bob.eve", "a*"), false))
	maybeFail(expect(match("alice.bob.eve", "a**"), true))
	maybeFail(expect(match("alice.bob.eve", "alice.bob*"), false))
	maybeFail(expect(match("alice.bob.eve", "alice.bob**"), true))
}

type failFunc func(...error)

var maybeFail failFunc

func initFailFunc(t *testing.T) failFunc {
	tester := t
	return func(errors ...error) {
		for _, err := range errors {
			if err != nil {
				pc := make([]uintptr, 1)
				runtime.Callers(2, pc)
				caller := runtime.FuncForPC(pc[0])
				_, line := caller.FileLine(caller.Entry())

				tester.Fatalf("%s:%d failed: %s", caller.Name(), line, err)
			}
		}
	}
}

func expect(actual, expected interface{}) error {
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("expected: %v, Actual: %v", expected, actual)
	}

	return nil
}
