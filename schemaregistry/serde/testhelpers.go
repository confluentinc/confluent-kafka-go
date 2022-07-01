package serde

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
)

// FailFunc is a function to call in case of failure
type FailFunc func(string, ...error)

// MaybeFail represents a fail function
var MaybeFail FailFunc

// InitFailFunc returns an initial fail function
func InitFailFunc(t *testing.T) FailFunc {
	tester := t
	return func(msg string, errors ...error) {
		for _, err := range errors {
			if err != nil {
				pc := make([]uintptr, 1)
				runtime.Callers(2, pc)
				caller := runtime.FuncForPC(pc[0])
				_, line := caller.FileLine(caller.Entry())

				tester.Fatalf("%s:%d failed: %s %s", caller.Name(), line, msg, err)
			}
		}
	}
}

// Expect compares the actual and expected values
func Expect(actual, expected interface{}) error {
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("expected: %v, Actual: %v", expected, actual)
	}

	return nil
}
