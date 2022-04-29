package schemaregistry

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"testing"
)

/*
#include <librdkafka/rdkafka.h>
*/
import "C"

type testConf map[string]interface{}
type failFunc func(string, ...error)

var testconf = make(testConf)
var srClient Client
var maybeFail failFunc

// NewTestConf reads the test suite config file testconf.json which must
// contain at least Brokers and Topic string properties.
// Returns Testconf if the testconf was found and usable,
// error if file can't be read correctly
func testconfRead() bool {
	cf, err := os.Open("../kafka/testconf.json")
	defer cf.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s testconf.json not found - ignoring test\n", err)
		return false
	}

	jp := json.NewDecoder(cf)
	err = jp.Decode(&testconf)

	if err != nil {
		panic(fmt.Sprintf("Failed to parse testconf: %s", err))
	}

	return true
}

// getObject returns a child object of the root testConf
func (tc testConf) getObject(name string) testConf {
	return tc[name].(map[string]interface{})
}

// getString returns a string representation of the value represented by key from the provided namespace
// if the namespace is an empty string the root object will be searched.
func (tc testConf) getString(key string) string {
	val, ok := tc[key]
	if ok {
		return val.(string)
	}
	return ""
}

// getInt returns an integer representation of the value represented by key from the provided namespace
// If the namespace is an empty string the root object will be searched.
func (tc testConf) getInt(key string) int {
	val, ok := tc[key]
	if ok {
		return val.(int)
	}
	return 0
}

func initFailFunc(t *testing.T) failFunc {
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

func expect(actual, expected interface{}) error {
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("expected: %v, Actual: %v", expected, actual)
	}

	return nil
}
