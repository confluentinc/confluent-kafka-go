package kafka

import (
	"C"
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// PlainCredsCallback is the type of the Go function that will be called to obtain the SASL username and password
// for a Kafka client/
type PlainCredsCallbackFunc func() (string, string, error)

var (
	// cbMap maps the address of the Kafka client (*rd_kafka_t) to the callback function. This map is necessary
	// because all Go Kafka clients use the same C function as the credentials callback. That C function then
	// passes control to a Go intermediary, which in turn calls the right Go callback by referencing this map.
	cbMap map[unsafe.Pointer]PlainCredsCallbackFunc
	mu    sync.RWMutex
)

func init() {
	cbMap = make(map[unsafe.Pointer]PlainCredsCallbackFunc)
}

// insertPlainCredsCallback inserts a Go callback into the disambiguation map (cbMap).
func insertPlainCredsCallback(rk unsafe.Pointer, fun PlainCredsCallbackFunc) {
	mu.Lock()
	defer mu.Unlock()
	cbMap[rk] = fun
}

// removePlainCredsCallback removes a Go callback from the disambiguation map. This should be called after
// destroying a Go Kafka client.
func removePlainCredsCallback(rk unsafe.Pointer) {
	mu.Lock()
	defer mu.Unlock()
	delete(cbMap, rk)
}

// extractCallbackFromConfig does the needful to check if the user has set "plain.creds.cb" in the config map.
// If so, extracts that value and casts it to the correct type and returns it.
func extractCallbackFromConfigMap(conf *ConfigMap) (PlainCredsCallbackFunc, error) {
	cb, err := conf.extract("plain.creds.cb", nil)
	if err != nil {
		return nil, err
	}
	if cb == nil {
		return nil, nil
	}
	// Can't use PlainCredsCallbackFunc here because of the way Go's type system works.
	if cbFun, ok := cb.(func() (string, string, error)); ok {
		return cbFun, nil
	}
	return nil, errors.New("plain.creds.cb callback function is not of the correct type")
}

// callPlainCredsCallback is the intermediary between the C function installed as the actual callback and the
// Go callback registered by the user. This intermediary is called by the C function. It will allocate memory
// for the username and password that are returned from the Go callback. It is the caller's responsibility
// (in other word: The C callback's responsibility) to remove those.
// Note: If the Go callback returns an error this function will return -1 and the actual error is lost.
//export callPlainCredsCallback
func callPlainCredsCallback(rk *C.int, username **C.char, password **C.char, errmsg **C.char) {
	*errmsg = nil
	mu.RLock()
	fun := cbMap[unsafe.Pointer(rk)]
	mu.RUnlock()

	if fun == nil {
		*errmsg = C.CString(fmt.Sprintf("callPlainCredsCallback called for unknown rk %x", rk))
		return
	}

	u, p, err := fun()
	if err != nil {
		*errmsg = C.CString(err.Error())
		return
	}

	*username = C.CString(u)
	*password = C.CString(p)
}
