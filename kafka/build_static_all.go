// +build !static
// +build static_all

package kafka

// #cgo LDFLAGS: -static
// #cgo pkg-config: rdkafka-static
import "C"
