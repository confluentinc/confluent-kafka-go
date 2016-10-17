// +build !static
// +build static_all

package kafka

// #cgo pkg-config: --static rdkafka
// #cgo LDFLAGS: -static
import "C"
