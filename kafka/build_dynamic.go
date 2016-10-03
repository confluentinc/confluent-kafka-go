// +build !static
// +build !static_all

package kafka

// #cgo pkg-config: rdkafka
// #cgo LDFLAGS: -lrdkafka
import "C"
