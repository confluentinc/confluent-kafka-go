// +build static
// +build !static_all

package kafka

// #cgo pkg-config: --static rdkafka
// #cgo LDFLAGS: -Wl,-Bstatic -lrdkafka -Wl,-Bdynamic
import "C"
