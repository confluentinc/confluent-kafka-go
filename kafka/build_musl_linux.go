// +build musl
// +build !dynamic
// +build !static
// +build !static_all

package kafka

// #cgo LDFLAGS: ${SRCDIR}/librdkafka/librdkafka_musl_linux.a -lm -ldl -lpthread -lrt
import "C"
