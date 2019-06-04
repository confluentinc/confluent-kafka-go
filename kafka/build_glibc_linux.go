// +build !musl
// +build !dynamic
// +build !static
// +build !static_all

package kafka

// #cgo LDFLAGS: ${SRCDIR}/librdkafka/librdkafka_glibc_linux.a -lm -lz -ldl -lpthread -lrt
import "C"
