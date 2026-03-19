//go:build !dynamic && !musl

package kafka

// #cgo CFLAGS: -DUSE_VENDORED_LIBRDKAFKA -DLIBRDKAFKA_STATICLIB
// #cgo LDFLAGS: ${SRCDIR}/librdkafka_vendor/librdkafka_glibc_linux_s390x.a -lm -ldl -lpthread -lrt -lssl -lcrypto -lsasl2 -lz -lcurl -lstdc++
import "C"

const LibrdkafkaLinkInfo = "static glibc_linux_s390x built from source"
