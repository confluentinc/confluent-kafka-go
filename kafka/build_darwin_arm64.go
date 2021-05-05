// +build !dynamic


// This file was hand-made by jvisser@confluent.io.

package kafka

// #cgo CFLAGS: -DUSE_VENDORED_LIBRDKAFKA
// #cgo LDFLAGS: ${SRCDIR}/librdkafka_vendor/librdkafka_darwin_arm64.a  -lz -lm -lsasl2 -ldl -lpthread
import "C"

// LibrdkafkaLinkInfo explains how librdkafka was linked to the Go client
const LibrdkafkaLinkInfo = "static darwin compiled from source at v1.6.1"
