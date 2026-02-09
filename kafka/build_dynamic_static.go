//go:build static_external
// +build static_external

package kafka

// #cgo pkg-config: rdkafka-static
import "C"

// LibrdkafkaLinkInfo explains how librdkafka was linked to the Go client
const LibrdkafkaLinkInfo = "dynamically linked to static librdkafka"
