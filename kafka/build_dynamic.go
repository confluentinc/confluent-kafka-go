// +build dynamic

package kafka

// #cgo pkg-config: rdkafka
import "C"

// LibrdkafkaLinkInfo explains how librdkafka was linked to the Go client
const LibrdkafkaLinkInfo = "dynamically linked to librdkafka"
