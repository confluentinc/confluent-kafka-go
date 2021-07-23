// Note upstream specifies a dynamic build tag for this file, but we want to build with dynamic
// linking to librdkafka always, so I removed it.

package kafka

// #cgo pkg-config: rdkafka
import "C"

// LibrdkafkaLinkInfo explains how librdkafka was linked to the Go client
const LibrdkafkaLinkInfo = "dynamically linked to librdkafka"
