// +build static

package kafka

// Force use of OpenSSL 1.0.x (from brew, rather than 0.9.8 from system)

// #cgo LDFLAGS: -L/usr/local/lib -lcrypto.1.0.0 -lssl.1.0.0
// #cgo pkg-config: rdkafka-static
import "C"
