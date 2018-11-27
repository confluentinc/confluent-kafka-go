// +build static_linux
// +build !static_all

package kafka

// #cgo pkg-config: rdkafka
// #cgo LDFLAGS: -Wl,-Bstatic -lrdkafka -llz4 -lssl -lcrypto -lz -lgcc -Wl,-Bdynamic -lsasl2 -lrt -ldl -lm -lpthread
import "C"
