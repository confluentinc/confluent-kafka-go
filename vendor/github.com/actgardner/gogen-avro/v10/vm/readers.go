package vm

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"unsafe"
)

type ByteReader interface {
	ReadByte() (byte, error)
}

func readBool(r io.Reader, buf []byte) (bool, error) {
	var b byte
	var err error
	if br, ok := r.(ByteReader); ok {
		b, err = br.ReadByte()
	} else {
		bs := buf[0:1]
		_, err = io.ReadFull(r, bs)
		if err != nil {
			return false, err
		}
		b = bs[0]
	}
	return b == 1, nil
}

func readBytes(r io.Reader, buf []byte) ([]byte, error) {
	size, err := readLong(r, buf)
	if err != nil {
		return nil, err
	}

	// makeslice can fail depending on available memory.
	// We arbitrarily limit string size to sane default (~2.2GB).
	if size < 0 || size > math.MaxInt32 {
		return nil, fmt.Errorf("bytes length out of range: %d", size)
	}

	if size == 0 {
		return []byte{}, nil
	}

	bb := make([]byte, size)
	_, err = io.ReadFull(r, bb)
	return bb, err
}

func readDouble(r io.Reader, buf []byte) (float64, error) {
	buf = buf[0:8]
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint64(buf)
	val := math.Float64frombits(bits)
	return val, nil
}

func readFloat(r io.Reader, buf []byte) (float32, error) {
	buf = buf[0:4]
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}
	bits := binary.LittleEndian.Uint32(buf)
	val := math.Float32frombits(bits)
	return val, nil
}

func readInt(r io.Reader, buf []byte) (int32, error) {
	var v int
	var b byte
	var err error
	if br, ok := r.(ByteReader); ok {
		for shift := uint(0); ; shift += 7 {
			if b, err = br.ReadByte(); err != nil {
				return 0, err
			}
			v |= int(b&127) << shift
			if b&128 == 0 {
				break
			}
		}
	} else {
		buf := buf[0:1]
		for shift := uint(0); ; shift += 7 {
			if _, err := io.ReadFull(r, buf); err != nil {
				return 0, err
			}
			b = buf[0]
			v |= int(b&127) << shift
			if b&128 == 0 {
				break
			}
		}
	}
	datum := (int32(v>>1) ^ -int32(v&1))
	return datum, nil
}

func readLong(r io.Reader, buf []byte) (int64, error) {
	var v uint64
	var b byte
	var err error
	if br, ok := r.(ByteReader); ok {
		for shift := uint(0); ; shift += 7 {
			if b, err = br.ReadByte(); err != nil {
				return 0, err
			}
			v |= uint64(b&127) << shift
			if b&128 == 0 {
				break
			}
		}
	} else {
		buf := buf[0:1]
		for shift := uint(0); ; shift += 7 {
			if _, err = io.ReadFull(r, buf); err != nil {
				return 0, err
			}
			b = buf[0]
			v |= uint64(b&127) << shift
			if b&128 == 0 {
				break
			}
		}
	}
	datum := (int64(v>>1) ^ -int64(v&1))
	return datum, nil
}

func readString(r io.Reader, buf []byte) (string, error) {
	len, err := readLong(r, buf)
	if err != nil {
		return "", err
	}

	// makeslice can fail depending on available memory.
	// We arbitrarily limit string size to sane default (~2.2GB).
	if len < 0 || len > math.MaxInt32 {
		return "", fmt.Errorf("string length out of range: %d", len)
	}

	if len == 0 {
		return "", nil
	}

	bb := make([]byte, len)
	_, err = io.ReadFull(r, bb)
	if err != nil {
		return "", err
	}
	return *(*string)(unsafe.Pointer(&bb)), nil
}

func readFixed(r io.Reader, size int) ([]byte, error) {
	bb := make([]byte, size)
	_, err := io.ReadFull(r, bb)
	return bb, err
}
