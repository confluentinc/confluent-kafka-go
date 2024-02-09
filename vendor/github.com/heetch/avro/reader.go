package avro

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const bufSize = 512

// read reads n bytes from the buffer and returns
// them. The returned slice is only valid until
// the next call to any of the reader methods.
// n must be less than or equal to cap(d.buf).
func (d *decoder) read(n int) []byte {
	if d.fill(n) < n {
		d.error(io.ErrUnexpectedEOF)
	}
	buf := d.buf[d.scan : d.scan+n]
	d.scan += n
	return buf
}

func (d *decoder) fill(n int) int {
	if len(d.buf)-d.scan >= n {
		return n
	}
	if d.readErr != nil {
		// If there's an error, there's no point in doing
		// anything more. This is also crucial to avoid
		// corrupting the buffer when it has been provided by a
		// caller.
		return len(d.buf) - d.scan
	}
	// Slide any remaining bytes to the
	// start of the buffer.
	total := copy(d.buf, d.buf[d.scan:])
	d.scan = 0
	d.buf = d.buf[:cap(d.buf)]
	for total < n {
		if d.readErr != nil {
			if d.readErr == io.EOF {
				d.buf = d.buf[:total]
				return total
			}
			d.error(d.readErr)
		}
		nr, err := d.r.Read(d.buf[total:])
		if err != nil {
			d.readErr = err
		}
		total += nr
	}
	d.buf = d.buf[:total]
	return n
}

func (d *decoder) readBool() bool {
	return d.read(1)[0] != 0
}

func (d *decoder) readDouble() float64 {
	bits := binary.LittleEndian.Uint64(d.read(8))
	return math.Float64frombits(bits)
}

func (d *decoder) readFloat() float64 {
	bits := binary.LittleEndian.Uint32(d.read(4))
	return float64(math.Float32frombits(bits))
}

func (d *decoder) readBytes() []byte {
	// TODO bounds-check readLong result.
	// https://github.com/heetch/avro/issues/33
	size := d.readLong()
	// Make a temporary buffer for the bytes, limiting the size to
	// an arbitrary sane default (~2.2GB).
	if size < 0 || size > math.MaxInt32 {
		d.error(fmt.Errorf("length out of range: %d", size))
	}
	return d.readFixed(int(size))
}

func (d *decoder) readFixed(size int) []byte {
	if size < cap(d.buf) {
		// The bytes will fit in the buffer we already
		// have, so use that.
		return d.read(size)
	}
	buf := make([]byte, size)
	n := copy(buf, d.buf[d.scan:])
	_, err := io.ReadFull(d.r, buf[n:])
	if err != nil {
		d.error(err)
	}
	d.scan = len(d.buf)
	return buf
}

func (d *decoder) readLong() int64 {
	// Note: d.fill doesn't mind if we get less
	// than the required number of bytes.
	d.fill(binary.MaxVarintLen64)
	x, nr := binary.Varint(d.buf[d.scan:])
	switch {
	case nr > 0:
		d.scan += nr
		return x
	case nr == 0:
		d.error(io.ErrUnexpectedEOF)
	default:
		d.error(fmt.Errorf("integer too large"))
	}
	panic("unreachable")
}

func (d *decoder) readString() string {
	return string(d.readBytes())
}
