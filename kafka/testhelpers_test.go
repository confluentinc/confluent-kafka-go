package kafka

import (
	"testing"
	"time"
)

// ratepdisp tracks and prints message & byte rates
type ratedisp struct {
	name      string
	start     time.Time
	lastPrint time.Time
	every     float64
	cnt       int64
	size      int64
	b         *testing.B
}

// ratedisp_start sets up a new rate displayer
func ratedispStart(b *testing.B, name string, every float64) (pf ratedisp) {
	now := time.Now()
	return ratedisp{name: name, start: now, lastPrint: now, b: b, every: every}
}

// reset start time and counters
func (rd *ratedisp) reset() {
	rd.start = time.Now()
	rd.cnt = 0
	rd.size = 0
}

// print the current (accumulated) rate
func (rd *ratedisp) print(pfx string) {
	elapsed := time.Since(rd.start).Seconds()

	rd.b.Logf("%s: %s%d messages in %fs (%.0f msgs/s), %d bytes (%.3fMb/s)",
		rd.name, pfx, rd.cnt, elapsed, float64(rd.cnt)/elapsed,
		rd.size, (float64(rd.size)/elapsed)/(1024*1024))
}

// tick adds cnt of total size size to the rate displayer and also prints
// running stats every 1s.
func (rd *ratedisp) tick(cnt, size int64) {
	rd.cnt += cnt
	rd.size += size

	if time.Since(rd.lastPrint).Seconds() >= rd.every {
		rd.print("")
		rd.lastPrint = time.Now()
	}
}
