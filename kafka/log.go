package kafka

import (
	"fmt"
	"time"
)

// LogEvent represent the log from librdkafka internal log queue
type LogEvent struct {
	Name      string
	Fac       string
	Str       string
	Level     int
	Timestamp time.Time
}

func (le LogEvent) String() string {
	return fmt.Sprintf("[%v][%s][%d][%s] %s", le.Timestamp.Format(time.RFC3339), le.Fac, le.Level, le.Name, le.Str)
}
