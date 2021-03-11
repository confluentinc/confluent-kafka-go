package soaktest

/**
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/shirou/gopsutil/process"
	"os"
	"sync"
	"syscall"
	"time"
)

const datadogHost = "127.0.0.1:8125"
const ddPfx = "kafka.client.soak.go."

var lastRusage syscall.Rusage
var lastRusageTime time.Time

const memoryRss = "memory.rss."
const cpuUser = "cpu.user"
const cpuSystem = "cpu.system"
const memoryRssMax = "memory.rss.max"

var client, err = statsd.New(datadogHost)
var proc, _ = process.NewProcess(int32(os.Getpid()))

// DatadogIncrement submits increment type metrics
func DatadogIncrement(metricsName string, incrval float64, tags []string) {
	IncreTags := []string{"environment:dev"}
	if tags != nil {
		IncreTags = tags
	}
	client.Incr(ddPfx+metricsName, IncreTags, incrval)
}

// DatadogGauge submits gauge type metrics
func DatadogGauge(metricsName string,
	val float64,
	tags []string) {
	gaugeTags := []string{"environment:dev"}
	if tags != nil {
		gaugeTags = tags
	}
	client.Gauge(ddPfx+metricsName, val, gaugeTags, 1)
}

// calcRusageDeltas calculates user CPU usage, system CPU usage
// and max rss memory
func calcRusageDeltas(metricsName string,
	curr, prev syscall.Rusage,
	elapsed float64) {
	// User CPU %
	userCPU := ((float64)(curr.Utime.Sec - prev.Utime.Sec)) / elapsed * 100.0
	DatadogGauge(metricsName+cpuUser, userCPU, nil)

	//System CPU %
	sysCPU := ((float64)(curr.Stime.Sec - prev.Stime.Sec)) / elapsed * 100.0
	DatadogGauge(metricsName+cpuSystem, sysCPU, nil)

	//Max RSS memory (monotonic)
	maxRss := float64(curr.Maxrss) / 1024.0
	DatadogGauge(metricsName+memoryRssMax, maxRss, nil)

	InfoLogger.Printf("User CPU: %f, System CPU: %f, MaxRSS %f MiB\n",
		userCPU, sysCPU, maxRss)

}

// GetRusageMetrics is the entrance for resource calculation
// If caught a terminate signal, return, else call the GetRusage() function
// to calculate resource usage every 10 seconds
func GetRusageMetrics(metricsName string,
	wg *sync.WaitGroup,
	doneChan chan bool,
	errorChan chan error) {
	defer wg.Done()
	ticker := time.NewTicker(10000 * time.Millisecond)
	run := true
	for run {
		select {
		case <-doneChan:
			run = false
		case <-ticker.C:
			err := GetRusage(metricsName)
			if err != nil {
				ErrorLogger.Printf("Failed to get resource usage %s\n", err)
				errorChan <- err
				return
			}
		}
	}
}

// GetRusage calculates RSS memory usage
func GetRusage(metricsName string) error {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		ErrorLogger.Printf("Error: unable to gather resource usage data: %s\n",
			err)
		return err
	}

	now := time.Now()
	if !lastRusageTime.IsZero() {
		calcRusageDeltas(metricsName,
			ru,
			lastRusage,
			float64(now.Sub(lastRusageTime)/time.Millisecond))
	}
	lastRusage = ru
	lastRusageTime = now

	// Current RSS memory
	memoryInfo, err := proc.MemoryInfo()
	if err != nil {
		ErrorLogger.Printf("Error: unable to gather memory info: %s\n", err)
		return err
	}
	rss := float64(memoryInfo.RSS) / (1024.0 * 1024.0)
	DatadogGauge(memoryRss+metricsName, rss, nil)
	return nil
}
