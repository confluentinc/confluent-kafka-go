/**
 * Copyright 2020 Confluent Inc.
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

package main

// Transactions example

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Set to false to disable visualization, useful for troubleshooting.
var withVisualizer = true

// input and output topics
var inputTopic = "go-transactions-example-ingress-cars"
var outputTopic = "go-transactions-example-traffic-light-states"

// brokers holds the bootstrap servers
var brokers string

// logsChan is the common log channel for all Kafka client instances.
var logsChan chan kafka.LogEvent

// fatal resets the terminal and exits the application with the given message (arg).
func fatal(arg interface{}) {
	if withVisualizer {
		resetTerminal()
		panic(arg)
	}
}

func logReader(wg *sync.WaitGroup, termChan chan bool) {
	defer wg.Done()

	for {
		select {
		case logEvent := <-logsChan:
			addLog(logEvent.String())
		case <-termChan:
			return
		}
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker>\n", os.Args[0])
		os.Exit(1)
	}

	brokers = os.Args[1]

	rand.Seed(time.Now().Unix())

	logsChan = make(chan kafka.LogEvent, 100000)

	var wg sync.WaitGroup
	var termChan chan bool

	if !withVisualizer {
		termChan = make(chan bool)
	} else {
		wg.Add(1)
		termChan = initVisualizer(&wg)
	}

	wg.Add(1)
	go logReader(&wg, termChan)

	wg.Add(1)
	go generateInputMessages(&wg, termChan)

	wg.Add(1)
	go trafficLightProcessor(&wg, termChan)

	if !withVisualizer {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		close(termChan)
	} else {
		wg.Add(1)
		go trafficLightVisualizer(&wg, termChan)
		<-termChan
	}

	// Wait for all go-routines to finish
	wg.Wait()

	close(logsChan)
}
