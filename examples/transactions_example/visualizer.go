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

// This is the visualiser of intersection states, drawing
// frame with each intersection observed in the output topic, displaying
// the current number of cars and traffic light color per road.

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gdamore/tcell"
	"os"
	"sort"
	"sync"
	"time"
)

// Height and width (terminal characters) per intersection frame.
const heightPerIntersection = 20
const widthPerInersection = 39

var screen tcell.Screen

// drawPos identifies an absolute drawing coordinate.
//
// x=0,y=0 is top left of terminal.
type drawPos struct {
	x int
	y int
}

const showLogCnt = 20

var collectedLogs [showLogCnt]string
var logIndex int
var logPos drawPos
var logMutex sync.Mutex

// drawText draws the text string at the given position.
func drawText(s tcell.Screen, pos drawPos, text string, clearPad bool) {
	w, _ := s.Size()
	maxLen := w - pos.x
	if maxLen < 0 {
		return
	} else if maxLen < len(text) {
		text = text[0:maxLen]
	}

	style := tcell.StyleDefault
	for i, ch := range text {
		s.SetContent(pos.x+i, pos.y, ch, nil, style)
	}

	for i := len(text); clearPad && i < maxLen; i++ {
		s.SetContent(pos.x+i, pos.y, ' ', nil, style)
	}
}

// drawLogs draws the collected logs.
func drawLogs(s tcell.Screen) {
	logMutex.Lock()
	defer logMutex.Unlock()

	startIndex := (logIndex + 1) % len(collectedLogs)
	endIndex := logIndex

	y := 0
	for i := startIndex; ; i = (i + 1) % len(collectedLogs) {
		log := collectedLogs[i]
		if len(log) > 0 {
			drawText(s, drawPos{0, logPos.y + y}, log, true)
			y++
		}

		if i == endIndex {
			break
		}
	}
}

// addLog appends a log line to the cyclical log buffer which is
// drawn at the bottom of the screen.
func addLog(log string) {
	if screen != nil {
		logMutex.Lock()
		logIndex = (logIndex + 1) % len(collectedLogs)
		collectedLogs[logIndex] = log
		logMutex.Unlock()
	} else {
		fmt.Fprintf(os.Stderr, "%s\n", log)
	}
}

// drawFrame draws a frame outline between pos1.x,pos1.y
// and pos2.x,pos2.y with a name in the top frame.
func drawFrame(s tcell.Screen, pos1 drawPos, pos2 drawPos, name string) {
	maxNameLen := pos2.x - pos1.x - 6
	if len(name) > maxNameLen {
		name = name[0 : pos2.x-pos1.x-6]
	}

	style := tcell.StyleDefault
	for r := pos1.y; r < pos2.y; r++ {
		s.SetContent(pos1.x, r, '|', nil, style)
		s.SetContent(pos2.x, r, '|', nil, style)
		if r == pos1.y || r == pos2.y-1 {
			for c := pos1.x + 1; c < pos2.x; c++ {
				s.SetContent(c, r, '=', nil, style)
			}
		} else {
			// Clear contents of frame
			for c := pos1.x + 1; c < pos2.x; c++ {
				s.SetContent(c, r, ' ', nil, style)
			}
		}
	}

	drawText(s, drawPos{pos1.x + 2, pos1.y}, name, false)
}

// drawRoads draws the roads in the intersection and returns the
// light and eligible car positions keyed by road.
func drawRoads(s tcell.Screen, pos drawPos) (lightPos map[string]drawPos,
	lanes map[string][]drawPos) {

	layout := `
            |n| |
            |n  |
            |n| |
            |n  |
          | |n| |
          N |n  | E--
 ___________|||||___________
 _ _ _ _ _ _     _eeeeeeeeee
 wwwwwwwwww_     ___________
            ||||| S
        --W |  s| |
            | |s|
            |  s|
            | |s|
            |  s|
            | |s|
`

	lightPos = make(map[string]drawPos)
	lanes = make(map[string][]drawPos)
	pos.x += 3
	pos.y++
	style := tcell.StyleDefault
	x := 0
	y := 0
	for _, ch := range layout {
		if ch == '\n' {
			x = 0
			y++
			continue
		} else if ch == ' ' {
			x++
			continue
		} else if ch == 'N' {
			lightPos["north"] = drawPos{pos.x + x, pos.y + y}
		} else if ch == 'E' {
			lightPos["east"] = drawPos{pos.x + x, pos.y + y}
		} else if ch == 'S' {
			lightPos["south"] = drawPos{pos.x + x, pos.y + y}
		} else if ch == 'W' {
			lightPos["west"] = drawPos{pos.x + x, pos.y + y}
		} else if ch == 'n' {
			// prepend
			lanes["north"] = append([]drawPos{{pos.x + x, pos.y + y}}, lanes["north"]...)
			x++
			continue
		} else if ch == 'e' {
			lanes["east"] = append(lanes["east"], drawPos{pos.x + x, pos.y + y})
			if (x % 2) == 1 {
				ch = '_' // mid stripes
			} else {
				x++
				continue
			}
		} else if ch == 's' {
			lanes["south"] = append(lanes["south"], drawPos{pos.x + x, pos.y + y})
			x++
			continue
		} else if ch == 'w' {
			// prepend
			lanes["west"] = append([]drawPos{{pos.x + x, pos.y + y}}, lanes["west"]...)
			if (x % 1) == 0 {
				ch = '_' // side stripes
			} else {
				x++
				continue
			}
		}

		s.SetContent(pos.x+x, pos.y+y, ch, nil, style)
		x++
	}

	return lightPos, lanes
}

// drawLight draws the color of a traffic light.
func drawLight(s tcell.Screen, pos drawPos, color tcell.Color) {
	style := tcell.StyleDefault.Background(color)

	s.SetContent(pos.x, pos.y, 'o', nil, style)
}

// drawCars draws cars queuing up on an ingress road.
func drawCars(s tcell.Screen, road string, lane []drawPos, cnt int) {
	carChar := map[string]rune{
		"north": 'v',
		"east":  '<',
		"south": '^',
		"west":  '>',
	}
	ch := carChar[road]
	style := tcell.StyleDefault.Background(tcell.GetColor("blue"))
	for i := 0; i < cnt && i < len(lane); i++ {
		s.SetContent(lane[i].x, lane[i].y, ch, nil, style)
	}
}

// drawIntersection draws a single intersection.
func drawIntersection(isect intersectionStateMsg, s tcell.Screen, pos drawPos, id int) {

	drawFrame(s, pos, drawPos{pos.x + widthPerInersection, pos.y + heightPerIntersection}, isect.Name)
	lightPos, lanes := drawRoads(s, drawPos{pos.x + 2, pos.y})

	for _, lstate := range isect.Lights {
		color := tcell.GetColor(lstate.State)

		drawLight(s, lightPos[lstate.Road], color)
		drawCars(s, lstate.Road, lanes[lstate.Road], lstate.CarsWaiting)
	}
}

// drawIntersections draws all intersections in isectStates
// and the log footer.
func drawIntersections(isectStates map[string]intersectionStateMsg) {

	if screen == nil {
		return
	}

	s := screen
	w, h := s.Size()
	footerHeight := showLogCnt

	totalIntersectionCnt := len(isectStates)

	// Need some space for logs in the footer.
	if h <= footerHeight {
		fatal("Terminal is too small")
	}
	h -= footerHeight
	logPos = drawPos{0, h}

	// w/h per intersection, figure out how many intersections we
	// can show.
	iPerRow := w / widthPerInersection
	maxRows := h / heightPerIntersection
	if iPerRow < 1 || maxRows < 1 {
		fatal("Terminal is too small to visualize any intersections: reise your terminal window")
	}

	iCnt := iPerRow * maxRows
	if iCnt < totalIntersectionCnt {
		fmt.Fprintf(os.Stderr, "Warning: Terminal window is too small to show all intersections: %d/%d intersections shown\n", iCnt, totalIntersectionCnt)
	} else if iCnt > totalIntersectionCnt {
		iCnt = totalIntersectionCnt
	}

	names := []string{}
	for name := range isectStates {
		names = append(names, name)
	}

	sort.Strings(names)

	i := 0
	for _, name := range names {
		lstate := isectStates[name]
		if i >= iCnt {
			break
		}
		drawIntersection(lstate, s, drawPos{
			widthPerInersection * (i % iPerRow),
			heightPerIntersection * (i / iPerRow),
		}, i)
		i++
	}

	if len(names) == 0 {
		drawText(s, drawPos{0, 0}, "  Waiting for intersection state...", false)
		drawText(s, drawPos{0, 2}, "  Press Escape to quit", false)
	}

	drawLogs(s)
	s.Show()
}

// intersectionVisualizer monitors the output topic and visualizes the
// intersection traffic light states.
func trafficLightVisualizer(wg *sync.WaitGroup, termChan chan bool) {
	defer wg.Done()

	doTerm := false
	ticker := time.NewTicker(500 * time.Millisecond)

	// Create a consumer that consumes traffic light states
	// from the output topic and renders the latest state every
	// ticker interval.

	consumerConfig := &kafka.ConfigMap{
		"client.id":              "visualizer",
		"bootstrap.servers":      brokers,
		"group.id":               processorGroupID + "_visualizer",
		"auto.offset.reset":      "earliest",
		"go.logs.channel.enable": true,
		"go.logs.channel":        logsChan,
	}

	var err error
	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		fatal(err)
	}

	err = consumer.Subscribe(outputTopic, nil)
	if err != nil {
		fatal(err)
	}

	isectStates := make(map[string]intersectionStateMsg)

	for !doTerm {
		select {
		case <-ticker.C:
			drawIntersections(isectStates)

		case <-termChan:
			doTerm = true

		default:
			timeoutMs := 500 * time.Millisecond
			for {
				// Read as many messages as possible,
				// blocking on the first read.
				msg, err := consumer.ReadMessage(timeoutMs)
				timeoutMs = 0

				if err != nil {
					if err.(kafka.Error).Code() != kafka.ErrTimedOut {
						addLog(fmt.Sprintf("Visualizer: failed to read message: %s", err))
					}
					break
				} else if msg.Value == nil {
					// Empty message, ignore
					break
				}

				var isectMsg intersectionStateMsg
				err = json.Unmarshal(msg.Value, &isectMsg)
				if err != nil {
					addLog(fmt.Sprintf("Visualizer: failed to deserialize message at %s: %s: ignoring", msg.TopicPartition, err))
					continue
				}

				isectStates[isectMsg.Name] = isectMsg
			}
		}
	}
}

// resetTerminal resets the terminal prior to exiting
func resetTerminal() {
	if screen == nil {
		return
	}

	screen.Clear()
	screen.Sync()
	screen.Fini()
	fmt.Printf("\n")
}

// initVisualizer sets up the terminal for visualization and
// returns the termination channel other go-routines should listen to
// for knowing when to terminate.
func initVisualizer(wg *sync.WaitGroup) (termChan chan bool) {

	tcell.SetEncodingFallback(tcell.EncodingFallbackASCII)
	s, err := tcell.NewScreen()
	if err != nil {
		fatal(err)
	}
	if err = s.Init(); err != nil {
		fatal(err)
	}

	s.SetStyle(tcell.StyleDefault.
		Foreground(tcell.ColorWhite).
		Background(tcell.ColorBlack))
	s.Clear()

	screen = s

	termChan = make(chan bool)

	go func() {
		defer wg.Done()

		for {
			doClear := false

			ev := s.PollEvent()
			switch ev := ev.(type) {
			case *tcell.EventKey:
				switch ev.Key() {
				case tcell.KeyEscape, tcell.KeyCtrlC:
					resetTerminal()
					close(termChan)
					return
				case tcell.KeyCtrlL:
					doClear = true
				}
			case *tcell.EventResize:
				doClear = true
			}

			if doClear {
				s.Clear()
				s.Fill(' ', tcell.StyleDefault)
				s.Sync()
			}

		}
	}()

	return termChan
}
