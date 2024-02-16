// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// We define our own element wrapper and similar to avoid depending on the protos within the
// engine package. This improves compile times, and readability of this package.

// TestStreamHandler manages TestStreamEvents for the ElementManager.
//
// TestStreams are a pipeline root like an Impulse. They kick off computation, and
// strictly manage Watermark advancements.
//
// A given pipeline can only have a single TestStream due to test streams
// requiring a single source of truth for Relative Processing Time advancements
// and ordering emissions of Elements.
// All operations with testStreamHandler are expected to be in the element manager's
// refresh lock critical section.
type testStreamHandler struct {
	ID string

	nextEventIndex int
	events         []tsEvent
	// Initialzed with normal "time.Now", so this does change by relative nature.
	processingTime time.Time // Override for the processing time clock, for triggers and ProcessContinuations.

	tagState map[string]tagState // Map from event tag to related outputs.

	completed bool // indicates that no further test stream events exist, and all watermarks are advanced to infinity. Used to send the final event, once.
}

func makeTestStreamHandler(id string) *testStreamHandler {
	return &testStreamHandler{
		ID:       id,
		tagState: map[string]tagState{},
	}
}

// tagState tracks state for a given tag.
type tagState struct {
	watermark   mtime.Time // Current Watermark for this tag.
	pcollection string     // ID for the pcollection of this tag to look up consumers.
}

// Now represents the overridden ProcessingTime, which is only advanced when directed by an event.
// Overrides the elementManager "clock".
func (ts *testStreamHandler) Now() time.Time {
	return ts.processingTime
}

// TagsToPCollections recieves the map of local output tags to global pcollection ids.
func (ts *testStreamHandler) TagsToPCollections(tagToPcol map[string]string) {
	for tag, pcol := range tagToPcol {
		ts.tagState[tag] = tagState{
			watermark:   mtime.MinTimestamp,
			pcollection: pcol,
		}
		// If there is only one output pcollection, duplicate initial state to the
		// empty tag string.
		if len(tagToPcol) == 1 {
			ts.tagState[""] = ts.tagState[tag]
		}
	}
}

// AddElementEvent adds an element event to the test stream event queue.
func (ts *testStreamHandler) AddElementEvent(tag string, elements []TestStreamElement) {
	ts.events = append(ts.events, tsElementEvent{
		Tag:      tag,
		Elements: elements,
	})
}

// AddWatermarkEvent adds a watermark event to the test stream event queue.
func (ts *testStreamHandler) AddWatermarkEvent(tag string, newWatermark mtime.Time) {
	ts.events = append(ts.events, tsWatermarkEvent{
		Tag:          tag,
		NewWatermark: newWatermark,
	})
}

// AddProcessingTimeEvent adds a processing time event to the test stream event queue.
func (ts *testStreamHandler) AddProcessingTimeEvent(d time.Duration) {
	ts.events = append(ts.events, tsProcessingTimeEvent{
		AdvanceBy: d,
	})
}

// NextEvent returns the next event.
// If there are no more events, returns nil.
func (ts *testStreamHandler) NextEvent() tsEvent {
	if ts == nil {
		return nil
	}
	if ts.nextEventIndex >= len(ts.events) {
		if !ts.completed {
			ts.completed = true
			return tsFinalEvent{stageID: ts.ID}
		}
		return nil
	}
	ev := ts.events[ts.nextEventIndex]
	ts.nextEventIndex++
	return ev
}

// TestStreamElement wraps the provided bytes and timestamp for ingestion and use.
type TestStreamElement struct {
	Encoded   []byte
	EventTime mtime.Time
}

// tsEvent abstracts over the different TestStream Event kinds so we can keep
// them in the same queue.
type tsEvent interface {
	// Execute the associated event on this element manager.
	Execute(*ElementManager)
}

// tsElementEvent implements an element event, inserting additional elements
// to be pending for consuming stages.
type tsElementEvent struct {
	Tag      string
	Elements []TestStreamElement
}

// Execute this ElementEvent by routing pending element to their consuming stages.
func (ev tsElementEvent) Execute(em *ElementManager) {
	t := em.testStreamHandler.tagState[ev.Tag]

	var pending []element
	for _, e := range ev.Elements {
		pending = append(pending, element{
			window:    window.GlobalWindow{},
			timestamp: e.EventTime,
			elmBytes:  e.Encoded,
			pane:      typex.NoFiringPane(),
		})
	}

	// Update the consuming state.
	for _, sID := range em.consumers[t.pcollection] {
		ss := em.stages[sID]
		added := ss.AddPending(pending)
		em.addPending(added)
		em.watermarkRefreshes.insert(sID)
	}

	for _, link := range em.sideConsumers[t.pcollection] {
		ss := em.stages[link.Global]
		ss.AddPendingSide(pending, link.Transform, link.Local)
		em.watermarkRefreshes.insert(link.Global)
	}
}

// tsWatermarkEvent sets the watermark for the new stage.
type tsWatermarkEvent struct {
	Tag          string
	NewWatermark mtime.Time
}

// Execute this WatermarkEvent by updating the watermark for the tag, and notify affected downstream stages.
func (ev tsWatermarkEvent) Execute(em *ElementManager) {
	t := em.testStreamHandler.tagState[ev.Tag]

	if ev.NewWatermark < t.watermark {
		panic("test stream event decreases watermark. Watermarks cannot go backwards.")
	}
	t.watermark = ev.NewWatermark
	em.testStreamHandler.tagState[ev.Tag] = t

	// Update the upstream watermarks in the consumers.
	for _, sID := range em.consumers[t.pcollection] {
		ss := em.stages[sID]
		ss.updateUpstreamWatermark(ss.inputID, t.watermark)
		em.watermarkRefreshes.insert(sID)
	}
}

// tsProcessingTimeEvent implements advancing the synthetic processing time.
type tsProcessingTimeEvent struct {
	AdvanceBy time.Duration
}

// Execute this ProcessingTime event by advancing the synthetic processing time.
func (ev tsProcessingTimeEvent) Execute(em *ElementManager) {
	em.testStreamHandler.processingTime = em.testStreamHandler.processingTime.Add(ev.AdvanceBy)
}

// tsFinalEvent is the "last" event we perform after all preceeding events.
// It's automatically inserted once the user defined events have all been executed.
// It updates the upstream watermarks for all consumers to infinity.
type tsFinalEvent struct {
	stageID string
}

func (ev tsFinalEvent) Execute(em *ElementManager) {
	em.addPending(1) // We subtrack a pending after event execution, so add one now.
	ss := em.stages[ev.stageID]
	kickSet := ss.updateWatermarks(em)
	kickSet.insert(ev.stageID)
	em.watermarkRefreshes.merge(kickSet)
}

// TestStreamBuilder builds a synthetic sequence of events for the engine to execute.
// A pipeline may only have a single TestStream and may panic.
type TestStreamBuilder interface {
	AddElementEvent(tag string, elements []TestStreamElement)
	AddWatermarkEvent(tag string, newWatermark mtime.Time)
	AddProcessingTimeEvent(d time.Duration)
}

type testStreamImpl struct {
	em *ElementManager
}

var (
	_ TestStreamBuilder = (*testStreamImpl)(nil)
	_ TestStreamBuilder = (*testStreamHandler)(nil)
)

func (tsi *testStreamImpl) initHandler(id string) {
	if tsi.em.testStreamHandler == nil {
		tsi.em.testStreamHandler = makeTestStreamHandler(id)
	}
}

// TagsToPCollections recieves the map of local output tags to global pcollection ids.
func (tsi *testStreamImpl) TagsToPCollections(tagToPcol map[string]string) {
	tsi.em.testStreamHandler.TagsToPCollections(tagToPcol)
}

// AddElementEvent adds an element event to the test stream event queue.
func (tsi *testStreamImpl) AddElementEvent(tag string, elements []TestStreamElement) {
	tsi.em.testStreamHandler.AddElementEvent(tag, elements)
	tsi.em.addPending(1)
}

// AddWatermarkEvent adds a watermark event to the test stream event queue.
func (tsi *testStreamImpl) AddWatermarkEvent(tag string, newWatermark mtime.Time) {
	tsi.em.testStreamHandler.AddWatermarkEvent(tag, newWatermark)
	tsi.em.addPending(1)
}

// AddProcessingTimeEvent adds a processing time event to the test stream event queue.
func (tsi *testStreamImpl) AddProcessingTimeEvent(d time.Duration) {
	tsi.em.testStreamHandler.AddProcessingTimeEvent(d)
	tsi.em.addPending(1)
}
