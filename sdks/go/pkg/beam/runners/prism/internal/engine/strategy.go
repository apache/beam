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
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// This file is intended to consolidate handling of WindowingStrategy and trigger
// logic independantly from the bulk of the ElementManager.
//
// Triggers by their nature only apply to Aggregation transforms, in particular:
// GroupByKeys and CoGroupByKeys are aggregations.
// Triggers also affect downstream side inputs. That is, a side input consumer
// is vaccuously an aggregation.
//
// Triggers are PerKey+PerWindow, and they may or may not use state per trigger.
//
// The unique state per trigger is the trickiest bit to handle. In principle
// it could just be handled by the existing state system, which might be
// sufficient, but it's not prepared for unique tagging per trigger itself.
// It would also add additional overhead since state is kept as the serialized
// bytes, instead of in a manipulatable form.
//
// Instead, each key+window state cell contains a trigger specific state map,
// handled via pointer equality from the trigger itself.

// WinStrat configures the windowing strategy for the stage, based on the
// stage's input PCollection.
type WinStrat struct {
	AllowedLateness time.Duration // Used to extend duration
	Accumulating    bool          // If true, elements remain pending until the last firing.

	Trigger Trigger // Evaluated during execution.
}

// IsTriggerReady updates the trigger state with the given input, and returns
// if the trigger is ready to fire.
func (ws WinStrat) IsTriggerReady(input triggerInput, state *StateData) bool {
	ws.Trigger.onElement(input, state)

	if ws.Trigger.shouldFire(state) {
		ws.Trigger.onFire(state)
		return true
	}
	return false
}

// EarliestCompletion marks when we can close a window.
func (ws WinStrat) EarliestCompletion(w typex.Window) mtime.Time {
	return w.MaxTimestamp().Add(ws.AllowedLateness)
}

func (ws WinStrat) IsNeverTrigger() bool {
	_, ok := ws.Trigger.(*TriggerNever)
	return ok
}

func (ws WinStrat) String() string {
	return fmt.Sprintf("WinStrat[AllowedLateness:%v Trigger:%v]", ws.AllowedLateness, ws.Trigger)
}

// triggerInput represents a Key + window + stage's trigger conditions.
type triggerInput struct {
	newElementCount    int        // The number of new elements since the last check.
	endOfWindowReached bool       // Whether or not the end of the window has been reached.
	emNow              mtime.Time // The current processing time in the runner.
}

// Trigger represents a trigger for a windowing strategy.  A trigger determines when
// to fire a window based on the arrival of elements and the passage of time.
//
// See https://s.apache.org/beam-triggers for a more detailed look at triggers.
type Trigger interface {
	reset(state *StateData)

	// onElement updates the trigger state based on the provided input. This may
	// transition triggers into a fireable state, but will never make them "finished".
	onElement(input triggerInput, state *StateData)
	// shouldFire returns whether the trigger is able to fire or not.
	shouldFire(state *StateData) bool
	// onFire commits that the trigger has fired, so triggers may transition to
	// a finished state.
	onFire(state *StateData)

	// TODO handle https://github.com/apache/beam/issues/31438 merging triggers and state for merging windows (sessions, but also custom merging windows)
}

// triggerState retains additional state for a given trigger execution.
// Each trigger is responsible for maintaining it's own state as needed.
type triggerState struct {
	// finished indicates if the trigger has already fired or not.
	finished bool
	// extra is where additional data can be stored.
	extra any
}

func (ts triggerState) String() string {
	return fmt.Sprintf("triggerState[finished: %v; state: %v]", ts.finished, ts.extra)
}

// nullTrigger is a 0 size object that exists to be embedded in triggers that
// perform no action on trigger method calls. Triggers with this embedded will
// gain an implementation of the trigger methods that do nothing, and behavior
// must be overridden by the trigger for correct evaluation.
type nullTrigger struct{}

func (nullTrigger) onElement(triggerInput, *StateData) {}
func (nullTrigger) onFire(*StateData)                  {}
func (nullTrigger) reset(*StateData)                   {}

// TriggerNever is never ready.
// There will only be an ON_TIME output and a final output at window expiration.
type TriggerNever struct{ nullTrigger }

func (*TriggerNever) shouldFire(*StateData) bool {
	return false
}

func (t *TriggerNever) reset(state *StateData) {}

func (t *TriggerNever) String() string {
	return "Never"
}

// TriggerAlways is always ready.
// There will be an output for every element, and a final output at window expiration.
// Equivalent to TriggerRepeatedly {TriggerElementCount{1}}
type TriggerAlways struct{ nullTrigger }

func (*TriggerAlways) shouldFire(*StateData) bool {
	return true
}

func subTriggersOnElement(t Trigger, input triggerInput, state *StateData, subTriggers []Trigger) {
	ts := state.getTriggerState(t)
	if ts.finished {
		return
	}

	for _, sub := range subTriggers {
		sub.onElement(input, state)
	}
}

func subTriggersReset(t Trigger, state *StateData, subTriggers []Trigger) {
	for _, sub := range subTriggers {
		sub.reset(state)
	}
	delete(state.Trigger, t)
}

func triggerClearAndFinish(t Trigger, state *StateData) {
	t.reset(state)
	ts := state.getTriggerState(t)
	ts.finished = true
	state.setTriggerState(t, ts)
}

func (t *TriggerAlways) String() string {
	return "Always"
}

// TriggerAfterAll is ready when all subTriggers are ready.
// There will be an output when all subTriggers are ready.
// Logically, an "AND" trigger.
type TriggerAfterAll struct {
	SubTriggers []Trigger
}

func (t *TriggerAfterAll) onElement(input triggerInput, state *StateData) {
	subTriggersOnElement(t, input, state, t.SubTriggers)
}

func (t *TriggerAfterAll) shouldFire(state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	shouldFire := true
	for _, sub := range t.SubTriggers {
		shouldFire = shouldFire && sub.shouldFire(state)
	}
	return shouldFire
}

func (t *TriggerAfterAll) onFire(state *StateData) {
	unfinished := false
	for _, sub := range t.SubTriggers {
		if sub.shouldFire(state) {
			sub.onFire(state)
		}
		if !state.getTriggerState(sub).finished {
			unfinished = true
		}
	}
	if unfinished {
		return
	}
	triggerClearAndFinish(t, state)
}

func (t *TriggerAfterAll) reset(state *StateData) {
	subTriggersReset(t, state, t.SubTriggers)
}

func (t *TriggerAfterAll) String() string {
	return fmt.Sprintf("AfterAll[%v]", t.SubTriggers)
}

// TriggerAfterAny is ready the first time any of the subTriggers are ready.
// Logically, an "OR" trigger.
type TriggerAfterAny struct {
	SubTriggers []Trigger
}

func (t *TriggerAfterAny) onElement(input triggerInput, state *StateData) {
	subTriggersOnElement(t, input, state, t.SubTriggers)
}

func (t *TriggerAfterAny) shouldFire(state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	for _, sub := range t.SubTriggers {
		if sub.shouldFire(state) {
			return true
		}
	}
	return false
}

func (t *TriggerAfterAny) onFire(state *StateData) {
	if !t.shouldFire(state) {
		return
	}
	triggerClearAndFinish(t, state)
}

func (t *TriggerAfterAny) reset(state *StateData) {
	subTriggersReset(t, state, t.SubTriggers)
}

func (t *TriggerAfterAny) String() string {
	return fmt.Sprintf("AfterAny[%v]", t.SubTriggers)
}

// TriggerAfterEach processes each trigger before executing the next.
// Starting with the first subtrigger, ready when the _current_ subtrigger
// is ready. After output, advances the current trigger by one.
type TriggerAfterEach struct {
	SubTriggers []Trigger
}

func (t *TriggerAfterEach) onElement(input triggerInput, state *StateData) {
	ts := state.getTriggerState(t)
	if ts.finished {
		return
	}
	// Only update the first unfinished sub trigger.
	for _, sub := range t.SubTriggers {
		if state.getTriggerState(sub).finished {
			continue
		}
		sub.onElement(input, state)
		return
	}
}

func (t *TriggerAfterEach) shouldFire(state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	for _, sub := range t.SubTriggers {
		if state.getTriggerState(sub).finished {
			continue
		}
		return sub.shouldFire(state)
	}
	return false
}

func (t *TriggerAfterEach) onFire(state *StateData) {
	if !t.shouldFire(state) {
		return
	}
	for _, sub := range t.SubTriggers {
		if state.getTriggerState(sub).finished {
			continue
		}
		sub.onFire(state)
		if !state.getTriggerState(sub).finished {
			return
		}
	}
	triggerClearAndFinish(t, state)
}

func (t *TriggerAfterEach) reset(state *StateData) {
	subTriggersReset(t, state, t.SubTriggers)
}

func (t *TriggerAfterEach) String() string {
	return fmt.Sprintf("AfterEach[%v]", t.SubTriggers)
}

// TriggerElementCount triggers when there have been at least the required number
// of elements have arrived.
//
// TriggerElementCount stores the current element count in it's extra state field.
type TriggerElementCount struct {
	ElementCount int
}

func (t *TriggerElementCount) onElement(input triggerInput, state *StateData) {
	ts := state.getTriggerState(t)
	if ts.finished {
		return
	}

	if ts.extra == nil {
		ts.extra = int(0)
	}
	count := ts.extra.(int) + input.newElementCount
	ts.extra = count
	state.setTriggerState(t, ts)
}

func (t *TriggerElementCount) shouldFire(state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	if ts.extra == nil {
		return false
	}
	return ts.extra.(int) >= t.ElementCount
}

func (t *TriggerElementCount) onFire(state *StateData) {
	if !t.shouldFire(state) {
		return
	}
	ts := state.getTriggerState(t)
	ts.finished = true
	ts.extra = nil
	state.setTriggerState(t, ts)
}

func (t *TriggerElementCount) reset(state *StateData) {
	delete(state.Trigger, t)
}

func (t *TriggerElementCount) String() string {
	return fmt.Sprintf("ElementCount[%v]", t.ElementCount)
}

// TriggerOrFinally is ready whenever either of it's subtriggers fire.
// Ceases to be ready after the Finally trigger shouldFire.
type TriggerOrFinally struct {
	Main    Trigger // repeated
	Finally Trigger // terminates execution.
}

func (t *TriggerOrFinally) onElement(input triggerInput, state *StateData) {
	ts := state.getTriggerState(t)
	if ts.finished {
		return
	}
	t.Main.onElement(input, state)
	t.Finally.onElement(input, state)
}

func (t *TriggerOrFinally) shouldFire(state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	return t.Main.shouldFire(state) || t.Finally.shouldFire(state)
}

func (t *TriggerOrFinally) onFire(state *StateData) {
	if !t.shouldFire(state) {
		return
	}
	if t.Finally.shouldFire(state) {
		t.Finally.onFire(state)
		ts := state.getTriggerState(t)
		ts.finished = true
		state.setTriggerState(t, ts)
	} else {
		t.Main.onFire(state)
		if state.getTriggerState(t.Main).finished {
			t.Main.reset(state)
		}
	}
}

func (t *TriggerOrFinally) reset(state *StateData) {
	t.Main.reset(state)
	t.Finally.reset(state)
	delete(state.Trigger, t)
}

func (t *TriggerOrFinally) String() string {
	return fmt.Sprintf("OrFinally[Repeat:%v Until:%v]", t.Main, t.Finally)
}

// TriggerRepeatedly is a composite trigger that will fire whenever the Repeated trigger is ready.
// If the Repeated trigger is finished, it's state will be reset.
type TriggerRepeatedly struct {
	Repeated Trigger
}

func (t *TriggerRepeatedly) onElement(input triggerInput, state *StateData) {
	t.Repeated.onElement(input, state)
}

func (t *TriggerRepeatedly) shouldFire(state *StateData) bool {
	return t.Repeated.shouldFire(state)
}

func (t *TriggerRepeatedly) onFire(state *StateData) {
	if !t.shouldFire(state) {
		return
	}
	t.Repeated.onFire(state)
	// If the subtrigger is finished, reset it.
	if repeatedTs := state.getTriggerState(t.Repeated); repeatedTs.finished {
		t.Repeated.reset(state)
	}
}

func (t *TriggerRepeatedly) reset(state *StateData) {
	t.Repeated.reset(state)
	delete(state.Trigger, t)
}

func (t *TriggerRepeatedly) String() string {
	return fmt.Sprintf("Repeat[%v]", t.Repeated)
}

// TriggerAfterEndOfWindow is a composite trigger that will fire whenever the
// the early Triggers are ready prior to the end of window, implicitly repeated.
// After the end of window, the Late trigger will be implicitly repeated.
//
// Uses the extra state field to track if the end of the window has been reached.
type TriggerAfterEndOfWindow struct {
	Early, Late Trigger
}

func (t *TriggerAfterEndOfWindow) onElement(input triggerInput, state *StateData) {
	ts := state.getTriggerState(t)
	if ts.finished {
		return
	}
	if ts.extra == nil {
		ts.extra = false
	}
	previouslyEndOfWindow := ts.extra.(bool)
	if !previouslyEndOfWindow && input.endOfWindowReached {
		// We have transitioned. Clear early state and mark it finished
		if t.Early != nil {
			triggerClearAndFinish(t.Early, state)
		}
		if t.Late == nil {
			triggerClearAndFinish(t, state)
			return
		}
	}
	ts.extra = input.endOfWindowReached
	state.setTriggerState(t, ts)

	if t.Early != nil && !state.getTriggerState(t.Early).finished {
		t.Early.onElement(input, state)
		return
	} else if t.Late != nil && input.endOfWindowReached {
		t.Late.onElement(input, state)
	}
}

func (t *TriggerAfterEndOfWindow) shouldFire(state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	if t.Early != nil && !state.getTriggerState(t.Early).finished {
		return t.Early.shouldFire(state) || ts.extra.(bool)
	} else if t.Late != nil && ts.extra.(bool) {
		return t.Late.shouldFire(state)
	}
	return false
}

func (t *TriggerAfterEndOfWindow) onFire(state *StateData) {
	ts := state.getTriggerState(t)
	if ts.finished {
		return
	}
	if t.Early != nil && !state.getTriggerState(t.Early).finished {
		if t.Early.shouldFire(state) {
			t.Early.onFire(state)
			if state.getTriggerState(t.Early).finished {
				t.Early.reset(state)
			}
		}
	} else if t.Late == nil {
		return
	} else if ts.extra.(bool) { // If we're in late firings.
		t.Late.onFire(state)
		if state.getTriggerState(t.Late).finished {
			t.Late.reset(state)
		}
	}
}

func (t *TriggerAfterEndOfWindow) reset(state *StateData) {
	if t.Early != nil {
		t.Early.reset(state)
	}
	if t.Late != nil {
		t.Late.reset(state)
	}
	delete(state.Trigger, t)
}

func (t *TriggerAfterEndOfWindow) String() string {
	return fmt.Sprintf("AfterEndOfWindow[Early: %v Late: %v]", t.Early, t.Late)
}

// TriggerDefault fires once the window ends, but then fires per element
// received for late data. Equivalent to AfterEndOfWindow{Late: Always{}}
//
// Uses the extra state field to track if the end of the window has been reached.
type TriggerDefault struct{}

func (t *TriggerDefault) reset(state *StateData) {
	delete(state.Trigger, t)
}

func (t *TriggerDefault) onElement(input triggerInput, state *StateData) {
	ts := state.getTriggerState(t)
	ts.extra = input.endOfWindowReached
	state.setTriggerState(t, ts)
}

func (t *TriggerDefault) shouldFire(state *StateData) bool {
	ts := state.getTriggerState(t)
	return ts.extra.(bool)
}

func (t *TriggerDefault) onFire(*StateData) {}

func (t *TriggerDefault) String() string {
	return "Default"
}

// TimestampTransform is the engine's representation of a processing time transform.
type TimestampTransform struct {
	Delay         time.Duration
	AlignToPeriod time.Duration
	AlignToOffset time.Duration
}

// TriggerAfterProcessingTime fires once after a specified amount of processing time
// has passed since an element was first seen.
// Uses the extra state field to track the processing time of the first element.
type TriggerAfterProcessingTime struct {
	Transforms []TimestampTransform
}

type afterProcessingTimeState struct {
	emNow              mtime.Time
	firingTime         mtime.Time
	endOfWindowReached bool
}

func (t *TriggerAfterProcessingTime) onElement(input triggerInput, state *StateData) {
	ts := state.getTriggerState(t)
	if ts.finished {
		return
	}

	if ts.extra == nil {
		ts.extra = afterProcessingTimeState{
			emNow:              input.emNow,
			firingTime:         t.applyTimestampTransforms(input.emNow),
			endOfWindowReached: input.endOfWindowReached,
		}
	} else {
		s, _ := ts.extra.(afterProcessingTimeState)
		s.emNow = input.emNow
		s.endOfWindowReached = input.endOfWindowReached
		ts.extra = s
	}

	state.setTriggerState(t, ts)
}

func (t *TriggerAfterProcessingTime) applyTimestampTransforms(start mtime.Time) mtime.Time {
	ret := start
	for _, transform := range t.Transforms {
		ret = ret + mtime.Time(transform.Delay/time.Millisecond)
		if transform.AlignToPeriod > 0 {
			// timestamp - (timestamp % period) + period
			// And with an offset, we adjust before and after.
			tsMs := ret
			periodMs := mtime.Time(transform.AlignToPeriod / time.Millisecond)
			offsetMs := mtime.Time(transform.AlignToOffset / time.Millisecond)

			adjustedMs := tsMs - offsetMs
			alignedMs := adjustedMs - (adjustedMs % periodMs) + periodMs + offsetMs
			ret = alignedMs
		}
	}
	return ret
}

func (t *TriggerAfterProcessingTime) shouldFire(state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.extra == nil || ts.finished {
		return false
	}
	s := ts.extra.(afterProcessingTimeState)
	return s.emNow >= s.firingTime
}

func (t *TriggerAfterProcessingTime) onFire(state *StateData) {
	ts := state.getTriggerState(t)
	if ts.finished {
		return
	}

	// We don't reset the state here, only mark it as finished
	ts.finished = true
	state.setTriggerState(t, ts)
}

func (t *TriggerAfterProcessingTime) reset(state *StateData) {
	ts := state.getTriggerState(t)
	if ts.extra != nil {
		if ts.extra.(afterProcessingTimeState).endOfWindowReached {
			delete(state.Trigger, t)
			return
		}
	}

	// Not reaching the end of window yet.
	// We keep the state (especially the next possible firing time) in case the trigger is called again
	ts.finished = false
	s := ts.extra.(afterProcessingTimeState)
	s.firingTime = t.applyTimestampTransforms(s.firingTime) // compute next possible firing time
	ts.extra = s
	state.setTriggerState(t, ts)
}

func (t *TriggerAfterProcessingTime) String() string {
	return fmt.Sprintf("AfterProcessingTime[%v]", t.Transforms)
}
