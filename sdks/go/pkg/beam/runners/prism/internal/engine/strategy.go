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
}

// EarliestCompletion marks when we can close a window.
func (ws WinStrat) EarliestCompletion(w typex.Window) mtime.Time {
	return w.MaxTimestamp().Add(ws.AllowedLateness)
}

func (ws WinStrat) String() string {
	return fmt.Sprintf("WinStrat[AllowedLateness:%v]", ws.AllowedLateness)
}

// triggerInput represents a Key + window + stage's trigger conditions.
type triggerInput struct {
	newElementCount    int  // The number of new elements since the last check.
	endOfWindowReached bool // Whether or not the end of the window has been reached.
}

// Trigger represents a trigger for a windowing strategy.  A trigger determines when
// to fire a window based on the arrival of elements and the passage of time.
//
// See https://s.apache.org/beam-triggers for a more detailed look at triggers.
type Trigger interface {
	// IsReady determines if the trigger is ready to fire based on the current state.
	isReady(input triggerInput, state *StateData) bool
	reset(state *StateData)

	// TODO merging triggers and state for merging windows
}

// triggerState retains additional state for a given trigger execution.
// Each trigger is responsible for maintaining it's own state as needed.
type triggerState struct {
	// finished indicates if the trigger has already fired or not.
	finished bool
	// extra is where additional data can be stored.
	extra any
}

// TriggerNever is never ready.
// There will only be an ON_TIME output and a final output at window expiration.
type TriggerNever struct{}

func (*TriggerNever) isReady(triggerInput, *StateData) bool {
	return false
}

func (t *TriggerNever) reset(state *StateData) {}

// TriggerAlways is always ready.
// There will be an output for every element, and a final output at window expiration.
// Equivalent to TriggerRepeatedly {TriggerElementCount{1}}
type TriggerAlways struct{}

func (*TriggerAlways) isReady(triggerInput, *StateData) bool {
	return true
}

func (t *TriggerAlways) reset(state *StateData) {}

// TriggerAfterAll is ready when all subTriggers are ready.
// There will be an output when all subTriggers are ready.
// Logically, an "AND" trigger.
type TriggerAfterAll struct {
	SubTriggers []Trigger
}

func (t *TriggerAfterAll) isReady(input triggerInput, state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	finishedCount := 0
	for _, t := range t.SubTriggers {
		// Don't re-evaluate.
		if state.getTriggerState(t).finished {
			finishedCount++
			continue
		}
		// If this trigger ever becomes ready, mark it finished, since triggerAfterAll only fires once anyway.
		if t.isReady(input, state) {
			finishedCount++
			sts := state.getTriggerState(t)
			sts.finished = true
			state.setTriggerState(t, sts)
		}
	}
	ready := finishedCount == len(t.SubTriggers)
	if ready {
		ts.finished = true
	}
	state.setTriggerState(t, ts)
	return ready
}

func (t *TriggerAfterAll) reset(state *StateData) {
	for _, sub := range t.SubTriggers {
		sub.reset(state)
	}
	delete(state.Trigger, t)
}

// TriggerAfterAny is ready the first time any of the subTriggers are ready.
// Logically, an "OR" trigger.
type TriggerAfterAny struct {
	SubTriggers []Trigger
}

func (t *TriggerAfterAny) isReady(input triggerInput, state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	anyReady := false
	for _, t := range t.SubTriggers {
		anyReady = t.isReady(input, state)
		if anyReady {
			break
		}
	}
	if anyReady {
		ts.finished = true
		for _, sub := range t.SubTriggers {
			// clear any subtrigger state, as we're fireing anyway.
			sub.reset(state)
		}
	}
	state.setTriggerState(t, ts)
	return anyReady
}

func (t *TriggerAfterAny) reset(state *StateData) {
	for _, sub := range t.SubTriggers {
		sub.reset(state)
	}
	delete(state.Trigger, t)
}

// TriggerAfterEach processes each trigger before executing the next.
// Starting with the first subtrigger, ready when the _current_ subtrigger
// is ready. After output, advances the current trigger by one.
//
// TriggerAfterEach stores the current trigger index in it's extra state field.
type TriggerAfterEach struct {
	SubTriggers []Trigger
}

func (t *TriggerAfterEach) isReady(input triggerInput, state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	// Use extra for the current trigger index.
	if ts.extra == nil {
		ts.extra = int(0)
	}
	current := ts.extra.(int)
	ready := false
	if t.SubTriggers[current].isReady(input, state) {
		current++
		ready = true
	}
	if current >= len(t.SubTriggers) {
		ts.finished = true
	}
	ts.extra = current
	state.setTriggerState(t, ts)
	return ready
}

func (t *TriggerAfterEach) reset(state *StateData) {
	for _, sub := range t.SubTriggers {
		sub.reset(state)
	}
	delete(state.Trigger, t)
}

// TriggerElementCount triggers when there have been at least the required number
// of elements have arrived.
//
// TriggerElementCount stores the current element count in it's extra state field.
type TriggerElementCount struct {
	ElementCount int
}

func (t *TriggerElementCount) isReady(input triggerInput, state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}

	if ts.extra == nil {
		ts.extra = int(0)
	}
	count := ts.extra.(int) + input.newElementCount
	ts.extra = count
	ready := count >= t.ElementCount
	if ready {
		ts.finished = true
	}
	state.setTriggerState(t, ts)
	return ready
}

func (t *TriggerElementCount) reset(state *StateData) {
	delete(state.Trigger, t)
}

// TriggerOrFinally is ready whenever either of it's subtriggers fire.
// Ceases to be ready after the Finally trigger isReady.
type TriggerOrFinally struct {
	Main    Trigger // repeated
	Finally Trigger // terminates execution.
}

func (t *TriggerOrFinally) isReady(input triggerInput, state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}

	mainReady := t.Main.isReady(input, state)
	finallyReady := t.Finally.isReady(input, state)

	if mainReady || finallyReady {
		t.Main.reset(state)
	}

	if finallyReady {
		t.Finally.reset(state)
		ts.finished = true
	}

	state.setTriggerState(t, ts)
	return mainReady || finallyReady
}

func (t *TriggerOrFinally) reset(state *StateData) {
	t.Main.reset(state)
	t.Finally.reset(state)
	delete(state.Trigger, t)
}

// TriggerRepeatedly is a composite trigger that will fire whenever the Repeated trigger is ready.
// If the Repeated trigger is finished, it's state will be reset.
type TriggerRepeatedly struct {
	Repeated Trigger
}

func (t *TriggerRepeatedly) isReady(input triggerInput, state *StateData) bool {
	mainReady := t.Repeated.isReady(input, state)

	// If the subtrigger is finished, reset it.
	if repeatedTs := state.getTriggerState(t.Repeated); repeatedTs.finished {
		t.Repeated.reset(state)
	}
	return mainReady
}

func (t *TriggerRepeatedly) reset(state *StateData) {
	t.Repeated.reset(state)
	delete(state.Trigger, t)
}

// TriggerAfterEndOfWindow is a composite trigger that will fire whenever the
// the early Triggers are ready prior to the end of window, implicitly repeated.
// After the end of window, the Late trigger will be implicitly repeated.
type TriggerAfterEndOfWindow struct {
	Early, Late Trigger
}

func (t *TriggerAfterEndOfWindow) isReady(input triggerInput, state *StateData) bool {
	ts := state.getTriggerState(t)
	if ts.finished {
		return false
	}
	// There are too many places we may exit, use a defer to reset instead of
	// doing it manually.
	defer func() {
		state.setTriggerState(t, ts)
	}()

	ready := false

	if !state.getTriggerState(t.Early).finished {
		// We're still on early firings.
		// This trigger is ready if the early trigger is ready, or the end of window has happened.
		ready = input.endOfWindowReached || t.Early.isReady(input, state)
		if !input.endOfWindowReached {
			// If it's not the end of window then this is an early firing, so
			// reset, if it is finished.
			if state.getTriggerState(t.Early).finished {
				t.Early.reset(state)
			}
			return ready
		}
		// It must be the end of window firing.
		earlyState := state.getTriggerState(t.Early)
		// Set the early trigger to be finished, and *don't* reset it
		// but do nil out the extra state. We need to keep the finished
		// bit around to avoid further early triggers.
		earlyState.finished = true
		earlyState.extra = nil
		state.setTriggerState(t.Early, earlyState)

		if t.Late != nil {
			// Zero out the late trigger as we're about to need it.
			t.Late.reset(state)
		}
		return ready
	} else if t.Late != nil {
		ready = t.Late.isReady(input, state)
		// Repeat forever.
		if state.getTriggerState(t.Late).finished {
			t.Late.reset(state)
		}
	} else {
		ts.finished = true
	}
	return ready
}

func (t *TriggerAfterEndOfWindow) reset(state *StateData) {
	t.Early.reset(state)
	t.Late.reset(state)
	delete(state.Trigger, t)
}

// TODO
// TriggerAfterProcessingTime
// TriggerDefault
