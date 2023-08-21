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

// Package trigger helps construct aggregation triggers with beam.WindowInto.
package trigger

import (
	"fmt"
	"time"
)

// Trigger describes when to emit new aggregations.
type Trigger interface {
	trigger()
}

// DefaultTrigger fires once after the end of window. Late Data is discarded.
type DefaultTrigger struct{}

// String implements the Stringer interface and returns trigger details as a string.
func (t DefaultTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t DefaultTrigger) trigger() {}

// Default constructs a default trigger that fires once after the end of window.
// Late Data is discarded.
func Default() *DefaultTrigger {
	return &DefaultTrigger{}
}

// AlwaysTrigger fires immediately after receiving an element.
type AlwaysTrigger struct{}

func (t AlwaysTrigger) trigger() {}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AlwaysTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

// Always constructs a trigger that fires immediately
// whenever an element is received.
//
// Equivalent to trigger.Repeat(trigger.AfterCount(1))
func Always() *AlwaysTrigger {
	return &AlwaysTrigger{}
}

// AfterCountTrigger fires after receiving elementCount elements.
type AfterCountTrigger struct {
	elementCount int32
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AfterCountTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t AfterCountTrigger) trigger() {}

// ElementCount returns the elementCount.
func (t *AfterCountTrigger) ElementCount() int32 {
	return t.elementCount
}

// AfterCount constructs a trigger that fires after
// at least `count` number of elements are processed.
func AfterCount(count int32) *AfterCountTrigger {
	if count < 1 {
		panic(fmt.Errorf("trigger.AfterCount(%v) must be a positive integer", count))
	}
	return &AfterCountTrigger{elementCount: count}
}

// AfterProcessingTimeTrigger fires after passage of times defined in timestampTransforms.
type AfterProcessingTimeTrigger struct {
	timestampTransforms []TimestampTransform
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AfterProcessingTimeTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t AfterProcessingTimeTrigger) trigger() {}

// TimestampTransforms returns the timestampTransforms.
func (t *AfterProcessingTimeTrigger) TimestampTransforms() []TimestampTransform {
	return t.timestampTransforms
}

// AfterProcessingTime constructs a trigger that fires relative to
// when input first arrives.
//
// Must be configured with calls to PlusDelay, or AlignedTo. May be
// configured with additional delay.
func AfterProcessingTime() *AfterProcessingTimeTrigger {
	return &AfterProcessingTimeTrigger{}
}

// TimestampTransform describes how an after processing time trigger
// time is transformed to determine when to fire an aggregation.const
// The base timestamp is always the when the first element of the pane
// is received.
//
// A series of these transforms will be applied in order emit at regular intervals.
type TimestampTransform interface {
	timestampTransform()
}

// DelayTransform takes the timestamp and adds the given delay to it.
type DelayTransform struct {
	Delay int64 // in milliseconds
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *DelayTransform) String() string {
	return fmt.Sprintf("%#v", t)
}

func (DelayTransform) timestampTransform() {}

// AlignToTransform takes the timestamp and transforms it to the lowest
// multiple of the period starting from the offset.
//
// Eg. A period of 20 with an offset of 45 would have alignments at 5,25,45,65 etc.
// Timestamps would be transformed as follows: 0 to 5 would be transformed to 5,
// 6 to 25 would be transformed to 25, 26 to 45 would be transformed to 45, and so on.
type AlignToTransform struct {
	Period, Offset int64 // in milliseconds
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AlignToTransform) String() string {
	return fmt.Sprintf("%#v", t)
}

func (AlignToTransform) timestampTransform() {}

// PlusDelay configures an AfterProcessingTime trigger to fire after a specified delay,
// no smaller than a millisecond.
func (t *AfterProcessingTimeTrigger) PlusDelay(delay time.Duration) *AfterProcessingTimeTrigger {
	if delay < time.Millisecond {
		panic(fmt.Errorf("can't apply processing delay of less than a millisecond. Got: %v", delay))
	}
	t.timestampTransforms = append(t.timestampTransforms, DelayTransform{Delay: int64(delay / time.Millisecond)})
	return t
}

// AlignedTo configures an AfterProcessingTime trigger to fire
// at the smallest multiple of period since the offset greater than the first element timestamp.
//
// * Period may not be smaller than a millisecond.
// * Offset may be a zero time (time.Time{}).
func (t *AfterProcessingTimeTrigger) AlignedTo(period time.Duration, offset time.Time) *AfterProcessingTimeTrigger {
	if period < time.Millisecond {
		panic(fmt.Errorf("can't apply an alignment period of less than a millisecond. Got: %v", period))
	}
	offsetMillis := int64(0)
	if !offset.IsZero() {
		// TODO: Change to call UnixMilli() once we move to only supporting a go version > 1.17.
		offsetMillis = offset.Unix()*1e3 + int64(offset.Nanosecond())/1e6
	}
	t.timestampTransforms = append(t.timestampTransforms, AlignToTransform{
		Period: int64(period / time.Millisecond),
		Offset: offsetMillis,
	})
	return t
}

// RepeatTrigger fires a sub-trigger repeatedly.
type RepeatTrigger struct {
	subtrigger Trigger
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *RepeatTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t RepeatTrigger) trigger() {}

// SubTrigger returns the trigger to be repeated.
func (t *RepeatTrigger) SubTrigger() Trigger {
	return t.subtrigger
}

// Repeat constructs a trigger that fires a trigger repeatedly
// once the condition has been met.
//
// Ex: trigger.Repeat(trigger.AfterCount(1)) is same as trigger.Always().
func Repeat(t Trigger) *RepeatTrigger {
	if t == nil {
		panic("trigger argument to trigger.Repeat() cannot be nil")
	}
	return &RepeatTrigger{subtrigger: t}
}

// AfterEndOfWindowTrigger provides option to set triggers for early and late firing.
type AfterEndOfWindowTrigger struct {
	earlyFiring Trigger
	lateFiring  Trigger
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AfterEndOfWindowTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t AfterEndOfWindowTrigger) trigger() {}

// Early returns the Early Firing trigger for AfterEndOfWindowTrigger.
func (t *AfterEndOfWindowTrigger) Early() Trigger {
	return t.earlyFiring
}

// Late returns the Late Firing trigger for AfterEndOfWindowTrigger.
func (t *AfterEndOfWindowTrigger) Late() Trigger {
	return t.lateFiring
}

// AfterEndOfWindow constructs a trigger that is configurable for early firing
// (before the end of window) and late firing (after the end of window).
//
// Must call EarlyFiring or LateFiring method on this trigger at the time of setting.
func AfterEndOfWindow() *AfterEndOfWindowTrigger {
	defaultEarly := Default()
	return &AfterEndOfWindowTrigger{earlyFiring: defaultEarly, lateFiring: nil}
}

// EarlyFiring configures an AfterEndOfWindow trigger with an implicitly
// repeated trigger that applies before the end of the window.
func (t *AfterEndOfWindowTrigger) EarlyFiring(early Trigger) *AfterEndOfWindowTrigger {
	t.earlyFiring = early
	return t
}

// LateFiring configures an AfterEndOfWindow trigger with an implicitly
// repeated trigger that applies after the end of the window.
//
// Not setting a late firing trigger means elements are discarded.
func (t *AfterEndOfWindowTrigger) LateFiring(late Trigger) *AfterEndOfWindowTrigger {
	t.lateFiring = late
	return t
}

// AfterAnyTrigger fires after any of sub-trigger fires.
type AfterAnyTrigger struct {
	subtriggers []Trigger
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AfterAnyTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t AfterAnyTrigger) trigger() {}

// SubTriggers returns the component triggers.
func (t *AfterAnyTrigger) SubTriggers() []Trigger {
	return t.subtriggers
}

// AfterAny returns a new AfterAny trigger with subtriggers set to passed argument.
func AfterAny(triggers []Trigger) *AfterAnyTrigger {
	if len(triggers) <= 1 {
		panic("empty slice passed as an argument to trigger.AfterAny()")
	}
	return &AfterAnyTrigger{subtriggers: triggers}
}

// AfterAllTrigger fires after all subtriggers are fired.
type AfterAllTrigger struct {
	subtriggers []Trigger
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AfterAllTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t AfterAllTrigger) trigger() {}

// SubTriggers returns the component triggers.
func (t *AfterAllTrigger) SubTriggers() []Trigger {
	return t.subtriggers
}

// AfterAll returns a new AfterAll trigger with subtriggers set to the passed argument.
func AfterAll(triggers []Trigger) *AfterAllTrigger {
	if len(triggers) <= 1 {
		panic(fmt.Sprintf("number of subtriggers to trigger.AfterAll() should be greater than 1, got: %v", len(triggers)))
	}
	return &AfterAllTrigger{subtriggers: triggers}
}

// OrFinallyTrigger is ready whenever either of its subtriggers are ready, but finishes output
// when the finally subtrigger fires.
type OrFinallyTrigger struct {
	main    Trigger // Trigger governing main output; may fire repeatedly.
	finally Trigger // Trigger governing termination of output.
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *OrFinallyTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t OrFinallyTrigger) trigger() {}

// OrFinally trigger has main trigger which may fire repeatedly and the finally trigger.
// Output is produced when the finally trigger fires.
func OrFinally(main, finally Trigger) *OrFinallyTrigger {
	if main == nil || finally == nil {
		panic("main and finally trigger arguments to trigger.OrFinally() cannot be nil")
	}
	return &OrFinallyTrigger{main: main, finally: finally}
}

// Main returns the main trigger of OrFinallyTrigger.
func (t *OrFinallyTrigger) Main() Trigger {
	return t.main
}

// Finally returns the finally trigger of OrFinallyTrigger.
func (t *OrFinallyTrigger) Finally() Trigger {
	return t.finally
}

// NeverTrigger is never ready to fire.
type NeverTrigger struct{}

// String implements the Stringer interface and returns trigger details as a string.
func (t *NeverTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t NeverTrigger) trigger() {}

// Never creates a Never Trigger that is never ready to fire.
// There will only be an ON_TIME output and a final output at window expiration.
func Never() *NeverTrigger {
	return &NeverTrigger{}
}

// AfterSynchronizedProcessingTimeTrigger fires when processing time synchronizes with arrival time.
type AfterSynchronizedProcessingTimeTrigger struct{}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AfterSynchronizedProcessingTimeTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}

func (t AfterSynchronizedProcessingTimeTrigger) trigger() {}

// AfterSynchronizedProcessingTime creates a new AfterSynchronizedProcessingTime trigger that fires when
// processing time synchronizes with arrival time.
func AfterSynchronizedProcessingTime() *AfterSynchronizedProcessingTimeTrigger {
	return &AfterSynchronizedProcessingTimeTrigger{}
}

// AfterEachTrigger fires when each trigger is ready. Order of triggers matters.
type AfterEachTrigger struct {
	subtriggers []Trigger
}

func (t AfterEachTrigger) trigger() {}

// AfterEach creates a new AfterEach trigger that fires after each trigger is ready. It follows the order
// of triggers passed in as arguments. Let's say if the second trigger gets ready but the first one is not ready
// then it won't be fired until first triggered is ready and fired.
func AfterEach(subtriggers []Trigger) *AfterEachTrigger {
	return &AfterEachTrigger{subtriggers: subtriggers}
}

// Subtriggers returns the list of subtriggers for the current AfterEach trigger.
func (t *AfterEachTrigger) Subtriggers() []Trigger {
	return t.subtriggers
}

// String implements the Stringer interface and returns trigger details as a string.
func (t *AfterEachTrigger) String() string {
	return fmt.Sprintf("%#v", t)
}
