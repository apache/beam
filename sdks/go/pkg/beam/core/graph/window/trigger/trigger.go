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

// Package trigger helps construct aggregation triggers. It defines the trigger API
// for Go SDK. It is experimental and subject to change.
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

func (t DefaultTrigger) trigger() {}

// Default constructs a default trigger that fires once after the end of window.
// Late Data is discarded.
func Default() *DefaultTrigger {
	return &DefaultTrigger{}
}

// AlwaysTrigger fires immediately after receiving an element.
type AlwaysTrigger struct{}

func (t AlwaysTrigger) trigger() {}

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

func (t AfterCountTrigger) trigger() {}

// ElementCount returns the elementCount.
func (t *AfterCountTrigger) ElementCount() int32 {
	return t.elementCount
}

// AfterCount constructs a trigger that fires after
// at least `count` number of elements are processed.
func AfterCount(count int32) *AfterCountTrigger {
	return &AfterCountTrigger{elementCount: count}
}

// AfterProcessingTimeTrigger fires after passage of times defined in timestampTransforms.
type AfterProcessingTimeTrigger struct {
	timestampTransforms []TimestampTransform
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
	return &RepeatTrigger{subtrigger: t}
}

// AfterEndOfWindowTrigger provides option to set triggers for early and late firing.
type AfterEndOfWindowTrigger struct {
	earlyFiring Trigger
	lateFiring  Trigger
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
// NYI(BEAM-3304). Intended for framework use only.
type AfterAnyTrigger struct {
	subtriggers []Trigger
}

func (t AfterAnyTrigger) trigger() {}

// SubTriggers returns the component triggers.
func (t *AfterAnyTrigger) SubTriggers() []Trigger {
	return t.subtriggers
}

// AfterAllTrigger fires after all subtriggers are fired.
// NYI(BEAM-3304). Intended for framework use only.
type AfterAllTrigger struct {
	subtriggers []Trigger
}

func (t AfterAllTrigger) trigger() {}

// SubTriggers returns the component triggers.
func (t *AfterAllTrigger) SubTriggers() []Trigger {
	return t.subtriggers
}

// OrFinallyTrigger serves as final condition to cause any trigger to fire.
// NYI(BEAM-3304). Intended for framework use only.
type OrFinallyTrigger struct{}

func (t OrFinallyTrigger) trigger() {}

// NeverTrigger is never ready to fire.
// NYI(BEAM-3304). Intended for framework use only.
type NeverTrigger struct{}

func (t NeverTrigger) trigger() {}

// AfterSynchronizedProcessingTimeTrigger fires when processing time synchronises with arrival time.
// NYI(BEAM-3304). Intended for framework use only.
type AfterSynchronizedProcessingTimeTrigger struct{}

func (t AfterSynchronizedProcessingTimeTrigger) trigger() {}
