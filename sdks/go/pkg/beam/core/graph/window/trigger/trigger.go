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

package trigger

import (
	"fmt"
	"time"
)

// Trigger describes when to emit new aggregations.
// Fields are exported for use by the framework, and not intended
// to be set by end users.
//
// This API is experimental and subject to change.
type Trigger struct {
	Kind                string
	SubTriggers         []Trigger            // Repeat, OrFinally, Any, All
	TimestampTransforms []TimestampTransform // AfterProcessingTime
	ElementCount        int32                // ElementCount
	EarlyTrigger        *Trigger             // AfterEndOfWindow
	LateTrigger         *Trigger             // AfterEndOfWindow
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

const (
	DefaultTrigger                         string = "Trigger_Default_"
	AlwaysTrigger                          string = "Trigger_Always_"
	AfterAnyTrigger                        string = "Trigger_AfterAny_"
	AfterAllTrigger                        string = "Trigger_AfterAll_"
	AfterProcessingTimeTrigger             string = "Trigger_AfterProcessing_Time_"
	ElementCountTrigger                    string = "Trigger_ElementCount_"
	AfterEndOfWindowTrigger                string = "Trigger_AfterEndOfWindow_"
	RepeatTrigger                          string = "Trigger_Repeat_"
	OrFinallyTrigger                       string = "Trigger_OrFinally_"
	NeverTrigger                           string = "Trigger_Never_"
	AfterSynchronizedProcessingTimeTrigger string = "Trigger_AfterSynchronizedProcessingTime_"
)

// Default constructs a default trigger that fires once after the end of window.
// Late Data is discarded.
func Default() Trigger {
	return Trigger{Kind: DefaultTrigger}
}

// Always constructs a trigger that fires immediately
// whenever an element is received.
//
// Equivalent to window.Repeat(window.AfterCount(1))
func Always() Trigger {
	return Trigger{Kind: AlwaysTrigger}
}

// AfterCount constructs a trigger that fires after
// at least `count` number of elements are processed.
func AfterCount(count int32) Trigger {
	return Trigger{Kind: ElementCountTrigger, ElementCount: count}
}

// AfterProcessingTime constructs a trigger that fires relative to
// when input first arrives.
//
// Must be configured with calls to PlusDelay, or AlignedTo. May be
// configured with additional delay.
func AfterProcessingTime() Trigger {
	return Trigger{Kind: AfterProcessingTimeTrigger}
}

// PlusDelay configures an AfterProcessingTime trigger to fire after a specified delay,
// no smaller than a millisecond.
func (tr Trigger) PlusDelay(delay time.Duration) Trigger {
	if tr.Kind != AfterProcessingTimeTrigger {
		panic(fmt.Errorf("can't apply processing delay to %s, want: AfterProcessingTimeTrigger", tr.Kind))
	}
	if delay < time.Millisecond {
		panic(fmt.Errorf("can't apply processing delay of less than a millisecond. Got: %v", delay))
	}
	tr.TimestampTransforms = append(tr.TimestampTransforms, DelayTransform{Delay: int64(delay / time.Millisecond)})
	return tr
}

// AlignedTo configures an AfterProcessingTime trigger to fire
// at the smallest multiple of period since the offset greater than the first element timestamp.
//
// * Period may not be smaller than a millisecond.
// * Offset may be a zero time (time.Time{}).
func (tr Trigger) AlignedTo(period time.Duration, offset time.Time) Trigger {
	if tr.Kind != AfterProcessingTimeTrigger {
		panic(fmt.Errorf("can't apply processing delay to %s, want: AfterProcessingTimeTrigger", tr.Kind))
	}
	if period < time.Millisecond {
		panic(fmt.Errorf("can't apply an alignment period of less than a millisecond. Got: %v", period))
	}
	offsetMillis := int64(0)
	if !offset.IsZero() {
		// TODO: Change to call UnixMilli() once we move to only supporting a go version > 1.17.
		offsetMillis = offset.Unix()*1e3 + int64(offset.Nanosecond())/1e6
	}
	tr.TimestampTransforms = append(tr.TimestampTransforms, AlignToTransform{
		Period: int64(period / time.Millisecond),
		Offset: offsetMillis,
	})
	return tr
}

// Repeat constructs a trigger that fires a trigger repeatedly
// once the condition has been met.
//
// Ex: window.Repeat(window.AfterCount(1)) is same as window.Always().
func Repeat(tr Trigger) Trigger {
	return Trigger{Kind: RepeatTrigger, SubTriggers: []Trigger{tr}}
}

// AfterEndOfWindow constructs a trigger that is configurable for early firing
// (before the end of window) and late firing (after the end of window).
//
// Default Options are: Default Trigger for EarlyFiring and No LateFiring.
// Override it with EarlyFiring and LateFiring methods on this trigger.
func AfterEndOfWindow() Trigger {
	defaultEarly := Default()
	return Trigger{Kind: AfterEndOfWindowTrigger, EarlyTrigger: &defaultEarly, LateTrigger: nil}
}

// EarlyFiring configures an AfterEndOfWindow trigger with an implicitly
// repeated trigger that applies before the end of the window.
func (tr Trigger) EarlyFiring(early Trigger) Trigger {
	if tr.Kind != AfterEndOfWindowTrigger {
		panic(fmt.Errorf("can't apply early firing to %s, want: AfterEndOfWindowTrigger", tr.Kind))
	}
	tr.EarlyTrigger = &early
	return tr
}

// LateFiring configures an AfterEndOfWindow trigger with an implicitly
// repeated trigger that applies after the end of the window.
//
// Not setting a late firing trigger means elements are discarded.
func (tr Trigger) LateFiring(late Trigger) Trigger {
	if tr.Kind != AfterEndOfWindowTrigger {
		panic(fmt.Errorf("can't apply late firing to %s, want: AfterEndOfWindowTrigger", tr.Kind))
	}
	tr.LateTrigger = &late
	return tr
}
