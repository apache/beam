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

package window

import "fmt"

type Trigger struct {
	Kind         string
	SubTriggers  []Trigger
	Delay        int64 // in milliseconds
	ElementCount int32
	EarlyTrigger *Trigger
	LateTrigger  *Trigger
}

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

// TriggerDefault constructs a default trigger that fires once after the end of window.
// Late Data is discarded.
func TriggerDefault() Trigger {
	return Trigger{Kind: DefaultTrigger}
}

// TriggerAlways constructs an always trigger that keeps firing immediately after an element is processed.
// Equivalent to window.TriggerRepeat(window.TriggerAfterCount(1))
func TriggerAlways() Trigger {
	return Trigger{Kind: AlwaysTrigger}
}

// TriggerAfterCount constructs an element count trigger that fires after atleast `count` number of elements are processed.
func TriggerAfterCount(count int32) Trigger {
	return Trigger{Kind: ElementCountTrigger, ElementCount: count}
}

// TriggerAfterProcessingTime constructs an after processing time trigger that fires after 'delay' milliseconds of processing time have passed.
func TriggerAfterProcessingTime(delay int64) Trigger {
	return Trigger{Kind: AfterProcessingTimeTrigger, Delay: delay}
}

// TriggerRepeat constructs a repeat trigger that fires a trigger repeatedly once the condition has been met.
// Ex: window.TriggerRepeat(window.TriggerAfterCount(1)) is same as window.TriggerAlways().
func TriggerRepeat(tr Trigger) Trigger {
	return Trigger{Kind: RepeatTrigger, SubTriggers: []Trigger{tr}}
}

// TriggerAfterEndOfWindow constructs an end of window trigger that is configurable for early firing trigger(before the end of window)
// and late firing trigger(after the end of window).
// Default Options are: Default Trigger for EarlyFiring and No LateFiring. Override it with EarlyFiring and LateFiring methods on this trigger.
func TriggerAfterEndOfWindow() Trigger {
	defaultEarly := TriggerDefault()
	return Trigger{Kind: AfterEndOfWindowTrigger, EarlyTrigger: &defaultEarly, LateTrigger: nil}
}

// EarlyFiring configures AfterEndOfWindow trigger with an early firing trigger.
func (tr Trigger) EarlyFiring(early Trigger) Trigger {
	if tr.Kind != AfterEndOfWindowTrigger {
		panic(fmt.Errorf("can't apply early firing to %s, got: %s, want: AfterEndOfWindowTrigger", tr.Kind, tr.Kind))
	}
	tr.EarlyTrigger = &early
	return tr
}

// LateFiring configures AfterEndOfWindow trigger with a late firing trigger
func (tr Trigger) LateFiring(late Trigger) Trigger {
	if tr.Kind != AfterEndOfWindowTrigger {
		panic(fmt.Errorf("can't apply late firing to %s, got: %s, want: AfterEndOfWindowTrigger", tr.Kind, tr.Kind))
	}
	tr.LateTrigger = &late
	return tr
}
