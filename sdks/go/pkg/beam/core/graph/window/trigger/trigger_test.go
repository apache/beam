// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package trigger

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestAfterCountTrigger(t *testing.T) {
	tr := AfterCount(1)
	want := int32(1)
	if got := tr.ElementCount(); got != want {
		t.Errorf("element count not configured correctly. got %v, want %v", got, want)
	}
}

func TestAfterProcessingTimeTrigger(t *testing.T) {
	tests := []struct {
		tr *AfterProcessingTimeTrigger
		tt []TimestampTransform
	}{
		{
			tr: AfterProcessingTime().PlusDelay(time.Millisecond),
			tt: []TimestampTransform{DelayTransform{Delay: 1}},
		},
		{
			tr: AfterProcessingTime().PlusDelay(time.Millisecond).AlignedTo(time.Millisecond, time.Time{}),
			tt: []TimestampTransform{DelayTransform{Delay: 1}, AlignToTransform{Period: 1, Offset: 0}},
		},
	}
	for _, test := range tests {
		if diff := cmp.Diff(test.tr.TimestampTransforms(), test.tt); diff != "" {
			t.Errorf("timestamp transforms are not equal: %v", diff)
		}
	}
}

func TestRepeatTrigger(t *testing.T) {
	subTr := AfterCount(2)
	tr := Repeat(subTr)

	if got, ok := tr.SubTrigger().(*AfterCountTrigger); ok && got != subTr {
		t.Errorf("subtrigger not configured correctly. got %v, want %v", got, subTr)
	}
}
func TestRepeatTrigger_SubTrigger(t1 *testing.T) {
	subTr := AfterCount(2)
	t := &RepeatTrigger{subtrigger: subTr}
	if got, want := t.SubTrigger(), subTr; !reflect.DeepEqual(got, want) {
		t1.Errorf("SubTrigger() = %v, want %v", got, want)
	}
}

func TestAfterEndOfWindowTrigger(t *testing.T) {
	earlyTr := AfterCount(50)
	lateTr := Always()
	tr := AfterEndOfWindow().EarlyFiring(earlyTr).LateFiring(lateTr)

	if got, ok := tr.Early().(*AfterCountTrigger); ok && got != earlyTr {
		t.Errorf("early firing trigger not configured correctly. got %v, want %v", got, earlyTr)
	}
	if got, ok := tr.Late().(*AlwaysTrigger); ok && got != lateTr {
		t.Errorf("late firing trigger not configured correctly. got %v, want %v", got, lateTr)
	}
}

func TestAfterAll(t *testing.T) {
	args := []Trigger{AfterCount(int32(10)), AfterProcessingTime()}
	want := &AfterAllTrigger{subtriggers: args}
	if got := AfterAll(args); !reflect.DeepEqual(got, want) {
		t.Errorf("AfterAll() = %v, want %v", got, want)
	}
}

func TestAfterAllTrigger_SubTriggers(t1 *testing.T) {
	args := []Trigger{AfterCount(int32(10)), AfterProcessingTime()}
	t := &AfterAllTrigger{subtriggers: args}

	if got, want := t.SubTriggers(), args; !reflect.DeepEqual(got, want) {
		t1.Errorf("SubTriggers() = %v, want %v", got, want)
	}
}

func TestAfterAny(t *testing.T) {
	args := []Trigger{AfterCount(int32(10)), AfterProcessingTime()}
	want := &AfterAnyTrigger{subtriggers: args}

	if got := AfterAny(args); !reflect.DeepEqual(got, want) {
		t.Errorf("AfterAny() = %v, want %v", got, want)
	}
}

func TestAfterAnyTrigger_SubTriggers(t1 *testing.T) {
	args := []Trigger{AfterCount(int32(10)), AfterProcessingTime()}

	t := &AfterAnyTrigger{subtriggers: args}
	if got, want := t.SubTriggers(), args; !reflect.DeepEqual(got, want) {
		t1.Errorf("SubTriggers() = %v, want %v", got, want)
	}
}

func TestAfterEach(t *testing.T) {
	args := []Trigger{AfterCount(int32(10)), AfterProcessingTime()}
	want := &AfterEachTrigger{subtriggers: args}

	if got := AfterEach(args); !reflect.DeepEqual(got, want) {
		t.Errorf("AfterEach() = %v, want %v", got, want)
	}
}

func TestAfterEachTrigger_Subtriggers(t1 *testing.T) {
	args := []Trigger{AfterCount(int32(10)), AfterProcessingTime()}

	t := &AfterAnyTrigger{subtriggers: args}
	if got, want := t.SubTriggers(), args; !reflect.DeepEqual(got, want) {
		t1.Errorf("SubTriggers() = %v, want %v", got, want)
	}
}

func TestAfterEndOfWindowTrigger_Early(t1 *testing.T) {
	early := AfterCount(int32(50))
	t := &AfterEndOfWindowTrigger{
		earlyFiring: early,
		lateFiring:  Always(),
	}
	if got, want := t.Early(), early; !reflect.DeepEqual(got, want) {
		t1.Errorf("Early() = %v, want %v", got, want)
	}
}

func TestAfterEndOfWindowTrigger_EarlyFiring(t1 *testing.T) {
	early := AfterCount(int32(50))
	t := &AfterEndOfWindowTrigger{
		earlyFiring: Default(),
		lateFiring:  Always(),
	}
	want := &AfterEndOfWindowTrigger{
		earlyFiring: early,
		lateFiring:  Always(),
	}
	if got := t.EarlyFiring(early); !reflect.DeepEqual(got, want) {
		t1.Errorf("EarlyFiring() = %v, want %v", got, want)
	}
}

func TestAfterEndOfWindowTrigger_Late(t1 *testing.T) {
	late := Always()
	t := &AfterEndOfWindowTrigger{
		earlyFiring: AfterCount(int32(50)),
		lateFiring:  late,
	}

	if got, want := t.Late(), late; !reflect.DeepEqual(got, want) {
		t1.Errorf("Late() = %v, want %v", got, want)
	}
}

func TestAfterEndOfWindowTrigger_LateFiring(t1 *testing.T) {
	late := Always()
	t := &AfterEndOfWindowTrigger{
		earlyFiring: AfterCount(int32(50)),
		lateFiring:  Default(),
	}
	want := &AfterEndOfWindowTrigger{
		earlyFiring: AfterCount(int32(50)),
		lateFiring:  late,
	}

	if got := t.LateFiring(late); !reflect.DeepEqual(got, want) {
		t1.Errorf("LateFiring() = %v, want %v", got, want)
	}
}

func TestAfterProcessingTimeTrigger_AlignedTo(t1 *testing.T) {
	t := &AfterProcessingTimeTrigger{}
	period, offset := int64(1), int64(0)
	want := &AfterProcessingTimeTrigger{
		timestampTransforms: []TimestampTransform{AlignToTransform{Period: period, Offset: offset}},
	}
	if got := t.AlignedTo(time.Millisecond, time.Time{}); !reflect.DeepEqual(got, want) {
		t1.Errorf("AlignedTo() = %v, want %v", got, want)
	}
}

func TestAfterProcessingTimeTrigger_PlusDelay(t1 *testing.T) {
	t := &AfterProcessingTimeTrigger{}

	want := &AfterProcessingTimeTrigger{
		timestampTransforms: []TimestampTransform{DelayTransform{Delay: int64(1)}},
	}

	if got := t.PlusDelay(time.Millisecond); !reflect.DeepEqual(got, want) {
		t1.Errorf("PlusDelay() = %v, want %v", got, want)
	}
}

func TestAfterProcessingTimeTrigger_TimestampTransforms(t1 *testing.T) {
	period, offset := int64(1), int64(0)
	tt := []TimestampTransform{AlignToTransform{Period: period, Offset: offset}}
	t := &AfterProcessingTimeTrigger{
		timestampTransforms: tt,
	}
	if got, want := t.TimestampTransforms(), tt; !reflect.DeepEqual(got, want) {
		t1.Errorf("TimestampTransforms() = %v, want %v", got, want)
	}
}

func TestAfterSynchronizedProcessingTime(t *testing.T) {
	want := &AfterSynchronizedProcessingTimeTrigger{}
	if got := AfterSynchronizedProcessingTime(); !reflect.DeepEqual(got, want) {
		t.Errorf("AfterSynchronizedProcessingTime() = %v, want %v", got, want)
	}
}

func TestAlways(t *testing.T) {
	want := &AlwaysTrigger{}
	if got := Always(); !reflect.DeepEqual(got, want) {
		t.Errorf("Always() = %v, want %v", got, want)
	}
}
func TestDefault(t *testing.T) {
	want := &DefaultTrigger{}
	if got := Default(); !reflect.DeepEqual(got, want) {
		t.Errorf("Default() = %v, want %v", got, want)
	}
}

func TestNever(t *testing.T) {
	want := &NeverTrigger{}
	if got := Never(); !reflect.DeepEqual(got, want) {
		t.Errorf("Never() = %v, want %v", got, want)
	}
}

func TestOrFinally(t *testing.T) {
	main := AfterCount(50)
	finally := Always()
	want := &OrFinallyTrigger{
		main:    main,
		finally: finally,
	}
	if got := OrFinally(main, finally); !reflect.DeepEqual(got, want) {
		t.Errorf("OrFinally() = %v, want %v", got, want)
	}
}

func TestOrFinallyTrigger_Finally(t1 *testing.T) {
	main := AfterCount(50)
	finally := Always()
	t := &OrFinallyTrigger{
		main:    main,
		finally: finally,
	}

	if got, want := t.Finally(), finally; !reflect.DeepEqual(got, want) {
		t1.Errorf("Finally() = %v, want %v", got, want)
	}
}

func TestOrFinallyTrigger_Main(t1 *testing.T) {
	main := AfterCount(50)
	finally := Always()
	t := &OrFinallyTrigger{
		main:    main,
		finally: finally,
	}

	if got, want := t.Main(), main; !reflect.DeepEqual(got, want) {
		t1.Errorf("Main() = %v, want %v", got, want)
	}
}
