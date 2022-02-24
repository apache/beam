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
