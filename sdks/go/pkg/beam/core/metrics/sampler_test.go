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

package metrics

import (
	"context"
	"testing"
	"time"
)

func checkStateTime(t *testing.T, r map[Labels][4]*ExecutionState, label Labels, sb, pb, fb, tb time.Duration) {
	v := r[label]
	if v[StartBundle].TotalTime != sb || v[ProcessBundle].TotalTime != pb || v[FinishBundle].TotalTime != fb || v[TotalBundle].TotalTime != tb {
		t.Errorf("want start: %v, process:%v, finish:%v, total:%v; got: start: %v, process:%v, finish:%v, total:%v",
			sb, pb, fb, tb, v[StartBundle].TotalTime, v[ProcessBundle].TotalTime, v[FinishBundle].TotalTime, v[TotalBundle].TotalTime)
	}
}
func TestSampler(t *testing.T) {
	ctx := context.Background()
	bctx := SetBundleID(ctx, "test")

	st := GetStore(bctx)
	s := NewSampler(bctx, st)

	pctx := SetPTransformID(bctx, "transform")
	label := PTransformLabels("transform")

	SetPTransformState(pctx, StartBundle)
	s.Sample(pctx, 200*time.Millisecond)

	// validate states and their time till now
	r := s.store.GetRegistry()
	checkStateTime(t, r, label, 200*time.Millisecond, 0, 0, 200*time.Millisecond)
	g := s.store.GetExecutionStore()[label]
	if g.numberOfTransitions != 1 {
		t.Errorf("number of transitions: %v, want 1", g.numberOfTransitions)
	}

	SetPTransformState(pctx, ProcessBundle)
	s.Sample(pctx, 200*time.Millisecond)
	SetPTransformState(pctx, ProcessBundle)
	SetPTransformState(pctx, ProcessBundle)
	s.Sample(pctx, 200*time.Millisecond)

	// validate states and their time till now
	r = s.store.GetRegistry()
	checkStateTime(t, r, label, 200*time.Millisecond, 400*time.Millisecond, 0, 600*time.Millisecond)
	e := getExecStoreData(pctx, label)
	if e.NumberOfTransitions != 4 || e.MillisSinceLastTransition != 0 {
		t.Errorf("number of transitions: %v, want 4 \nmillis since last transition: %v, want 0ms", e.NumberOfTransitions, e.MillisSinceLastTransition)
	}

	s.Sample(pctx, 200*time.Millisecond)
	s.Sample(pctx, 200*time.Millisecond)
	s.Sample(pctx, 200*time.Millisecond)

	// validate states and their time till now
	r = s.store.GetRegistry()
	checkStateTime(t, r, label, 200*time.Millisecond, 1000*time.Millisecond, 0, 1200*time.Millisecond)
	e = getExecStoreData(pctx, label)
	if e.NumberOfTransitions != 4 || e.MillisSinceLastTransition != 600*time.Millisecond {
		t.Errorf("number of transitions: %v, want 4 \nmillis since last transition: %v, want 600ms", e.NumberOfTransitions, e.MillisSinceLastTransition)
	}

	SetPTransformState(pctx, FinishBundle)
	s.Sample(pctx, 200*time.Millisecond)
	// validate states and their time till now
	r = s.store.GetRegistry()
	checkStateTime(t, r, label, 200*time.Millisecond, 1000*time.Millisecond, 200*time.Millisecond, 1400*time.Millisecond)
	e = getExecStoreData(pctx, label)
	if e.NumberOfTransitions != 5 || e.MillisSinceLastTransition != 0 {
		t.Errorf("number of transitions: %v, want 5 \nmillis since last transition: %v, want 0ms", e.NumberOfTransitions, e.MillisSinceLastTransition)
	}
}
