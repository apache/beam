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

func checkStateTime(t *testing.T, s StateSampler, label string, sb, pb, fb, tb time.Duration) {
	t.Helper()
	r := s.store.GetRegistry()
	v := r[label]
	if v[StartBundle].TotalTime != sb || v[ProcessBundle].TotalTime != pb || v[FinishBundle].TotalTime != fb || v[TotalBundle].TotalTime != tb {
		t.Errorf("got: start: %v, process:%v, finish:%v, total:%v; want start: %v, process:%v, finish:%v, total:%v",
			v[StartBundle].TotalTime, v[ProcessBundle].TotalTime, v[FinishBundle].TotalTime, v[TotalBundle].TotalTime, sb, pb, fb, tb)
	}
}

func checkBundleState(ctx context.Context, t *testing.T, transitions int64, millisSinceLastTransition time.Duration) {
	t.Helper()
	e := getExecStoreData(ctx)
	if e.NumberOfTransitions != transitions || e.MillisSinceLastTransition != millisSinceLastTransition {
		t.Errorf("number of transitions: %v, want %v \nmillis since last transition: %vms, want %vms", e.NumberOfTransitions, transitions, e.MillisSinceLastTransition, millisSinceLastTransition)
	}
}

func TestSampler(t *testing.T) {
	ctx := context.Background()
	bctx := SetBundleID(ctx, "test")

	st := GetStore(bctx)
	s := NewSampler(bctx, st)

	pctx := SetPTransformID(bctx, "transform")
	label := "transform"

	SetPTransformState(pctx, StartBundle)
	s.Sample(bctx, 200*time.Millisecond)

	// validate states and their time till now
	checkStateTime(t, s, label, 200*time.Millisecond, 0, 0, 200*time.Millisecond)
	checkBundleState(bctx, t, 1, 0)

	SetPTransformState(pctx, ProcessBundle)
	s.Sample(bctx, 200*time.Millisecond)
	SetPTransformState(pctx, ProcessBundle)
	SetPTransformState(pctx, ProcessBundle)
	s.Sample(bctx, 200*time.Millisecond)

	// validate states and their time till now
	checkStateTime(t, s, label, 200*time.Millisecond, 400*time.Millisecond, 0, 600*time.Millisecond)
	checkBundleState(bctx, t, 4, 0)

	s.Sample(bctx, 200*time.Millisecond)
	s.Sample(bctx, 200*time.Millisecond)
	s.Sample(bctx, 200*time.Millisecond)

	// validate states and their time till now
	checkStateTime(t, s, label, 200*time.Millisecond, 1000*time.Millisecond, 0, 1200*time.Millisecond)
	checkBundleState(bctx, t, 4, 600*time.Millisecond)

	SetPTransformState(pctx, FinishBundle)
	s.Sample(bctx, 200*time.Millisecond)
	// validate states and their time till now
	checkStateTime(t, s, label, 200*time.Millisecond, 1000*time.Millisecond, 200*time.Millisecond, 1400*time.Millisecond)
	checkBundleState(bctx, t, 5, 0)
}

func TestSampler_TwoPTransforms(t *testing.T) {
	ctx := context.Background()
	bctx := SetBundleID(ctx, "bundle")

	st := GetStore(bctx)
	s := NewSampler(bctx, st)

	ctxA := SetPTransformID(bctx, "transformA")
	ctxB := SetPTransformID(bctx, "transformB")

	labelA := "transformA"
	labelB := "transformB"

	SetPTransformState(ctxA, ProcessBundle)
	s.Sample(bctx, 200*time.Millisecond)

	// validate states and their time till now
	checkStateTime(t, s, labelA, 0, 200*time.Millisecond, 0, 200*time.Millisecond)
	checkStateTime(t, s, labelB, 0, 0, 0, 0)
	checkBundleState(bctx, t, 1, 0)

	SetPTransformState(ctxB, ProcessBundle)
	s.Sample(bctx, 200*time.Millisecond)
	SetPTransformState(ctxA, ProcessBundle)
	SetPTransformState(ctxB, ProcessBundle)
	s.Sample(bctx, 200*time.Millisecond)

	// validate states and their time till now
	checkStateTime(t, s, labelA, 0, 200*time.Millisecond, 0, 200*time.Millisecond)
	checkStateTime(t, s, labelB, 0, 400*time.Millisecond, 0, 400*time.Millisecond)
	checkBundleState(bctx, t, 4, 0)

	s.Sample(bctx, 200*time.Millisecond)
	s.Sample(bctx, 200*time.Millisecond)
	s.Sample(bctx, 200*time.Millisecond)

	// validate states and their time till now
	checkStateTime(t, s, labelA, 0, 200*time.Millisecond, 0, 200*time.Millisecond)
	checkStateTime(t, s, labelB, 0, 1000*time.Millisecond, 0, 1000*time.Millisecond)
	checkBundleState(bctx, t, 4, 600*time.Millisecond)

	SetPTransformState(ctxA, FinishBundle)
	s.Sample(bctx, 200*time.Millisecond)
	SetPTransformState(ctxB, FinishBundle)

	// validate states and their time till now
	checkStateTime(t, s, labelA, 0, 200*time.Millisecond, 200*time.Millisecond, 400*time.Millisecond)
	checkStateTime(t, s, labelB, 0, 1000*time.Millisecond, 0, 1000*time.Millisecond)
	checkBundleState(bctx, t, 6, 0)
}
