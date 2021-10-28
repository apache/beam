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
	"sync/atomic"
	"testing"
	"time"
)

func checkStateTime(t *testing.T, s StateSampler, label string, sb, pb, fb, tb time.Duration) {
	t.Helper()
	r := s.store.stateRegistry
	v := r[label]
	if v[StartBundle].TotalTime != sb || v[ProcessBundle].TotalTime != pb || v[FinishBundle].TotalTime != fb || v[TotalBundle].TotalTime != tb {
		t.Errorf("got: start: %v, process:%v, finish:%v, total:%v; want start: %v, process:%v, finish:%v, total:%v",
			v[StartBundle].TotalTime, v[ProcessBundle].TotalTime, v[FinishBundle].TotalTime, v[TotalBundle].TotalTime, sb, pb, fb, tb)
	}
}

func checkBundleState(ctx context.Context, t *testing.T, s StateSampler, transitions int64, millisSinceLastTransition time.Duration) {
	t.Helper()
	e := atomic.LoadInt64(s.store.transitions)
	if e != transitions || s.millisSinceLastTransition != millisSinceLastTransition {
		t.Errorf("number of transitions: %v, want %v \nmillis since last transition: %vms, want %vms", e, transitions, s.millisSinceLastTransition, millisSinceLastTransition)
	}
}

func TestSampler(t *testing.T) {
	ctx := context.Background()
	bctx := SetBundleID(ctx, "test")
	interval := 200 * time.Millisecond
	st := GetStore(bctx)
	s := NewSampler(st)

	pctx := SetPTransformID(bctx, "transform")
	label := "transform"

	pt := NewPTransformState("transform")

	SetPTransformState(pctx, pt, StartBundle)
	s.Sample(bctx, interval)

	// validate states and their time till now
	checkStateTime(t, s, label, 200*time.Millisecond, 0, 0, 200*time.Millisecond)
	checkBundleState(bctx, t, s, 1, 0)

	SetPTransformState(pctx, pt, ProcessBundle)
	s.Sample(bctx, interval)
	SetPTransformState(pctx, pt, ProcessBundle)
	SetPTransformState(pctx, pt, ProcessBundle)
	s.Sample(bctx, interval)

	// validate states and their time till now
	checkStateTime(t, s, label, 200*time.Millisecond, 400*time.Millisecond, 0, 600*time.Millisecond)
	checkBundleState(bctx, t, s, 4, 0)

	s.Sample(bctx, interval)
	s.Sample(bctx, interval)
	s.Sample(bctx, interval)

	// validate states and their time till now
	checkStateTime(t, s, label, 200*time.Millisecond, 1000*time.Millisecond, 0, 1200*time.Millisecond)
	checkBundleState(bctx, t, s, 4, 600*time.Millisecond)

	SetPTransformState(pctx, pt, FinishBundle)
	s.Sample(bctx, interval)
	// validate states and their time till now
	checkStateTime(t, s, label, 200*time.Millisecond, 1000*time.Millisecond, 200*time.Millisecond, 1400*time.Millisecond)
	checkBundleState(bctx, t, s, 5, 0)
}

func TestSampler_TwoPTransforms(t *testing.T) {
	ctx := context.Background()
	bctx := SetBundleID(ctx, "bundle")
	interval := 200 * time.Millisecond
	st := GetStore(bctx)
	s := NewSampler(st)

	ctxA := SetPTransformID(bctx, "transformA")
	ctxB := SetPTransformID(bctx, "transformB")

	labelA := "transformA"
	labelB := "transformB"

	ptA := NewPTransformState(labelA)
	ptB := NewPTransformState(labelB)
	SetPTransformState(ctxA, ptA, ProcessBundle)
	s.Sample(bctx, interval)

	// validate states and their time till now
	checkStateTime(t, s, labelA, 0, 200*time.Millisecond, 0, 200*time.Millisecond)
	checkStateTime(t, s, labelB, 0, 0, 0, 0)
	checkBundleState(bctx, t, s, 1, 0)

	SetPTransformState(ctxB, ptB, ProcessBundle)
	s.Sample(bctx, interval)
	SetPTransformState(ctxA, ptA, ProcessBundle)
	SetPTransformState(ctxB, ptB, ProcessBundle)
	s.Sample(bctx, interval)

	// validate states and their time till now
	checkStateTime(t, s, labelA, 0, 200*time.Millisecond, 0, 200*time.Millisecond)
	checkStateTime(t, s, labelB, 0, 400*time.Millisecond, 0, 400*time.Millisecond)
	checkBundleState(bctx, t, s, 4, 0)

	s.Sample(bctx, interval)
	s.Sample(bctx, interval)
	s.Sample(bctx, interval)

	// validate states and their time till now
	checkStateTime(t, s, labelA, 0, 200*time.Millisecond, 0, 200*time.Millisecond)
	checkStateTime(t, s, labelB, 0, 1000*time.Millisecond, 0, 1000*time.Millisecond)
	checkBundleState(bctx, t, s, 4, 600*time.Millisecond)

	SetPTransformState(ctxA, ptA, FinishBundle)
	s.Sample(bctx, interval)
	SetPTransformState(ctxB, ptB, FinishBundle)

	// validate states and their time till now
	checkStateTime(t, s, labelA, 0, 200*time.Millisecond, 200*time.Millisecond, 400*time.Millisecond)
	checkStateTime(t, s, labelB, 0, 1000*time.Millisecond, 0, 1000*time.Millisecond)
	checkBundleState(bctx, t, s, 6, 0)
}

// goos: darwin
// goarch: amd64
// pkg: github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// BenchmarkMsec_SetPTransformState-12    	73519825	        16.52 ns/op	       0 B/op	       0 allocs/op
func BenchmarkMsec_SetPTransformState(b *testing.B) {
	ctx := context.Background()
	bctx := SetBundleID(ctx, "benchmark")
	pctx := SetPTransformID(bctx, "transform")

	pt := NewPTransformState("transform")
	for i := 0; i < b.N; i++ {
		SetPTransformState(pctx, pt, StartBundle)
	}
}

// goos: darwin
// goarch: amd64
// pkg: github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// BenchmarkMsec_Sample-12    	39142482	        29.60 ns/op	       0 B/op	       0 allocs/op
func BenchmarkMsec_Sample(b *testing.B) {
	ctx := context.Background()
	bctx := SetBundleID(ctx, "benchmark")
	pctx := SetPTransformID(bctx, "transform")

	pt := NewPTransformState("transform")
	SetPTransformState(pctx, pt, StartBundle)
	st := GetStore(bctx)
	s := NewSampler(st)
	interval := 200 * time.Millisecond
	for i := 0; i < b.N; i++ {
		s.Sample(bctx, interval)
	}
}

// goos: darwin
// goarch: amd64
// pkg: github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// BenchmarkMsec_Combined-12    	28194320	        35.86 ns/op	       0 B/op	       0 allocs/op
func BenchmarkMsec_Combined(b *testing.B) {
	ctx := context.Background()
	bctx := SetBundleID(ctx, "benchmark")

	st := GetStore(bctx)
	s := NewSampler(st)
	done := make(chan int)
	interval := 200 * time.Millisecond
	go func(done chan int, s StateSampler) {
		for {
			select {
			case <-done:
				return
			default:
				s.Sample(bctx, 5*time.Millisecond)
				time.Sleep(5 * time.Millisecond)
			}
		}
	}(done, s)

	time.Sleep(interval)
	ctxA := SetPTransformID(bctx, "transformA")
	ctxB := SetPTransformID(bctx, "transformB")
	ptA := NewPTransformState("transformA")
	ptB := NewPTransformState("transformB")

	for i := 0; i < b.N; i++ {
		SetPTransformState(ctxA, ptA, ProcessBundle)
		SetPTransformState(ctxB, ptB, ProcessBundle)
	}
	close(done)
}
