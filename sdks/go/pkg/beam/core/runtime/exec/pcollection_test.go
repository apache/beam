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

package exec

import (
	"context"
	"math"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
)

// TestPCollection verifies that the PCollection node works correctly.
// Seed is by default set to 0, so we have a "deterministic" set of
// randomness for the samples.
func TestPCollection(t *testing.T) {
	a := &CaptureNode{UID: 1}
	pcol := &PCollection{UID: 2, Out: a, Coder: coder.NewVarInt(), WindowCoder: coder.NewGlobalWindow()}
	// The "large" 2nd value is to ensure the values are encoded properly,
	// and that Min & Max are behaving.
	inputs := []any{int64(1), int64(2000000000), int64(3)}
	in := &FixedRoot{UID: 3, Elements: makeInput(inputs...), Out: pcol}

	p, err := NewPlan("a", []Unit{a, pcol, in})
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		t.Fatalf("down failed: %v", err)
	}

	expected := makeValues(inputs...)
	if !equalList(a.Elements, expected) {
		t.Errorf("multiplex returned %v for a, want %v", extractValues(a.Elements...), extractValues(expected...))
	}
	snap := pcol.snapshot()
	if want, got := int64(len(expected)), snap.ElementCount; got != want {
		t.Errorf("snapshot miscounted: got %v, want %v", got, want)
	}
	checkPCollectionSizeSample(t, snap, 3, 7, 1, 5)
}

func TestPCollection_sizeReset(t *testing.T) {
	// Check the initial values after resetting.
	var pcol PCollection
	pcol.resetSize()
	snap := pcol.snapshot()
	checkPCollectionSizeSample(t, snap, 0, 0, math.MaxInt64, math.MinInt64)
}

func checkPCollectionSizeSample(t *testing.T, snap PCollectionSnapshot, count, sum, min, max int64) {
	t.Helper()
	if want, got := int64(count), snap.SizeCount; got != want {
		t.Errorf("sample count incorrect: got %v, want %v", got, want)
	}
	if want, got := int64(sum), snap.SizeSum; got != want {
		t.Errorf("sample sum incorrect: got %v, want %v", got, want)
	}
	if want, got := int64(min), snap.SizeMin; got != want {
		t.Errorf("sample min incorrect: got %v, want %v", got, want)
	}
	if want, got := int64(max), snap.SizeMax; got != want {
		t.Errorf("sample max incorrect: got %v, want %v", got, want)
	}
}

// BenchmarkPCollection measures the overhead of invoking a ParDo in a plan.
//
// On @lostluck's desktop (2020/02/20):
// BenchmarkPCollection-12                 44699806                24.8 ns/op             0 B/op          0 allocs/op
func BenchmarkPCollection(b *testing.B) {
	// Pre allocate the capture buffer and process buffer to avoid
	// unnecessary overhead.
	out := &CaptureNode{UID: 1, Elements: make([]FullValue, 0, b.N)}
	process := make([]MainInput, 0, b.N)
	for i := 0; i < b.N; i++ {
		process = append(process, MainInput{Key: FullValue{
			Windows:   window.SingleGlobalWindow,
			Timestamp: mtime.ZeroTimestamp,
			Elm:       int64(1),
		}})
	}
	pcol := &PCollection{UID: 2, Out: out, Coder: coder.NewVarInt(), WindowCoder: coder.NewGlobalWindow()}
	n := &FixedRoot{UID: 3, Elements: process, Out: pcol}
	p, err := NewPlan("a", []Unit{n, pcol, out})
	if err != nil {
		b.Fatalf("failed to construct plan: %v", err)
	}
	b.ResetTimer()
	if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
		b.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		b.Fatalf("down failed: %v", err)
	}
	if got, want := pcol.snapshot().ElementCount, int64(b.N); got != want {
		b.Errorf("did not process all elements: got %v, want %v", got, want)
	}
	if got, want := len(out.Elements), b.N; got != want {
		b.Errorf("did not process all elements: got %v, want %v", got, want)
	}
}

// BenchmarkPCollection_Baseline measures the baseline of the node benchmarking scaffold.
//
// On @lostluck's desktop (2020/02/20):
// BenchmarkPCollection_Baseline-12        62186372                18.8 ns/op             0 B/op          0 allocs/op
func BenchmarkPCollection_Baseline(b *testing.B) {
	// Pre allocate the capture buffer and process buffer to avoid
	// unnecessary overhead.
	out := &CaptureNode{UID: 1, Elements: make([]FullValue, 0, b.N)}
	process := make([]MainInput, 0, b.N)
	for i := 0; i < b.N; i++ {
		process = append(process, MainInput{Key: FullValue{
			Windows:   window.SingleGlobalWindow,
			Timestamp: mtime.ZeroTimestamp,
			Elm:       1,
		}})
	}
	n := &FixedRoot{UID: 3, Elements: process, Out: out}
	p, err := NewPlan("a", []Unit{n, out})
	if err != nil {
		b.Fatalf("failed to construct plan: %v", err)
	}
	b.ResetTimer()
	if err := p.Execute(context.Background(), "1", DataContext{}); err != nil {
		b.Fatalf("execute failed: %v", err)
	}
	if err := p.Down(context.Background()); err != nil {
		b.Fatalf("down failed: %v", err)
	}
	if got, want := len(out.Elements), b.N; got != want {
		b.Errorf("did not process all elements: got %v, want %v", got, want)
	}
}
