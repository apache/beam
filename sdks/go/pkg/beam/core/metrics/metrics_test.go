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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// bID is a bundleId to use in the tests, if nothing more specific is needed.
const bID = "bID"

// TestRobustness validates metrics not panicking if the context doesn't
// have the bundle or transform ID.
func TestRobustness(t *testing.T) {
	m := NewCounter("Test", "myCount")
	m.Inc(context.Background(), 3)
	ptCtx := SetPTransformID(context.Background(), "MY_TRANSFORM")
	m.Inc(ptCtx, 3)
	bCtx := SetBundleID(context.Background(), bID)
	m.Inc(bCtx, 3)
}

func ctxWith(b, pt string) context.Context {
	ctx := context.Background()
	ctx = SetBundleID(ctx, b)
	ctx = SetPTransformID(ctx, pt)
	return ctx
}

func TestCounter_Inc(t *testing.T) {
	ctxA := ctxWith(bID, "A")
	ctxB := ctxWith(bID, "B")

	tests := []struct {
		ns, n string // Counter name
		ctx   context.Context
		inc   int64
		value int64 // Internal variable to check
	}{
		{ns: "inc1", n: "count", ctx: ctxA, inc: 1, value: 1},
		{ns: "inc1", n: "count", ctx: ctxA, inc: 1, value: 2},
		{ns: "inc1", n: "ticker", ctx: ctxA, inc: 1, value: 1},
		{ns: "inc1", n: "ticker", ctx: ctxA, inc: 2, value: 3},
		{ns: "inc1", n: "count", ctx: ctxB, inc: 1, value: 1},
		{ns: "inc1", n: "count", ctx: ctxB, inc: 1, value: 2},
		{ns: "inc1", n: "ticker", ctx: ctxB, inc: 1, value: 1},
		{ns: "inc1", n: "ticker", ctx: ctxB, inc: 2, value: 3},
		{ns: "inc2", n: "count", ctx: ctxA, inc: 1, value: 1},
		{ns: "inc2", n: "count", ctx: ctxA, inc: 1, value: 2},
		{ns: "inc2", n: "ticker", ctx: ctxA, inc: 1, value: 1},
		{ns: "inc2", n: "ticker", ctx: ctxA, inc: 2, value: 3},
		{ns: "inc2", n: "count", ctx: ctxB, inc: 1, value: 1},
		{ns: "inc2", n: "count", ctx: ctxB, inc: 1, value: 2},
		{ns: "inc2", n: "ticker", ctx: ctxB, inc: 1, value: 1},
		{ns: "inc2", n: "ticker", ctx: ctxB, inc: 2, value: 3},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("add %d to %s.%s[%v] value: %d", test.inc, test.ns, test.n, test.ctx, test.value),
			func(t *testing.T) {
				m := NewCounter(test.ns, test.n)
				m.Inc(test.ctx, test.inc)

				cs := getCounterSet(test.ctx)
				c := cs.counters[m.hash]
				if got, want := c.value, test.value; got != want {
					t.Errorf("GetCounter(%q,%q).Inc(%v, %d) c.value got %v, want %v", test.ns, test.n, test.ctx, test.inc, got, want)
				}
			})
	}
}

func TestCounter_Dec(t *testing.T) {
	ctxA := ctxWith(bID, "A")
	ctxB := ctxWith(bID, "B")

	tests := []struct {
		ns, n string // Counter name
		ctx   context.Context
		dec   int64
		value int64 // Internal variable to check
	}{
		{ns: "dec1", n: "count", ctx: ctxA, dec: 1, value: -1},
		{ns: "dec1", n: "count", ctx: ctxA, dec: 1, value: -2},
		{ns: "dec1", n: "ticker", ctx: ctxA, dec: 1, value: -1},
		{ns: "dec1", n: "ticker", ctx: ctxA, dec: 2, value: -3},
		{ns: "dec1", n: "count", ctx: ctxB, dec: 1, value: -1},
		{ns: "dec1", n: "count", ctx: ctxB, dec: 1, value: -2},
		{ns: "dec1", n: "ticker", ctx: ctxB, dec: 1, value: -1},
		{ns: "dec1", n: "ticker", ctx: ctxB, dec: 2, value: -3},
		{ns: "dec2", n: "count", ctx: ctxA, dec: 1, value: -1},
		{ns: "dec2", n: "count", ctx: ctxA, dec: 1, value: -2},
		{ns: "dec2", n: "ticker", ctx: ctxA, dec: 1, value: -1},
		{ns: "dec2", n: "ticker", ctx: ctxA, dec: 2, value: -3},
		{ns: "dec2", n: "count", ctx: ctxB, dec: 1, value: -1},
		{ns: "dec2", n: "count", ctx: ctxB, dec: 1, value: -2},
		{ns: "dec2", n: "ticker", ctx: ctxB, dec: 1, value: -1},
		{ns: "dec2", n: "ticker", ctx: ctxB, dec: 2, value: -3},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("subtract %d to %s.%s[%v] value: %d", test.dec, test.ns, test.n, test.ctx, test.value),
			func(t *testing.T) {
				m := NewCounter(test.ns, test.n)
				m.Dec(test.ctx, test.dec)

				cs := getCounterSet(test.ctx)
				c := cs.counters[m.hash]
				if got, want := c.value, test.value; got != want {
					t.Errorf("GetCounter(%q,%q).Dec(%v, %d) c.value got %v, want %v", test.ns, test.n, test.ctx, test.dec, got, want)
				}
			})
	}
}

func TestDistribution_Update(t *testing.T) {
	ctxA := ctxWith(bID, "A")
	ctxB := ctxWith(bID, "B")
	tests := []struct {
		ns, n                string // Counter name
		ctx                  context.Context
		v                    int64
		count, sum, min, max int64 // Internal variables to check
	}{
		{ns: "update1", n: "latency", ctx: ctxA, v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update1", n: "latency", ctx: ctxA, v: 1, count: 2, sum: 2, min: 1, max: 1},
		{ns: "update1", n: "latency", ctx: ctxA, v: 1, count: 3, sum: 3, min: 1, max: 1},
		{ns: "update1", n: "size", ctx: ctxA, v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update1", n: "size", ctx: ctxA, v: 2, count: 2, sum: 3, min: 1, max: 2},
		{ns: "update1", n: "size", ctx: ctxA, v: 3, count: 3, sum: 6, min: 1, max: 3},
		{ns: "update1", n: "size", ctx: ctxA, v: -4, count: 4, sum: 2, min: -4, max: 3},
		{ns: "update1", n: "size", ctx: ctxA, v: 1, count: 5, sum: 3, min: -4, max: 3},
		{ns: "update1", n: "latency", ctx: ctxB, v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update1", n: "latency", ctx: ctxB, v: 1, count: 2, sum: 2, min: 1, max: 1},
		{ns: "update1", n: "size", ctx: ctxB, v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update1", n: "size", ctx: ctxB, v: 2, count: 2, sum: 3, min: 1, max: 2},
		{ns: "update2", n: "latency", ctx: ctxA, v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update2", n: "latency", ctx: ctxA, v: 1, count: 2, sum: 2, min: 1, max: 1},
		{ns: "update2", n: "size", ctx: ctxA, v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update2", n: "size", ctx: ctxA, v: 2, count: 2, sum: 3, min: 1, max: 2},
		{ns: "update2", n: "latency", ctx: ctxB, v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update2", n: "latency", ctx: ctxB, v: 1, count: 2, sum: 2, min: 1, max: 1},
		{ns: "update2", n: "size", ctx: ctxB, v: 1, count: 1, sum: 1, min: 1, max: 1},
		{ns: "update2", n: "size", ctx: ctxB, v: 2, count: 2, sum: 3, min: 1, max: 2},
		{ns: "update1", n: "size", ctx: ctxA, v: 1, count: 6, sum: 4, min: -4, max: 3},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("add %d to %s.%s[%q] count: %d sum: %d", test.v, test.ns, test.n, test.ctx, test.count, test.sum),
			func(t *testing.T) {
				m := NewDistribution(test.ns, test.n)
				m.Update(test.ctx, test.v)

				cs := getCounterSet(test.ctx)
				d := cs.distributions[m.hash]
				if got, want := d.count, test.count; got != want {
					t.Errorf("GetDistribution(%q,%q).Update(%v, %d) d.count got %v, want %v", test.ns, test.n, test.ctx, test.v, got, want)
				}
				if got, want := d.sum, test.sum; got != want {
					t.Errorf("GetDistribution(%q,%q).Update(%v, %d) d.sum got %v, want %v", test.ns, test.n, test.ctx, test.v, got, want)
				}
				if got, want := d.min, test.min; got != want {
					t.Errorf("GetDistribution(%q,%q).Update(%v, %d) d.min got %v, want %v", test.ns, test.n, test.ctx, test.v, got, want)
				}
				if got, want := d.max, test.max; got != want {
					t.Errorf("GetDistribution(%q,%q).Update(%v, %d) d.max got %v, want %v", test.ns, test.n, test.ctx, test.v, got, want)
				}
			})
	}
}

func testclock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestGauge_Set(t *testing.T) {
	ctxA := ctxWith(bID, "A")
	ctxB := ctxWith(bID, "B")
	tests := []struct {
		ns, n string // Counter name
		ctx   context.Context
		v     int64
		t     time.Time
	}{
		{ns: "set1", n: "load", ctx: ctxA, v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "load", ctx: ctxA, v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "speed", ctx: ctxA, v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "speed", ctx: ctxA, v: 2, t: time.Unix(0, 0)},
		{ns: "set1", n: "load", ctx: ctxB, v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "load", ctx: ctxB, v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "speed", ctx: ctxB, v: 1, t: time.Unix(0, 0)},
		{ns: "set1", n: "speed", ctx: ctxB, v: 2, t: time.Unix(0, 0)},
		{ns: "set2", n: "load", ctx: ctxA, v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "load", ctx: ctxA, v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "speed", ctx: ctxA, v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "speed", ctx: ctxA, v: 2, t: time.Unix(0, 0)},
		{ns: "set2", n: "load", ctx: ctxB, v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "load", ctx: ctxB, v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "speed", ctx: ctxB, v: 1, t: time.Unix(0, 0)},
		{ns: "set2", n: "speed", ctx: ctxB, v: 2, t: time.Unix(0, 0)},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("set %s.%s[%v] to %d at %v", test.ns, test.n, test.ctx, test.v, test.t),
			func(t *testing.T) {
				m := NewGauge(test.ns, test.n)
				now = testclock(test.t)
				m.Set(test.ctx, test.v)

				cs := getCounterSet(test.ctx)
				g := cs.gauges[m.hash]
				if got, want := g.v, test.v; got != want {
					t.Errorf("GetGauge(%q,%q).Set(%v, %d) g.v got %v, want %v", test.ns, test.n, test.ctx, test.v, got, want)
				}
				if got, want := g.t, test.t; got != want {
					t.Errorf("GetGauge(%q,%q).Set(%v, %d) t.t got %v, want %v", test.ns, test.n, test.ctx, test.v, got, want)
				}
			})
	}
}

func TestNameCollisions(t *testing.T) {
	ns, c, d, g := "collisions", "counter", "distribution", "gauge"
	// Checks that user code panics if a counter attempts to be defined in the same PTransform
	// Collisions are unfortunately only detectable at runtime, and only if both the initial
	// metric, and the new metric are actually used, since we don't know the context until
	// then.
	// Pre-create and use so that we have existing metrics to collide with.
	ctx := SetBundleID(context.Background(), bID)
	cctx, dctx, gctx := SetPTransformID(ctx, c), SetPTransformID(ctx, d), SetPTransformID(ctx, g)
	NewCounter(ns, c).Inc(cctx, 1)
	NewDistribution(ns, d).Update(dctx, 1)
	NewGauge(ns, g).Set(gctx, 1)
	tests := []struct {
		existing, new kind
	}{
		{existing: kindSumCounter, new: kindSumCounter},
		{existing: kindSumCounter, new: kindDistribution},
		{existing: kindSumCounter, new: kindGauge},
		{existing: kindDistribution, new: kindSumCounter},
		{existing: kindDistribution, new: kindDistribution},
		{existing: kindDistribution, new: kindGauge},
		{existing: kindGauge, new: kindSumCounter},
		{existing: kindGauge, new: kindDistribution},
		{existing: kindGauge, new: kindGauge},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%s name collides with %s", test.existing, test.new),
			func(t *testing.T) {
				defer func() {
					if test.existing != test.new {
						if e := recover(); e != nil {
							t.Logf("panic caught re-using a name between a %s, and a %s", test.existing, test.new)
							return
						}
						t.Error("panic expected")
					} else {
						t.Log("reusing names is fine when the metric is the same type:", test.existing, test.new)
					}
				}()
				var name string
				var ctx context.Context
				switch test.existing {
				case kindSumCounter:
					name = c
					ctx = cctx
				case kindDistribution:
					name = d
					ctx = dctx
				case kindGauge:
					name = g
					ctx = gctx
				default:
					t.Fatalf("unknown existing metricType with value: %v", int(test.existing))
				}
				switch test.new {
				case kindSumCounter:
					NewCounter(ns, name).Inc(ctx, 1)
				case kindDistribution:
					NewDistribution(ns, name).Update(ctx, 1)
				case kindGauge:
					NewGauge(ns, name).Set(ctx, 1)
				default:
					t.Fatalf("unknown new metricType with value: %v", int(test.new))
				}

			})
	}
}

func TestMergeCounters(t *testing.T) {
	realKey := StepKey{Name: "real"}
	tests := []struct {
		name                 string
		attempted, committed map[StepKey]int64
		want                 []CounterResult
	}{
		{
			name: "merge",
			attempted: map[StepKey]int64{
				realKey: 5,
			},
			committed: map[StepKey]int64{
				realKey: 7,
			},
			want: []CounterResult{{Attempted: 5, Committed: 7, Key: realKey}},
		}, {
			name: "attempted only",
			attempted: map[StepKey]int64{
				realKey: 5,
			},
			committed: map[StepKey]int64{},
			want:      []CounterResult{{Attempted: 5, Key: realKey}},
		}, {
			name:      "committed only",
			attempted: map[StepKey]int64{},
			committed: map[StepKey]int64{
				realKey: 7,
			},
			want: []CounterResult{{Committed: 7, Key: realKey}},
		},
	}
	less := func(a, b CounterResult) bool {
		return a.Key.Name < b.Key.Name
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := MergeCounters(test.attempted, test.committed)
			if d := cmp.Diff(test.want, got, cmpopts.SortSlices(less)); d != "" {
				t.Errorf("MergeCounters(%+v, %+v) = %+v, want %+v\ndiff:\n%v", test.attempted, test.committed, got, test.want, d)
			}
		})
	}
}

func TestMergeDistributions(t *testing.T) {
	realKey := StepKey{Name: "real"}
	distA := DistributionValue{Count: 2, Sum: 5, Min: 1, Max: 4}
	distB := DistributionValue{Count: 3, Sum: 5, Min: 1, Max: 2}
	tests := []struct {
		name                 string
		attempted, committed map[StepKey]DistributionValue
		want                 []DistributionResult
	}{
		{
			name: "merge",
			attempted: map[StepKey]DistributionValue{
				realKey: distA,
			},
			committed: map[StepKey]DistributionValue{
				realKey: distB,
			},
			want: []DistributionResult{{Attempted: distA, Committed: distB, Key: realKey}},
		}, {
			name: "attempted only",
			attempted: map[StepKey]DistributionValue{
				realKey: distA,
			},
			committed: map[StepKey]DistributionValue{},
			want:      []DistributionResult{{Attempted: distA, Key: realKey}},
		}, {
			name:      "committed only",
			attempted: map[StepKey]DistributionValue{},
			committed: map[StepKey]DistributionValue{
				realKey: distB,
			},
			want: []DistributionResult{{Committed: distB, Key: realKey}},
		},
	}
	less := func(a, b DistributionResult) bool {
		return a.Key.Name < b.Key.Name
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := MergeDistributions(test.attempted, test.committed)
			if d := cmp.Diff(test.want, got, cmpopts.SortSlices(less)); d != "" {
				t.Errorf("MergeDistributions(%+v, %+v) = %+v, want %+v\ndiff:\n%v", test.attempted, test.committed, got, test.want, d)
			}
		})
	}
}

func TestMergePCols(t *testing.T) {
	realKey := StepKey{Name: "real"}
	pColA := PColValue{ElementCount: 1, SampledByteSize: DistributionValue{Count: 2, Sum: 3, Min: 4, Max: 5}}
	pColB := PColValue{ElementCount: 5, SampledByteSize: DistributionValue{Count: 4, Sum: 3, Min: 2, Max: 1}}
	tests := []struct {
		name                 string
		attempted, committed map[StepKey]PColValue
		want                 []PColResult
	}{
		{
			name: "merge",
			attempted: map[StepKey]PColValue{
				realKey: pColA,
			},
			committed: map[StepKey]PColValue{
				realKey: pColB,
			},
			want: []PColResult{{Attempted: pColA, Committed: pColB, Key: realKey}},
		}, {
			name: "attempted only",
			attempted: map[StepKey]PColValue{
				realKey: pColA,
			},
			committed: map[StepKey]PColValue{},
			want:      []PColResult{{Attempted: pColA, Key: realKey}},
		}, {
			name:      "committed only",
			attempted: map[StepKey]PColValue{},
			committed: map[StepKey]PColValue{
				realKey: pColB,
			},
			want: []PColResult{{Committed: pColB, Key: realKey}},
		},
	}
	less := func(a, b DistributionResult) bool {
		return a.Key.Name < b.Key.Name
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := MergePCols(test.attempted, test.committed)
			if d := cmp.Diff(test.want, got, cmpopts.SortSlices(less)); d != "" {
				t.Errorf("MergePCols(%+v, %+v) = %+v, want %+v\ndiff:\n%v", test.attempted, test.committed, got, test.want, d)
			}
		})
	}
}

func TestMergeGauges(t *testing.T) {
	realKey := StepKey{Name: "real"}
	now := time.Now()
	later := now.Add(time.Hour)
	gaugeA := GaugeValue{Value: 2, Timestamp: now}
	gaugeB := GaugeValue{Value: 3, Timestamp: later}
	tests := []struct {
		name                 string
		attempted, committed map[StepKey]GaugeValue
		want                 []GaugeResult
	}{
		{
			name: "merge",
			attempted: map[StepKey]GaugeValue{
				realKey: gaugeA,
			},
			committed: map[StepKey]GaugeValue{
				realKey: gaugeB,
			},
			want: []GaugeResult{{Attempted: gaugeA, Committed: gaugeB, Key: realKey}},
		}, {
			name: "attempted only",
			attempted: map[StepKey]GaugeValue{
				realKey: gaugeA,
			},
			committed: map[StepKey]GaugeValue{},
			want:      []GaugeResult{{Attempted: gaugeA, Key: realKey}},
		}, {
			name:      "committed only",
			attempted: map[StepKey]GaugeValue{},
			committed: map[StepKey]GaugeValue{
				realKey: gaugeB,
			},
			want: []GaugeResult{{Committed: gaugeB, Key: realKey}},
		},
	}
	less := func(a, b DistributionResult) bool {
		return a.Key.Name < b.Key.Name
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := MergeGauges(test.attempted, test.committed)
			if d := cmp.Diff(test.want, got, cmpopts.SortSlices(less)); d != "" {
				t.Errorf("MergeGauges(%+v, %+v) = %+v, want %+v\ndiff:\n%v", test.attempted, test.committed, got, test.want, d)
			}
		})
	}
}

func TestMergeMsecs(t *testing.T) {
	realKey := StepKey{Name: "real"}
	msecA := MsecValue{Start: time.Second, Process: 2 * time.Second, Finish: time.Second, Total: 4 * time.Second}
	msecB := MsecValue{Start: 2 * time.Second, Process: time.Second, Finish: 2 * time.Second, Total: 5 * time.Second}
	tests := []struct {
		name                 string
		attempted, committed map[StepKey]MsecValue
		want                 []MsecResult
	}{
		{
			name: "merge",
			attempted: map[StepKey]MsecValue{
				realKey: msecA,
			},
			committed: map[StepKey]MsecValue{
				realKey: msecB,
			},
			want: []MsecResult{{Attempted: msecA, Committed: msecB, Key: realKey}},
		}, {
			name: "attempted only",
			attempted: map[StepKey]MsecValue{
				realKey: msecA,
			},
			committed: map[StepKey]MsecValue{},
			want:      []MsecResult{{Attempted: msecA, Key: realKey}},
		}, {
			name:      "committed only",
			attempted: map[StepKey]MsecValue{},
			committed: map[StepKey]MsecValue{
				realKey: msecB,
			},
			want: []MsecResult{{Committed: msecB, Key: realKey}},
		},
	}
	less := func(a, b DistributionResult) bool {
		return a.Key.Name < b.Key.Name
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := MergeMsecs(test.attempted, test.committed)
			if d := cmp.Diff(test.want, got, cmpopts.SortSlices(less)); d != "" {
				t.Errorf("MergeMsecs(%+v, %+v) = %+v, want %+v\ndiff:\n%v", test.attempted, test.committed, got, test.want, d)
			}
		})
	}
}

func TestMsecQueryResult(t *testing.T) {
	realKey := StepKey{Step: "sumFn"}
	msecA := MsecValue{Start: 0, Process: 0, Finish: 0, Total: 0}
	msecB := MsecValue{Start: 200 * time.Millisecond, Process: 0, Finish: 0, Total: 200 * time.Millisecond}
	msecR := MsecResult{Attempted: msecA, Committed: msecB, Key: realKey}
	res := Results{msecs: []MsecResult{msecR}}

	tests := []struct {
		name        string
		queryResult Results
		query       string
		want        QueryResults
	}{
		{
			name:        "present",
			queryResult: res,
			query:       "sumFn",
			want:        QueryResults{msecs: []MsecResult{msecR}},
		}, {
			name:        "not present",
			queryResult: res,
			query:       "countFn",
			want:        QueryResults{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := res.Query(func(sr SingleResult) bool {
				return strings.Contains(sr.Transform(), test.query)
			})
			if len(got.Msecs()) != len(test.want.Msecs()) {
				t.Errorf("(Results).Query(by Transform %v) = %v, want = %v", test.query, got.Msecs(), test.want.Msecs())
			}
		})
	}
}

func TestPcolQueryResult(t *testing.T) {
	realKey := StepKey{Step: "sumFn"}
	pcolA := PColValue{}
	pcolB := PColValue{ElementCount: 1, SampledByteSize: DistributionValue{1, 1, 1, 1}}
	pcolR := PColResult{Attempted: pcolA, Committed: pcolB, Key: realKey}
	res := Results{pCols: []PColResult{pcolR}}

	tests := []struct {
		name        string
		queryResult Results
		query       string
		want        QueryResults
	}{
		{
			name:        "present",
			queryResult: res,
			query:       "sumFn",
			want:        QueryResults{pCols: []PColResult{pcolR}},
		}, {
			name:        "not present",
			queryResult: res,
			query:       "countFn",
			want:        QueryResults{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := res.Query(func(sr SingleResult) bool {
				return strings.Contains(sr.Transform(), test.query)
			})
			if len(got.PCols()) != len(test.want.PCols()) {
				t.Errorf("(Results).Query(by Transform %v) = %v, want = %v", test.query, got.PCols(), test.want.PCols())
			}
		})
	}
}

// Run on @shanemhansen's desktop (2022/01/03) go1.20 RC1 after changing hashName to use a pool of hashers
// sync.Pool can return thread-local results eliminating the need for a lock and increasing throughput.
// There are users in the wild who create an excessive number of Counters so a 4x improvement in throughput at the expense of
// creating GOMAXPROCS hasher values seems reasonable.
//
// name                                 old time/op  new time/op  delta
// Metrics/counter_inplace-12            376ns ±17%    88ns ± 7%  -76.66%  (p=0.008 n=5+5)
// Metrics/distribution_inplace-12       394ns ± 3%   153ns ± 8%  -61.17%  (p=0.008 n=5+5)
// Metrics/gauge_inplace-12              371ns ± 4%   258ns ± 1%  -30.37%  (p=0.008 n=5+5)
// Metrics/counter_predeclared-12       16.9ns ± 6%  17.0ns ± 3%     ~     (p=0.595 n=5+5)
// Metrics/distribution_predeclared-12  83.2ns ± 2%  84.9ns ± 1%     ~     (p=0.056 n=5+5)
// Metrics/gauge_predeclared-12          105ns ± 6%   110ns ± 5%   +4.81%  (p=0.032 n=5+5)
// Metrics/counter_raw-12               10.8ns ± 4%  12.0ns ±28%     ~     (p=0.151 n=5+5)
// Metrics/distribution_raw-12          77.6ns ± 7%  78.8ns ± 5%     ~     (p=0.841 n=5+5)
// Metrics/gauge_raw-12                 78.9ns ± 1%  77.3ns ± 4%     ~     (p=0.151 n=5+5)
// Metrics/getStore-12                  0.27ns ± 3%  0.27ns ± 2%     ~     (p=0.841 n=5+5)
// Metrics/getCounterSet-12             0.32ns ± 3%  0.31ns ± 0%   -1.28%  (p=0.048 n=5+4)
func BenchmarkMetrics(b *testing.B) {
	pt, c, d, g := "bench.bundle.data", "counter", "distribution", "gauge"
	aBundleID := "benchBID"
	ctx := ctxWith(aBundleID, pt)
	count := NewCounter(pt, c)
	dist := NewDistribution(pt, d)
	gaug := NewGauge(pt, g)

	rawCount := &counter{}
	rawDist := &distribution{}
	rawGauge := &gauge{}
	tests := []struct {
		name string
		call func()
	}{
		{"counter_inplace", func() { NewCounter(pt, c).Inc(ctx, 1) }},
		{"distribution_inplace", func() { NewDistribution(pt, d).Update(ctx, 1) }},
		{"gauge_inplace", func() { NewGauge(pt, g).Set(ctx, 1) }},
		{"counter_predeclared", func() { count.Inc(ctx, 1) }},
		{"distribution_predeclared", func() { dist.Update(ctx, 1) }},
		{"gauge_predeclared", func() { gaug.Set(ctx, 1) }},
		// These measure the unwrapped metric cell operations, without looking up
		// the store or counterset, rather than the user facing API.
		{"counter_raw", func() { rawCount.inc(1) }},
		{"distribution_raw", func() { rawDist.update(1) }},
		{"gauge_raw", func() { rawGauge.set(1) }},
		// This just measures GetStore and getCounterSet
		{"getStore", func() { GetStore(ctx) }},
		{"getCounterSet", func() { getCounterSet(ctx) }},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					test.call()
				}
			})
		})
	}
}
