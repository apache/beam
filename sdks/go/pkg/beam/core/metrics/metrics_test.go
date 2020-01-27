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
	"testing"
	"time"
)

// bID is a bundleId to use in the tests, if nothing more specific is needed.
const bID = "bID"

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
	NewCounter(ns, c).Inc(ctxWith(bID, c), 1)
	NewDistribution(ns, d).Update(ctxWith(bID, d), 1)
	NewGauge(ns, g).Set(ctxWith(bID, g), 1)
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
				switch test.existing {
				case kindSumCounter:
					name = c
				case kindDistribution:
					name = d
				case kindGauge:
					name = g
				default:
					t.Fatalf("unknown existing metricType with value: %v", int(test.existing))
				}
				switch test.new {
				case kindSumCounter:
					NewCounter(ns, name).Inc(ctxWith(bID, name), 1)
				case kindDistribution:
					NewDistribution(ns, name).Update(ctxWith(bID, name), 1)
				case kindGauge:
					NewGauge(ns, name).Set(ctxWith(bID, name), 1)
				default:
					t.Fatalf("unknown new metricType with value: %v", int(test.new))
				}

			})
	}
}

func TestClearBundleData(t *testing.T) {
	Clear()
	dump := func(t *testing.T) {
		dumpTo(func(format string, args ...interface{}) {
			t.Logf(format, args...)
		})
	}
	pt, c, d, g := "clear.bundle.data", "counter", "distribution", "gauge"
	aBundleID := "aBID"
	otherBundleID := "otherBID"
	NewCounter(pt, c).Inc(ctxWith(aBundleID, pt), 1)
	NewDistribution(pt, d).Update(ctxWith(aBundleID, pt), 1)
	NewGauge(pt, g).Set(ctxWith(aBundleID, pt), 1)

	NewCounter(pt, c).Inc(ctxWith(otherBundleID, pt), 1)
	NewDistribution(pt, d).Update(ctxWith(otherBundleID, pt), 1)
	NewGauge(pt, g).Set(ctxWith(otherBundleID, pt), 1)

	initialAP := ToProto(aBundleID, pt)
	if got, want := len(initialAP), 3; got != want {
		dump(t)
		t.Fatalf("len(ToProto(%q, %q)) = %v, want %v - initialAP: %v", aBundleID, pt, got, want, initialAP)
	}
	initialOP := ToProto(otherBundleID, pt)
	if got, want := len(initialOP), 3; got != want {
		dump(t)
		t.Fatalf("len(ToProto(%q, %q)) = %v, want %v - initialOP: %v", otherBundleID, pt, got, want, initialOP)
	}

	ClearBundleData(aBundleID)

	newAP := ToProto(aBundleID, pt)
	if got, want := len(newAP), 0; got != want {
		dump(t)
		t.Fatalf("len(ToProto(%q, %q)) = %v, want %v - newAP: %v", aBundleID, pt, got, want, newAP)
	}

	newOP := ToProto(otherBundleID, pt)
	if got, want := len(newOP), 3; got != want {
		dump(t)
		t.Fatalf("len(ToProto(%q, %q)) = %v, want %v - newOP: %v", otherBundleID, pt, got, want, newOP)
	}
}

// Run on @lostluck's desktop (2020/01/21) go1.13.4
//
// Allocs & bytes should be consistetn within go versions, but ns/op is relative to the running machine.
//
// BenchmarkMetrics/counter_inplace-12              4814373               243 ns/op              48 B/op          1 allocs/op
// BenchmarkMetrics/distribution_inplace-12         4455957               273 ns/op              48 B/op          1 allocs/op
// BenchmarkMetrics/gauge_inplace-12                4605908               265 ns/op              48 B/op          1 allocs/op
// BenchmarkMetrics/counter_predeclared-12         75339600                15.5 ns/op             0 B/op          0 allocs/op
// BenchmarkMetrics/distribution_predeclared-12            49202775                24.4 ns/op             0 B/op          0 allocs/op
// BenchmarkMetrics/gauge_predeclared-12                   46614810                28.3 ns/op             0 B/op          0 allocs/op
func BenchmarkMetrics(b *testing.B) {
	Clear()
	pt, c, d, g := "bench.bundle.data", "counter", "distribution", "gauge"
	aBundleID := "benchBID"
	ctx := ctxWith(aBundleID, pt)
	count := NewCounter(pt, c)
	dist := NewDistribution(pt, d)
	gauge := NewGauge(pt, g)
	tests := []struct {
		name string
		call func()
	}{
		{"counter_inplace", func() { NewCounter(pt, c).Inc(ctx, 1) }},
		{"distribution_inplace", func() { NewDistribution(pt, d).Update(ctx, 1) }},
		{"gauge_inplace", func() { NewGauge(pt, g).Set(ctx, 1) }},
		{"counter_predeclared", func() { count.Inc(ctx, 1) }},
		{"distribution_predeclared", func() { dist.Update(ctx, 1) }},
		{"gauge_predeclared", func() { gauge.Set(ctx, 1) }},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				test.call()
			}
		})
	}
}
