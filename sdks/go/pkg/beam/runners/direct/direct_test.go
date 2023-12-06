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

package direct

import (
	"context"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/google/go-cmp/cmp"
)

func executeWithT(ctx context.Context, t *testing.T, p *beam.Pipeline) (beam.PipelineResult, error) {
	fmt.Println("startingTest - ", t.Name())
	return Execute(ctx, p)
}

func init() {
	beam.RegisterFunction(dofn1)
	beam.RegisterFunction(dofn1x2)
	beam.RegisterFunction(dofn1x5)
	beam.RegisterFunction(dofn2x1)
	beam.RegisterFunction(dofn3x1)
	beam.RegisterFunction(dofn2x2KV)
	beam.RegisterFunction(dofnMultiMap)
	beam.RegisterFunction(dofn2)
	beam.RegisterFunction(dofnKV)
	beam.RegisterFunction(dofnKV2)
	beam.RegisterFunction(dofnGBK)
	beam.RegisterFunction(dofnGBK2)
	beam.RegisterType(reflect.TypeOf((*int64Check)(nil)))
	beam.RegisterType(reflect.TypeOf((*stringCheck)(nil)))

	beam.RegisterType(reflect.TypeOf((*testRow)(nil)))
	beam.RegisterFunction(dofnKV3)
	beam.RegisterFunction(dofnGBK3)

	beam.RegisterFunction(dofn1Counter)
	beam.RegisterFunction(dofnSink)

	beam.RegisterFunction(dofnEtKV1)
	beam.RegisterFunction(dofnEtKV2)
	beam.RegisterFunction(formatCoGBK2)
}

func dofn1(imp []byte, emit func(int64)) {
	emit(1)
	emit(2)
	emit(3)
}

func dofn1x2(imp []byte, emitA func(int64), emitB func(int64)) {
	emitA(1)
	emitA(2)
	emitA(3)
	emitB(4)
	emitB(5)
	emitB(6)
}

func dofn1x5(imp []byte, emitA, emitB, emitC, emitD, emitE func(int64)) {
	emitA(1)
	emitB(2)
	emitC(3)
	emitD(4)
	emitE(5)
	emitA(6)
	emitB(7)
	emitC(8)
	emitD(9)
	emitE(10)
}

func dofn2x1(imp []byte, iter func(*int64) bool, emit func(int64)) {
	var v, sum int64
	for iter(&v) {
		sum += v
	}
	emit(sum)
}

func dofn3x1(sum int64, iter1, iter2 func(*int64) bool, emit func(int64)) {
	var v int64
	for iter1(&v) {
		sum += v
	}
	for iter2(&v) {
		sum += v
	}
	emit(sum)
}

func dofn2x2KV(imp []byte, iter func(*string, *int64) bool, emitK func(string), emitV func(int64)) {
	var k string
	var v, sum int64
	for iter(&k, &v) {
		sum += v
		emitK(k)
	}
	emitV(sum)
}

func dofnMultiMap(key string, lookup func(string) func(*int64) bool, emitK func(string), emitV func(int64)) {
	var v, sum int64
	iter := lookup(key)
	for iter(&v) {
		sum += v
	}
	emitK(key)
	emitV(sum)
}

// int64Check validates that within a single bundle,
// we received the expected int64 values.
type int64Check struct {
	Name string
	Want []int
	got  []int
}

func (fn *int64Check) ProcessElement(v int64, _ func(int64)) {
	fn.got = append(fn.got, int(v))
}

func (fn *int64Check) FinishBundle(_ func(int64)) error {
	sort.Ints(fn.got)
	sort.Ints(fn.Want)
	if d := cmp.Diff(fn.Want, fn.got); d != "" {
		return fmt.Errorf("int64Check[%v] (-want, +got): %v", fn.Name, d)
	}
	return nil
}

// stringCheck validates that within a single bundle,
// we received the expected string values.
type stringCheck struct {
	Name string
	Want []string
	got  []string
}

func (fn *stringCheck) ProcessElement(v string, _ func(string)) {
	fn.got = append(fn.got, v)
}

func (fn *stringCheck) FinishBundle(_ func(string)) error {
	sort.Strings(fn.got)
	sort.Strings(fn.Want)
	if d := cmp.Diff(fn.Want, fn.got); d != "" {
		return fmt.Errorf("stringCheck[%v] (-want, +got): %v", fn.Name, d)
	}
	return nil
}

func dofn2(v int64, emit func(int64)) {
	emit(v + 1)
}

func dofnKV(imp []byte, emit func(string, int64)) {
	emit("a", 1)
	emit("b", 2)
	emit("a", 3)
	emit("b", 4)
	emit("a", 5)
	emit("b", 6)
}

func dofnKV2(imp []byte, emit func(int64, string)) {
	emit(1, "a")
	emit(2, "b")
	emit(1, "a")
	emit(2, "b")
	emit(1, "a")
	emit(2, "b")
}

func dofnGBK(k string, vs func(*int64) bool, emit func(int64)) {
	var v, sum int64
	for vs(&v) {
		sum += v
	}
	emit(sum)
}

func dofnGBK2(k int64, vs func(*string) bool, emit func(string)) {
	var v, sum string
	for vs(&v) {
		sum += v
	}
	emit(sum)
}

type testRow struct {
	A string
	B int64
}

func dofnKV3(imp []byte, emit func(testRow, testRow)) {
	emit(testRow{"a", 1}, testRow{"a", 1})
}

func dofnGBK3(k testRow, vs func(*testRow) bool, emit func(string)) {
	var v testRow
	vs(&v)
	emit(fmt.Sprintf("%v: %v", k, v))
}

const (
	ns = "directtest"
)

func dofnSink(ctx context.Context, _ []byte) {
	beam.NewCounter(ns, "sunk").Inc(ctx, 73)
}

func dofn1Counter(ctx context.Context, _ []byte, emit func(int64)) {
	beam.NewCounter(ns, "count").Inc(ctx, 1)
}

const baseTs = mtime.Time(1663663026000)

func dofnEtKV1(imp []byte, emit func(beam.EventTime, string, int64)) {
	emit(baseTs, "a", 1)
	emit(baseTs.Add(time.Millisecond*1200), "a", 3)

	emit(baseTs.Add(time.Millisecond*2500), "b", 3)
}

func dofnEtKV2(imp []byte, emit func(beam.EventTime, string, int64)) {
	emit(baseTs.Add(time.Millisecond*500), "a", 2)

	emit(baseTs.Add(time.Millisecond), "b", 1)
	emit(baseTs.Add(time.Millisecond*500), "b", 2)
}

func formatCoGBK2(s string, u func(*int64) bool, v func(*int64) bool) string {
	var ls0, ls1 []int64
	val := int64(0)
	for u(&val) {
		ls0 = append(ls0, val)
	}
	sort.Slice(ls0, func(i, j int) bool { return ls0[i] < ls0[j] })
	for v(&val) {
		ls1 = append(ls1, val)
	}
	sort.Slice(ls1, func(i, j int) bool { return ls1[i] < ls1[j] })
	return fmt.Sprintf("%s,%v,%v", s, ls0, ls1)
}

func TestRunner_Pipelines(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofn1, imp)
		beam.ParDo(s, &int64Check{
			Name: "simple",
			Want: []int{1, 2, 3},
		}, col)

		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("sequence", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		beam.Seq(s, imp, dofn1, dofn2, dofn2, dofn2, &int64Check{Name: "sequence", Want: []int{4, 5, 6}})
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("gbk", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofnKV, imp)
		gbk := beam.GroupByKey(s, col)
		beam.Seq(s, gbk, dofnGBK, &int64Check{Name: "gbk", Want: []int{9, 12}})
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("gbk2", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofnKV2, imp)
		gbk := beam.GroupByKey(s, col)
		beam.Seq(s, gbk, dofnGBK2, &stringCheck{Name: "gbk2", Want: []string{"aaa", "bbb"}})
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("gbk3", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofnKV3, imp)
		gbk := beam.GroupByKey(s, col)
		beam.Seq(s, gbk, dofnGBK3, &stringCheck{Name: "gbk3", Want: []string{"{a 1}: {a 1}"}})
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("sink_nooutputs", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		beam.ParDo0(s, dofnSink, imp)
		pr, err := executeWithT(context.Background(), t, p)
		if err != nil {
			t.Fatal(err)
		}
		qr := pr.Metrics().Query(func(sr metrics.SingleResult) bool {
			return sr.Name() == "sunk"
		})
		if got, want := qr.Counters()[0].Committed, int64(73); got != want {
			t.Errorf("pr.Metrics.Query(Name = \"sunk\")).Committed = %v, want %v", got, want)
		}
	})
	t.Run("fork_impulse", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col1 := beam.ParDo(s, dofn1, imp)
		col2 := beam.ParDo(s, dofn1, imp)
		beam.ParDo(s, &int64Check{
			Name: "fork check1",
			Want: []int{1, 2, 3},
		}, col1)
		beam.ParDo(s, &int64Check{
			Name: "fork check2",
			Want: []int{1, 2, 3},
		}, col2)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("fork_postDoFn", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col := beam.ParDo(s, dofn1, imp)
		beam.ParDo(s, &int64Check{
			Name: "fork check1",
			Want: []int{1, 2, 3},
		}, col)
		beam.ParDo(s, &int64Check{
			Name: "fork check2",
			Want: []int{1, 2, 3},
		}, col)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("fork_multipleOutputs1", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col1, col2 := beam.ParDo2(s, dofn1x2, imp)
		beam.ParDo(s, &int64Check{
			Name: "col1",
			Want: []int{1, 2, 3},
		}, col1)
		beam.ParDo(s, &int64Check{
			Name: "col2",
			Want: []int{4, 5, 6},
		}, col2)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("fork_multipleOutputs2", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col1, col2, col3, col4, col5 := beam.ParDo5(s, dofn1x5, imp)
		beam.ParDo(s, &int64Check{
			Name: "col1",
			Want: []int{1, 6},
		}, col1)
		beam.ParDo(s, &int64Check{
			Name: "col2",
			Want: []int{2, 7},
		}, col2)
		beam.ParDo(s, &int64Check{
			Name: "col3",
			Want: []int{3, 8},
		}, col3)
		beam.ParDo(s, &int64Check{
			Name: "col4",
			Want: []int{4, 9},
		}, col4)
		beam.ParDo(s, &int64Check{
			Name: "col5",
			Want: []int{5, 10},
		}, col5)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("flatten", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col1 := beam.ParDo(s, dofn1, imp)
		col2 := beam.ParDo(s, dofn1, imp)
		flat := beam.Flatten(s, col1, col2)
		beam.ParDo(s, &int64Check{
			Name: "flatten check",
			Want: []int{1, 1, 2, 2, 3, 3},
		}, flat)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("sideinput_iterable", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col1 := beam.ParDo(s, dofn1, imp)
		sum := beam.ParDo(s, dofn2x1, imp, beam.SideInput{Input: col1})
		beam.ParDo(s, &int64Check{
			Name: "iter sideinput check",
			Want: []int{6},
		}, sum)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("sideinput_iterableKV", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col1 := beam.ParDo(s, dofnKV, imp)
		keys, sum := beam.ParDo2(s, dofn2x2KV, imp, beam.SideInput{Input: col1})
		beam.ParDo(s, &stringCheck{
			Name: "iterKV sideinput check K",
			Want: []string{"a", "a", "a", "b", "b", "b"},
		}, keys)
		beam.ParDo(s, &int64Check{
			Name: "iterKV sideinput check V",
			Want: []int{21},
		}, sum)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("sideinput_multimap", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col1 := beam.ParDo(s, dofnKV, imp)
		keys := filter.Distinct(s, beam.DropValue(s, col1))
		ks, sum := beam.ParDo2(s, dofnMultiMap, keys, beam.SideInput{Input: col1})
		beam.ParDo(s, &stringCheck{
			Name: "iterKV sideinput check K",
			Want: []string{"a", "b"},
		}, ks)
		beam.ParDo(s, &int64Check{
			Name: "iterKV sideinput check V",
			Want: []int{9, 12},
		}, sum)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	// Validates the waiting on side input readiness in buffer.
	t.Run("sideinput_2iterable", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col0 := beam.ParDo(s, dofn1, imp)
		col1 := beam.ParDo(s, dofn1, imp)
		col2 := beam.ParDo(s, dofn2, col1)
		sum := beam.ParDo(s, dofn3x1, col0, beam.SideInput{Input: col1}, beam.SideInput{Input: col2})
		beam.ParDo(s, &int64Check{
			Name: "iter sideinput check",
			Want: []int{16, 17, 18},
		}, sum)
		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("window", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		col1 := beam.ParDo(s, dofnEtKV1, imp)
		wcol1 := beam.WindowInto(s, window.NewSessions(time.Second), col1)
		col2 := beam.ParDo(s, dofnEtKV2, imp)
		wcol2 := beam.WindowInto(s, window.NewSessions(time.Second), col2)
		coGBK := beam.CoGroupByKey(s, wcol1, wcol2)
		format := beam.ParDo(s, formatCoGBK2, coGBK)
		beam.ParDo(s, &stringCheck{
			Name: "window",
			Want: []string{
				"a,[1 3],[2]",
				"b,[3],[]",
				"b,[],[1 2]",
			},
		}, format)

		if _, err := executeWithT(context.Background(), t, p); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRunner_Metrics(t *testing.T) {
	t.Run("counter", func(t *testing.T) {
		p, s := beam.NewPipelineWithRoot()
		imp := beam.Impulse(s)
		beam.ParDo(s, dofn1Counter, imp)
		pr, err := executeWithT(context.Background(), t, p)
		if err != nil {
			t.Fatal(err)
		}
		qr := pr.Metrics().Query(func(sr metrics.SingleResult) bool {
			return sr.Name() == "count"
		})
		if got, want := qr.Counters()[0].Committed, int64(1); got != want {
			t.Errorf("pr.Metrics.Query(Name = \"count\")).Committed = %v, want %v", got, want)
		}
	})
}

func TestMain(m *testing.M) {
	// Can't use ptest since it causes a loop.
	if !flag.Parsed() {
		flag.Parse()
	}
	beam.Init()
	os.Exit(m.Run())
}

func init() {
	// Basic Registration
	// 	beam.RegisterFunction(identity)
	// 	beam.RegisterType(reflect.TypeOf((*source)(nil)))
	// 	beam.RegisterType(reflect.TypeOf((*discard)(nil)))

	// Generic registration
	register.Function2x0(identity)
	register.DoFn2x0[[]byte, func(int)]((*source)(nil))
	register.DoFn1x0[int]((*discard)(nil))
	register.Emitter1[int]()
}

type source struct {
	Count int
}

func (fn *source) ProcessElement(_ []byte, emit func(int)) {
	for i := 0; i < fn.Count; i++ {
		emit(i)
	}
}

func identity(v int, emit func(int)) {
	emit(v)
}

type discard struct {
	processed int
}

func (fn *discard) ProcessElement(int) {
	fn.processed++
}

// BenchmarkPipe checks basic throughput and exec overhead with everything registered.
//
// Just registered: ~700-900ns per call, 330B per DoFn, across 5 allocs per DoFn
//
// goos: linux
// goarch: amd64
// pkg: github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe/dofns=0-16         	 1657698	       763.0 ns/op	  10.49 MB/s	       763.0 ns/elm	     320 B/op	       6 allocs/op
// BenchmarkPipe/dofns=1-16         	  832784	      1294 ns/op	  12.37 MB/s	      1294 ns/elm	     656 B/op	      11 allocs/op
// BenchmarkPipe/dofns=2-16         	  633345	      1798 ns/op	  13.35 MB/s	       899.0 ns/elm	     992 B/op	      16 allocs/op
// BenchmarkPipe/dofns=3-16         	  471106	      2446 ns/op	  13.08 MB/s	       815.4 ns/elm	    1329 B/op	      21 allocs/op
// BenchmarkPipe/dofns=5-16         	  340099	      3634 ns/op	  13.21 MB/s	       726.8 ns/elm	    2001 B/op	      31 allocs/op
// BenchmarkPipe/dofns=10-16        	  183429	      6957 ns/op	  12.65 MB/s	       695.7 ns/elm	    3683 B/op	      56 allocs/op
// BenchmarkPipe/dofns=100-16       	   17956	     65986 ns/op	  12.25 MB/s	       659.9 ns/elm	   33975 B/op	     506 allocs/op
//
// Optimized w/ Generic reg: ~200-300ns per call, 150B per DoFn, across 2 allocs per DoFn
//
// goos: linux
// goarch: amd64
// pkg: github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe/dofns=0-16         	 9319206	       131.5 ns/op	  60.85 MB/s	       131.5 ns/elm	     152 B/op	       2 allocs/op
// BenchmarkPipe/dofns=1-16         	 4465477	       268.3 ns/op	  59.63 MB/s	       268.3 ns/elm	     304 B/op	       3 allocs/op
// BenchmarkPipe/dofns=2-16         	 2876710	       431.9 ns/op	  55.56 MB/s	       216.0 ns/elm	     456 B/op	       5 allocs/op
// BenchmarkPipe/dofns=3-16         	 2096349	       562.1 ns/op	  56.93 MB/s	       187.4 ns/elm	     608 B/op	       7 allocs/op
// BenchmarkPipe/dofns=5-16         	 1347927	       823.8 ns/op	  58.27 MB/s	       164.8 ns/elm	     912 B/op	      11 allocs/op
// BenchmarkPipe/dofns=10-16        	  737594	      1590 ns/op	  55.36 MB/s	       159.0 ns/elm	    1672 B/op	      21 allocs/op
// BenchmarkPipe/dofns=100-16       	   60728	     19696 ns/op	  41.02 MB/s	       197.0 ns/elm	   15357 B/op	     201 allocs/op
func BenchmarkPipe(b *testing.B) {
	makeBench := func(numDoFns int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(8 * int64(numDoFns+1))

			disc := &discard{}
			p, s := beam.NewPipelineWithRoot()
			imp := beam.Impulse(s)
			src := beam.ParDo(s, &source{Count: b.N}, imp)
			iden := src
			for i := 0; i < numDoFns; i++ {
				iden = beam.ParDo(s, identity, iden)
			}
			beam.ParDo0(s, disc, iden)
			Execute(context.TODO(), p)
			if disc.processed != b.N {
				b.Fatalf("processed dodn't match bench number: got %v want %v", disc.processed, b.N)
			}
			d := b.Elapsed()
			div := numDoFns
			if div == 0 {
				div = 1
			}
			div = div * b.N
			b.ReportMetric(float64(d)/float64(div), "ns/elm")
		}
	}
	for _, numDoFns := range []int{0, 1, 2, 3, 5, 10, 100} {
		b.Run(fmt.Sprintf("dofns=%d", numDoFns), makeBench(numDoFns))
	}
}
