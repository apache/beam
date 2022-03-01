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

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
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
