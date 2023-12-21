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

package internal_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/test/integration/primitives"
)

func TestMain(m *testing.M) {
	ptest.MainWithDefault(m, "testlocal")
}

func initRunner(t testing.TB) {
	t.Helper()
	if *jobopts.Endpoint == "" {
		s := jobservices.NewServer(0, internal.RunPipeline)
		*jobopts.Endpoint = s.Endpoint()
		go s.Serve()
		t.Cleanup(func() {
			*jobopts.Endpoint = ""
			s.Stop()
		})
	}
	if !jobopts.IsLoopback() {
		*jobopts.EnvironmentType = "loopback"
	}
	// Since we force loopback, avoid cross-compilation.
	f, err := os.CreateTemp("", "dummy")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	*jobopts.WorkerBinary = f.Name()
}

func execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	return universal.Execute(ctx, p)
}

func executeWithT(ctx context.Context, t testing.TB, p *beam.Pipeline) (beam.PipelineResult, error) {
	t.Helper()
	t.Log("startingTest - ", t.Name())
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	*jobopts.JobName = fmt.Sprintf("%v-%v", strings.ToLower(t.Name()), r1.Intn(1000))
	return execute(ctx, p)
}

func init() {
	// Not actually being used, but explicitly registering
	// will avoid accidentally using a different runner for
	// the tests if I change things later.
	beam.RegisterRunner("testlocal", execute)
}

func TestRunner_Pipelines(t *testing.T) {
	initRunner(t)

	tests := []struct {
		name     string
		pipeline func(s beam.Scope)
		metrics  func(t *testing.T, pr beam.PipelineResult)
	}{
		{
			name: "simple",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col := beam.ParDo(s, dofn1, imp)
				beam.ParDo(s, &int64Check{
					Name: "simple",
					Want: []int{1, 2, 3},
				}, col)
			},
		}, {
			name: "sequence",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				beam.Seq(s, imp, dofn1, dofn2, dofn2, dofn2, &int64Check{Name: "sequence", Want: []int{4, 5, 6}})
			},
		}, {
			name: "gbk",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col := beam.ParDo(s, dofnKV, imp)
				gbk := beam.GroupByKey(s, col)
				beam.Seq(s, gbk, dofnGBK, &int64Check{Name: "gbk", Want: []int{9, 12}})
			},
		}, {
			name: "gbk2",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col := beam.ParDo(s, dofnKV2, imp)
				gbk := beam.GroupByKey(s, col)
				beam.Seq(s, gbk, dofnGBK2, &stringCheck{Name: "gbk2", Want: []string{"aaa", "bbb"}})
			},
		}, {
			name: "gbk3",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col := beam.ParDo(s, dofnKV3, imp)
				gbk := beam.GroupByKey(s, col)
				beam.Seq(s, gbk, dofnGBK3, &stringCheck{Name: "gbk3", Want: []string{"{a 1}: {a 1}"}})
			},
		}, {
			name: "sink_nooutputs",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				beam.ParDo0(s, dofnSink, imp)
			},
			metrics: func(t *testing.T, pr beam.PipelineResult) {
				qr := pr.Metrics().Query(func(sr metrics.SingleResult) bool {
					return sr.Name() == "sunk"
				})
				if len(qr.Counters()) == 0 {
					t.Fatal("no metrics, expected one.")
				}
				if got, want := qr.Counters()[0].Committed, int64(73); got != want {
					t.Errorf("pr.Metrics.Query(Name = \"sunk\")).Committed = %v, want %v", got, want)
				}
			},
		}, {
			name: "fork_impulse",
			pipeline: func(s beam.Scope) {
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
			},
		}, {
			name: "fork_postDoFn",
			pipeline: func(s beam.Scope) {
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
			},
		}, {
			name: "fork_multipleOutputs1",
			pipeline: func(s beam.Scope) {
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
			},
		}, {
			name: "fork_multipleOutputs2",
			pipeline: func(s beam.Scope) {
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
			},
		}, {
			name: "flatten",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col1 := beam.ParDo(s, dofn1, imp)
				col2 := beam.ParDo(s, dofn1, imp)
				flat := beam.Flatten(s, col1, col2)
				beam.ParDo(s, &int64Check{
					Name: "flatten check",
					Want: []int{1, 1, 2, 2, 3, 3},
				}, flat)
			},
		}, {
			name: "sideinput_iterable_oneimpulse",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col1 := beam.ParDo(s, dofn1, imp)
				sum := beam.ParDo(s, dofn2x1, imp, beam.SideInput{Input: col1})
				beam.ParDo(s, &int64Check{
					Name: "iter sideinput check",
					Want: []int{6},
				}, sum)
			},
		}, {
			name: "sideinput_iterable_twoimpulse",
			pipeline: func(s beam.Scope) {
				imp1 := beam.Impulse(s)
				col1 := beam.ParDo(s, dofn1, imp1)
				imp2 := beam.Impulse(s)
				sum := beam.ParDo(s, dofn2x1, imp2, beam.SideInput{Input: col1})
				beam.ParDo(s, &int64Check{
					Name: "iter sideinput check",
					Want: []int{6},
				}, sum)
			},
		}, {
			name: "sideinput_iterableKV",
			pipeline: func(s beam.Scope) {
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
			},
		}, {
			name: "sideinput_iterableKV",
			pipeline: func(s beam.Scope) {
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
			},
		}, {
			name: "sideinput_multimap",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col1 := beam.ParDo(s, dofnKV, imp)
				keys := filter.Distinct(s, beam.DropValue(s, col1))
				ks, sum := beam.ParDo2(s, dofnMultiMap, keys, beam.SideInput{Input: col1})
				beam.ParDo(s, &stringCheck{
					Name: "multiMap sideinput check K",
					Want: []string{"a", "b"},
				}, ks)
				beam.ParDo(s, &int64Check{
					Name: "multiMap sideinput check V",
					Want: []int{9, 12},
				}, sum)
			},
		}, {
			// Ensures topological sort is correct.
			name: "sideinput_2iterable",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col0 := beam.ParDo(s, dofn1, imp)
				col1 := beam.ParDo(s, dofn1, imp)
				col2 := beam.ParDo(s, dofn2, col1)
				sum := beam.ParDo(s, dofn3x1, col0, beam.SideInput{Input: col1}, beam.SideInput{Input: col2})
				beam.ParDo(s, &int64Check{
					Name: "iter sideinput check",
					Want: []int{16, 17, 18},
				}, sum)
			},
		}, {
			name: "sideinput_sameAsMainInput",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col0 := beam.ParDo(s, dofn1, imp)
				sum := beam.ParDo(s, dofn3x1, col0, beam.SideInput{Input: col0}, beam.SideInput{Input: col0})
				beam.ParDo(s, &int64Check{
					Name: "sum sideinput check",
					Want: []int{13, 14, 15},
				}, sum)
			},
		}, {
			name: "sideinput_sameAsMainInput+Derived",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col0 := beam.ParDo(s, dofn1, imp)
				col1 := beam.ParDo(s, dofn2, col0)
				// Doesn't matter which of col0 or col1 is used.
				sum := beam.ParDo(s, dofn3x1, col0, beam.SideInput{Input: col0}, beam.SideInput{Input: col1})
				beam.ParDo(s, &int64Check{
					Name: "sum sideinput check",
					Want: []int{16, 17, 18},
				}, sum)
			},
		}, {
			// Main input is getting duplicated data, since it's being executed twice...
			// But that doesn't make any sense
			name: "sideinput_2iterable1Data2",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col0 := beam.ParDo(s, dofn1, imp)
				col1 := beam.ParDo(s, dofn2, col0)
				col2 := beam.ParDo(s, dofn2, col0)
				// Doesn't matter which of col1 or col2 is used.
				sum := beam.ParDo(s, dofn3x1, col0, beam.SideInput{Input: col2}, beam.SideInput{Input: col1})
				beam.ParDo(s, &int64Check{
					Name: "iter sideinput check",
					Want: []int{19, 20, 21},
				}, sum)
			},
		}, {
			// Re-use the same side inputs sequentially (the two consumers should be in the same stage.)
			name: "sideinput_two_2iterable1Data",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col0 := beam.ParDo(s, dofn1, imp)
				sideIn1 := beam.ParDo(s, dofn1, imp)
				sideIn2 := beam.ParDo(s, dofn1, imp)
				col1 := beam.ParDo(s, dofn3x1, col0, beam.SideInput{Input: sideIn1}, beam.SideInput{Input: sideIn2})
				sum := beam.ParDo(s, dofn3x1, col1, beam.SideInput{Input: sideIn1}, beam.SideInput{Input: sideIn2})
				beam.ParDo(s, &int64Check{
					Name: "check_sideinput_re-use",
					Want: []int{25, 26, 27},
				}, sum)
			},
		}, {
			name: "combine_perkey",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				in := beam.ParDo(s, dofn1kv, imp)
				keyedsum := beam.CombinePerKey(s, combineIntSum, in)
				sum := beam.DropKey(s, keyedsum)
				beam.ParDo(s, &int64Check{
					Name: "combine",
					Want: []int{6},
				}, sum)
			},
		}, {
			name: "combine_global",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				in := beam.ParDo(s, dofn1, imp)
				sum := beam.Combine(s, combineIntSum, in)
				beam.ParDo(s, &int64Check{
					Name: "combine",
					Want: []int{6},
				}, sum)
			},
		}, {
			name: "sdf_single_split",
			pipeline: func(s beam.Scope) {
				configs := beam.Create(s, SourceConfig{NumElements: 10, InitialSplits: 1})
				in := beam.ParDo(s, &intRangeFn{}, configs)
				beam.ParDo(s, &int64Check{
					Name: "sdf_single",
					Want: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				}, in)
			},
		}, {
			name:     "WindowedSideInputs",
			pipeline: primitives.ValidateWindowedSideInputs,
		}, {
			name:     "WindowSums_GBK",
			pipeline: primitives.WindowSums_GBK,
		}, {
			name:     "WindowSums_Lifted",
			pipeline: primitives.WindowSums_Lifted,
		}, {
			name: "ProcessContinuations_globalCombine",
			pipeline: func(s beam.Scope) {
				out := beam.ParDo(s, &selfCheckpointingDoFn{}, beam.Impulse(s))
				passert.Count(s, out, "num ints", 10)
			},
		}, {
			name: "flatten_to_sideInput",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col1 := beam.ParDo(s, dofn1, imp)
				col2 := beam.ParDo(s, dofn1, imp)
				flat := beam.Flatten(s, col1, col2)
				beam.ParDo(s, &int64Check{
					Name: "flatten check",
					Want: []int{1, 1, 2, 2, 3, 3},
				}, flat)
				passert.NonEmpty(s, flat)
			},
		}, {
			name: "gbk_into_gbk",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col1 := beam.ParDo(s, dofnKV, imp)
				gbk1 := beam.GroupByKey(s, col1)
				col2 := beam.ParDo(s, dofnGBKKV, gbk1)
				gbk2 := beam.GroupByKey(s, col2)
				out := beam.ParDo(s, dofnGBK, gbk2)
				passert.Equals(s, out, int64(9), int64(12))
			},
		}, {
			name: "lperror_gbk_into_cogbk_shared_input",
			pipeline: func(s beam.Scope) {
				want := beam.CreateList(s, []int{0})
				fruits := beam.CreateList(s, []int64{42, 42, 42})
				fruitsKV := beam.AddFixedKey(s, fruits)

				fruitsGBK := beam.GroupByKey(s, fruitsKV)
				fooKV := beam.ParDo(s, toFoo, fruitsGBK)
				fruitsFooCoGBK := beam.CoGroupByKey(s, fruitsKV, fooKV)
				got := beam.ParDo(s, toID, fruitsFooCoGBK)
				passert.Equals(s, got, want)
			},
		},
	}
	// TODO: Explicit DoFn Failure case.
	// TODO: Session windows, where some are not merged.

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			pr, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatal(err)
			}
			if test.metrics != nil {
				test.metrics(t, pr)
			}
		})
	}
}

func TestRunner_Metrics(t *testing.T) {
	initRunner(t)
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

func TestFailure(t *testing.T) {
	initRunner(t)

	p, s := beam.NewPipelineWithRoot()
	imp := beam.Impulse(s)
	beam.ParDo(s, doFnFail, imp)
	_, err := executeWithT(context.Background(), t, p)
	if err == nil {
		t.Fatalf("expected pipeline failure, but got a success")
	}
	if want := "doFnFail: failing as intended"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected pipeline failure with %q, but was %v", want, err)
	}
}

func TestRunner_Passert(t *testing.T) {
	initRunner(t)
	tests := []struct {
		name     string
		pipeline func(s beam.Scope)
		metrics  func(t *testing.T, pr beam.PipelineResult)
	}{
		{
			name: "Empty",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col1 := beam.ParDo(s, dofnEmpty, imp)
				passert.Empty(s, col1)
			},
		}, {
			name: "Equals-TwoEmpty",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col1 := beam.ParDo(s, dofnEmpty, imp)
				col2 := beam.ParDo(s, dofnEmpty, imp)
				passert.Equals(s, col1, col2)
			},
		}, {
			name: "Equals",
			pipeline: func(s beam.Scope) {
				imp := beam.Impulse(s)
				col1 := beam.ParDo(s, dofn1, imp)
				col2 := beam.ParDo(s, dofn1, imp)
				passert.Equals(s, col1, col2)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			pr, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatal(err)
			}
			if test.metrics != nil {
				test.metrics(t, pr)
			}
		})
	}
}

func toFoo(et beam.EventTime, id int, _ func(*int64) bool) (int, string) {
	return id, "ooo"
}

func toID(et beam.EventTime, id int, fruitIter func(*int64) bool, fooIter func(*string) bool) int {
	var fruit int64
	for fruitIter(&fruit) {
	}
	var foo string
	for fooIter(&foo) {
	}
	return id
}

func init() {
	register.Function3x2(toFoo)
	register.Function4x1(toID)
}

// TODO: PCollection metrics tests, in particular for element counts, in multi transform pipelines
// There's a doubling bug since we re-use the same pcollection IDs for the source & sink, and
// don't do any re-writing.

func init() {
	// Basic Registration
	// beam.RegisterFunction(identity)
	// beam.RegisterType(reflect.TypeOf((*source)(nil)))
	// beam.RegisterType(reflect.TypeOf((*discard)(nil)))

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
// No fusion (all elements encoded) (generic registration):
//
//		~2000ns per call, 2000B per DoFn, across 22 allocs per DoFn
//	 (using Basic regsitration adds 3 allocs per DoFn, a ~200 bytes, and ~200-400ns/elm)
//
// goos: linux
// goarch: amd64
// pkg: github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe/dofns=0-16         	  885811	      1333 ns/op	      1333 ns/elm	    1993 B/op	      22 allocs/op
// BenchmarkPipe/dofns=1-16         	  457683	      2636 ns/op	      2636 ns/elm	    3986 B/op	      44 allocs/op
// BenchmarkPipe/dofns=2-16         	  283699	      3975 ns/op	      1988 ns/elm	    6138 B/op	      66 allocs/op
// BenchmarkPipe/dofns=3-16         	  212767	      5689 ns/op	      1896 ns/elm	    8504 B/op	      88 allocs/op
// BenchmarkPipe/dofns=5-16         	  121842	      8279 ns/op	      1656 ns/elm	   11994 B/op	     132 allocs/op
// BenchmarkPipe/dofns=10-16        	   22059	     52877 ns/op	      5288 ns/elm	   30614 B/op	     443 allocs/op
// BenchmarkPipe/dofns=100-16       	    6614	    166364 ns/op	      1664 ns/elm	  192961 B/op	    2261 allocs/op
//
// With fusion (generic registration):
// ~200ns per call, 150B per DoFn, across 2 allocs per DoFn
// AKA comparible to Direct Runner, as expected.
//
// goos: linux
// goarch: amd64
// pkg: github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct
// cpu: 12th Gen Intel(R) Core(TM) i7-1260P
// BenchmarkPipe/dofns=0-16         	   7660638	       145.8 ns/op	       145.8 ns/elm	     152 B/op	       2 allocs/op
// BenchmarkPipe/dofns=1-16         	   3676358	       313.3 ns/op	       313.3 ns/elm	     304 B/op	       4 allocs/op
// BenchmarkPipe/dofns=2-16         	   2242688	       507.4 ns/op	       253.7 ns/elm	     457 B/op	       6 allocs/op
// BenchmarkPipe/dofns=3-16         	   1726969	       662.6 ns/op	       220.9 ns/elm	     610 B/op	       8 allocs/op
// BenchmarkPipe/dofns=5-16         	   1198765	      1005 ns/op	       201.0 ns/elm	     915 B/op	      12 allocs/op
// BenchmarkPipe/dofns=10-16        	    631459	      1874 ns/op	       187.4 ns/elm	    1679 B/op	      22 allocs/op
// BenchmarkPipe/dofns=100-16       	     57926	     19890 ns/op	       198.9 ns/elm	   15660 B/op	     206 allocs/op
func BenchmarkPipe(b *testing.B) {
	initRunner(b)
	makeBench := func(numDoFns int) func(b *testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			disc := &discard{}
			p, s := beam.NewPipelineWithRoot()
			imp := beam.Impulse(s)
			src := beam.ParDo(s, &source{Count: b.N}, imp)
			iden := src
			for i := 0; i < numDoFns; i++ {
				iden = beam.ParDo(s, identity, iden)
			}
			beam.ParDo0(s, disc, iden)
			_, err := execute(context.Background(), p)
			if err != nil {
				b.Fatal(err)
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
