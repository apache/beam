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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/test/integration/primitives"
)

func initRunner(t *testing.T) {
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

func executeWithT(ctx context.Context, t *testing.T, p *beam.Pipeline) (beam.PipelineResult, error) {
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
	// Job failure state reason isn't communicated with the state change over the API
	// so we can't check for a reason here.
}

// TODO: PCollection metrics tests, in particular for element counts, in multi transform pipelines
// There's a doubling bug since we re-use the same pcollection IDs for the source & sink, and
// don't do any re-writing.

func TestMain(m *testing.M) {
	ptest.MainWithDefault(m, "testlocal")
}
