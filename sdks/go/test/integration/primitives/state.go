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

package primitives

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func init() {
	register.DoFn3x1[state.Provider, string, int, string](&valueStateFn{})
	register.DoFn3x1[state.Provider, string, int, string](&bagStateFn{})
	register.DoFn3x1[state.Provider, string, int, string](&combiningStateFn{})
	register.Emitter2[string, int]()
	register.Combiner1[int](&combine1{})
	register.Combiner2[string, int](&combine2{})
	register.Combiner2[string, int](&combine3{})
	register.Combiner1[int](&combine4{})
}

type valueStateFn struct {
	State1 state.Value[int]
	State2 state.Value[string]
}

func (f *valueStateFn) ProcessElement(s state.Provider, w string, c int) string {
	i, ok, err := f.State1.Read(s)
	if err != nil {
		panic(err)
	}
	if !ok {
		i = 1
	}
	err = f.State1.Write(s, i+1)
	if err != nil {
		panic(err)
	}

	j, ok, err := f.State2.Read(s)
	if err != nil {
		panic(err)
	}
	if !ok {
		j = "I"
	}
	err = f.State2.Write(s, j+"I")
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s: %v, %s", w, i, j)
}

// ValueStateParDo tests a DoFn that uses value state.
func ValueStateParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, func(w string, emit func(string, int)) {
		emit(w, 1)
	}, in)
	counts := beam.ParDo(s, &valueStateFn{State1: state.MakeValueState[int]("key1"), State2: state.MakeValueState[string]("key2")}, keyed)
	passert.Equals(s, counts, "apple: 1, I", "pear: 1, I", "peach: 1, I", "apple: 2, II", "apple: 3, III", "pear: 2, II")

	return p
}

type bagStateFn struct {
	State1 state.Bag[int]
	State2 state.Bag[string]
}

func (f *bagStateFn) ProcessElement(s state.Provider, w string, c int) string {
	i, ok, err := f.State1.Read(s)
	if err != nil {
		panic(err)
	}
	if !ok {
		i = []int{}
	}
	err = f.State1.Add(s, 1)
	if err != nil {
		panic(err)
	}

	j, ok, err := f.State2.Read(s)
	if err != nil {
		panic(err)
	}
	if !ok {
		j = []string{}
	}
	err = f.State2.Add(s, "I")
	if err != nil {
		panic(err)
	}
	sum := 0
	for _, val := range i {
		sum += val
	}
	return fmt.Sprintf("%s: %v, %s", w, sum, strings.Join(j, ","))
}

// BagStateParDo tests a DoFn that uses bag state.
func BagStateParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, func(w string, emit func(string, int)) {
		emit(w, 1)
	}, in)
	counts := beam.ParDo(s, &bagStateFn{State1: state.MakeBagState[int]("key1"), State2: state.MakeBagState[string]("key2")}, keyed)
	passert.Equals(s, counts, "apple: 0, ", "pear: 0, ", "peach: 0, ", "apple: 1, I", "apple: 2, I,I", "pear: 1, I")

	return p
}

type combiningStateFn struct {
	State0 state.Combining[int, int, int]
	State1 state.Combining[int, int, int]
	State2 state.Combining[string, string, int]
	State3 state.Combining[string, string, int]
	State4 state.Combining[int, int, int]
}

type combine1 struct{}

func (ac *combine1) MergeAccumulators(a, b int) int {
	return a + b
}

type combine2 struct{}

func (ac *combine2) MergeAccumulators(a, b string) string {
	ai, _ := strconv.Atoi(a)
	bi, _ := strconv.Atoi(b)
	return strconv.Itoa(ai + bi)
}

func (ac *combine2) ExtractOutput(a string) int {
	ai, _ := strconv.Atoi(a)
	return ai
}

type combine3 struct{}

func (ac *combine3) CreateAccumulator() string {
	return "0"
}

func (ac *combine3) MergeAccumulators(a string, b string) string {
	ai, _ := strconv.Atoi(a)
	bi, _ := strconv.Atoi(b)
	return strconv.Itoa(ai + bi)
}

func (ac *combine3) ExtractOutput(a string) int {
	ai, _ := strconv.Atoi(a)
	return ai
}

type combine4 struct{}

func (ac *combine4) AddInput(a, b int) int {
	return a + b
}

func (ac *combine4) MergeAccumulators(a, b int) int {
	return a + b
}

func (f *combiningStateFn) ProcessElement(s state.Provider, w string, c int) string {
	i, _, err := f.State0.Read(s)
	if err != nil {
		panic(err)
	}
	err = f.State0.Add(s, 1)
	if err != nil {
		panic(err)
	}
	i1, _, err := f.State1.Read(s)
	if err != nil {
		panic(err)
	}
	err = f.State1.Add(s, 1)
	if err != nil {
		panic(err)
	}
	i2, _, err := f.State2.Read(s)
	if err != nil {
		panic(err)
	}
	err = f.State2.Add(s, "1")
	if err != nil {
		panic(err)
	}
	i3, _, err := f.State3.Read(s)
	if err != nil {
		panic(err)
	}
	err = f.State3.Add(s, "1")
	if err != nil {
		panic(err)
	}
	i4, _, err := f.State4.Read(s)
	if err != nil {
		panic(err)
	}
	err = f.State4.Add(s, 1)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s: %v %v %v %v %v", w, i, i1, i2, i3, i4)
}

// CombiningStateParDo tests a DoFn that uses value state.
func CombiningStateParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, func(w string, emit func(string, int)) {
		emit(w, 1)
	}, in)
	counts := beam.ParDo(s, &combiningStateFn{
		State0: state.MakeCombiningState[int, int, int]("key0", func(a, b int) int {
			return a + b
		}),
		State1: state.Combining[int, int, int](state.MakeCombiningState[int, int, int]("key1", &combine1{})),
		State2: state.Combining[string, string, int](state.MakeCombiningState[string, string, int]("key2", &combine2{})),
		State3: state.Combining[string, string, int](state.MakeCombiningState[string, string, int]("key3", &combine3{})),
		State4: state.Combining[int, int, int](state.MakeCombiningState[int, int, int]("key4", &combine4{}))},
		keyed)
	passert.Equals(s, counts, "apple: 0 0 0 0 0", "pear: 0 0 0 0 0", "peach: 0 0 0 0 0", "apple: 1 1 1 1 1", "apple: 2 2 2 2 2", "pear: 1 1 1 1 1")

	return p
}
