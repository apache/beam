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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func init() {
	register.DoFn3x1[state.Provider, string, int, string](&valueStateFn{})
	register.DoFn3x1[state.Provider, string, int, string](&valueStateClearFn{})
	register.DoFn3x1[state.Provider, string, int, string](&bagStateFn{})
	register.DoFn3x1[state.Provider, string, int, string](&bagStateClearFn{})
	register.DoFn3x1[state.Provider, string, int, string](&combiningStateFn{})
	register.DoFn3x1[state.Provider, string, int, string](&mapStateFn{})
	register.DoFn3x1[state.Provider, string, int, string](&mapStateClearFn{})
	register.DoFn3x1[state.Provider, string, int, string](&setStateFn{})
	register.DoFn3x1[state.Provider, string, int, string](&setStateClearFn{})
	register.Function2x0(pairWithOne)
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

func pairWithOne(w string, emit func(string, int)) {
	emit(w, 1)
}

// ValueStateParDo tests a DoFn that uses value state.
func ValueStateParDo(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &valueStateFn{}, keyed)
	passert.Equals(s, counts, "apple: 1, I", "pear: 1, I", "peach: 1, I", "apple: 2, II", "apple: 3, III", "pear: 2, II")
}

// ValueStateParDoWindowed tests a DoFn that uses windowed value state.
func ValueStateParDoWindowed(s beam.Scope) {
	timestampedData := beam.ParDo(s, &createTimestampedData{Data: []int{1, 1, 1, 2, 2, 3, 4, 4, 4, 4}}, beam.Impulse(s))
	wData := beam.WindowInto(s, window.NewFixedWindows(3*time.Second), timestampedData)
	counts := beam.ParDo(s, &valueStateFn{State1: state.MakeValueState[int]("key1"), State2: state.MakeValueState[string]("key2")}, wData)
	globalCounts := beam.WindowInto(s, window.NewGlobalWindows(), counts)
	passert.Equals(s, globalCounts, "magic: 1, I", "magic: 2, II", "magic: 3, III", "magic: 1, I", "magic: 2, II", "magic: 3, III", "magic: 1, I", "magic: 2, II", "magic: 3, III", "magic: 1, I")
}

type valueStateClearFn struct {
	State1 state.Value[int]
}

func (f *valueStateClearFn) ProcessElement(s state.Provider, w string, c int) string {
	i, ok, err := f.State1.Read(s)
	if err != nil {
		panic(err)
	}
	if ok {
		err = f.State1.Clear(s)
		if err != nil {
			panic(err)
		}
	} else {
		err = f.State1.Write(s, 1)
		if err != nil {
			panic(err)
		}
	}

	return fmt.Sprintf("%s: %v,%v", w, i, ok)
}

// ValueStateParDoClear tests that a DoFn that uses value state can be cleared.
func ValueStateParDoClear(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear", "pear", "apple")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &valueStateClearFn{State1: state.MakeValueState[int]("key1")}, keyed)
	passert.Equals(s, counts, "apple: 0,false", "pear: 0,false", "peach: 0,false", "apple: 1,true", "apple: 0,false", "pear: 1,true", "pear: 0,false", "apple: 1,true")
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
func BagStateParDo(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &bagStateFn{}, keyed)
	passert.Equals(s, counts, "apple: 0, ", "pear: 0, ", "peach: 0, ", "apple: 1, I", "apple: 2, I,I", "pear: 1, I")
}

type bagStateClearFn struct {
	State1 state.Bag[int]
}

func (f *bagStateClearFn) ProcessElement(s state.Provider, w string, c int) string {
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

	sum := 0
	for _, val := range i {
		sum += val
	}
	if sum == 3 {
		f.State1.Clear(s)
	}
	return fmt.Sprintf("%s: %v", w, sum)
}

// BagStateParDoClear tests a DoFn that uses bag state.
func BagStateParDoClear(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "apple", "apple", "pear", "apple", "apple", "pear", "pear", "pear", "apple", "pear")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &bagStateClearFn{State1: state.MakeBagState[int]("key1")}, keyed)
	passert.Equals(s, counts, "apple: 0", "pear: 0", "apple: 1", "apple: 2", "pear: 1", "apple: 3", "apple: 0", "pear: 2", "pear: 3", "pear: 0", "apple: 1", "pear: 1")
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

func init() {
	register.Function2x1(sumInt)
}

func sumInt(a, b int) int {
	return a + b
}

// CombiningStateParDo tests a DoFn that uses value state.
func CombiningStateParDo(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &combiningStateFn{
		State0: state.MakeCombiningState[int, int, int]("key0", sumInt),
		State1: state.Combining[int, int, int](state.MakeCombiningState[int, int, int]("key1", &combine1{})),
		State2: state.Combining[string, string, int](state.MakeCombiningState[string, string, int]("key2", &combine2{})),
		State3: state.Combining[string, string, int](state.MakeCombiningState[string, string, int]("key3", &combine3{})),
		State4: state.Combining[int, int, int](state.MakeCombiningState[int, int, int]("key4", &combine4{}))},
		keyed)
	passert.Equals(s, counts, "apple: 0 0 0 0 0", "pear: 0 0 0 0 0", "peach: 0 0 0 0 0", "apple: 1 1 1 1 1", "apple: 2 2 2 2 2", "pear: 1 1 1 1 1")
}

type mapStateFn struct {
	State1 state.Map[string, int]
}

func (f *mapStateFn) ProcessElement(s state.Provider, w string, c int) string {
	i, _, err := f.State1.Get(s, w)
	if err != nil {
		panic(err)
	}
	i++
	err = f.State1.Put(s, w, i)
	if err != nil {
		panic(err)
	}
	err = f.State1.Put(s, fmt.Sprintf("%v%v", w, i), i)
	if err != nil {
		panic(err)
	}
	j, _, err := f.State1.Get(s, w)
	if err != nil {
		panic(err)
	}
	if i != j {
		panic(fmt.Sprintf("Reading state multiple times for %v produced different results: %v != %v", w, i, j))
	}

	keys, _, err := f.State1.Keys(s)
	if err != nil {
		panic(err)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	return fmt.Sprintf("%v: %v, keys: %v", w, i, keys)
}

// MapStateParDo tests a DoFn that uses value state.
func MapStateParDo(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &mapStateFn{State1: state.MakeMapState[string, int]("key1")}, keyed)
	passert.Equals(s, counts, "apple: 1, keys: [apple apple1]", "pear: 1, keys: [pear pear1]", "peach: 1, keys: [peach peach1]", "apple: 2, keys: [apple apple1 apple2]", "apple: 3, keys: [apple apple1 apple2 apple3]", "pear: 2, keys: [pear pear1 pear2]")
}

type mapStateClearFn struct {
	State1 state.Map[string, int]
}

func (f *mapStateClearFn) ProcessElement(s state.Provider, w string, c int) string {
	_, ok, err := f.State1.Get(s, w)
	if err != nil {
		panic(err)
	}
	if ok {
		f.State1.Remove(s, w)
		f.State1.Put(s, fmt.Sprintf("%v%v", w, 1), 1)
		f.State1.Put(s, fmt.Sprintf("%v%v", w, 2), 1)
		f.State1.Put(s, fmt.Sprintf("%v%v", w, 3), 1)
	} else {
		_, ok, err := f.State1.Get(s, fmt.Sprintf("%v%v", w, 1))
		if err != nil {
			panic(err)
		}
		if ok {
			f.State1.Clear(s)
		} else {
			f.State1.Put(s, w, 1)
		}
	}

	keys, _, err := f.State1.Keys(s)
	if err != nil {
		panic(err)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, k := range keys {
		_, ok, err = f.State1.Get(s, k)
		if err != nil {
			panic(err)
		}
		if !ok {
			panic(fmt.Sprintf("%v is present in keys, but not in the map", k))
		}
	}

	return fmt.Sprintf("%v: %v", w, keys)
}

// MapStateParDoClear tests clearing and removing from a DoFn that uses map state.
func MapStateParDoClear(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &mapStateClearFn{State1: state.MakeMapState[string, int]("key1")}, keyed)
	passert.Equals(s, counts, "apple: [apple]", "pear: [pear]", "peach: [peach]", "apple: [apple1 apple2 apple3]", "apple: []", "pear: [pear1 pear2 pear3]")
}

type setStateFn struct {
	State1 state.Set[string]
}

func (f *setStateFn) ProcessElement(s state.Provider, w string, c int) string {
	ok, err := f.State1.Contains(s, w)
	if err != nil {
		panic(err)
	}
	err = f.State1.Add(s, w)
	if err != nil {
		panic(err)
	}
	if ok {
		err = f.State1.Add(s, fmt.Sprintf("%v%v", w, 1))
		if err != nil {
			panic(err)
		}
	}

	keys, _, err := f.State1.Keys(s)
	if err != nil {
		panic(err)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	return fmt.Sprintf("%v: %v, keys: %v", w, ok, keys)
}

// SetStateParDo tests a DoFn that uses set state.
func SetStateParDo(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &setStateFn{State1: state.MakeSetState[string]("key1")}, keyed)
	passert.Equals(s, counts, "apple: false, keys: [apple]", "pear: false, keys: [pear]", "peach: false, keys: [peach]", "apple: true, keys: [apple apple1]", "apple: true, keys: [apple apple1]", "pear: true, keys: [pear pear1]")
}

type setStateClearFn struct {
	State1 state.Set[string]
}

func (f *setStateClearFn) ProcessElement(s state.Provider, w string, c int) string {
	ok, err := f.State1.Contains(s, w)
	if err != nil {
		panic(err)
	}
	if ok {
		f.State1.Remove(s, w)
		f.State1.Add(s, fmt.Sprintf("%v%v", w, 1))
		f.State1.Add(s, fmt.Sprintf("%v%v", w, 2))
		f.State1.Add(s, fmt.Sprintf("%v%v", w, 3))
	} else {
		ok, err := f.State1.Contains(s, fmt.Sprintf("%v%v", w, 1))
		if err != nil {
			panic(err)
		}
		if ok {
			f.State1.Clear(s)
		} else {
			f.State1.Add(s, w)
		}
	}

	keys, _, err := f.State1.Keys(s)
	if err != nil {
		panic(err)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, k := range keys {
		ok, err = f.State1.Contains(s, k)
		if err != nil {
			panic(err)
		}
		if !ok {
			panic(fmt.Sprintf("%v is present in keys, but not in the map", k))
		}
	}

	return fmt.Sprintf("%v: %v", w, keys)
}

// SetStateParDoClear tests clearing and removing from a DoFn that uses set state.
func SetStateParDoClear(s beam.Scope) {
	in := beam.Create(s, "apple", "pear", "peach", "apple", "apple", "pear")
	keyed := beam.ParDo(s, pairWithOne, in)
	counts := beam.ParDo(s, &setStateClearFn{State1: state.MakeSetState[string]("key1")}, keyed)
	passert.Equals(s, counts, "apple: [apple]", "pear: [pear]", "peach: [peach]", "apple: [apple1 apple2 apple3]", "apple: []", "pear: [pear1 pear2 pear3]")
}
