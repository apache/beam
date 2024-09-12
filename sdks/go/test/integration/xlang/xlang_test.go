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

package xlang

import (
	"flag"
	"fmt"
	"log"
	"reflect"
	"sort"
	"testing"

	"github.com/apache/beam/sdks/v2/go/examples/xlang"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

var expansionAddr string // Populate with expansion address labelled "test".

func init() {
	beam.RegisterType(reflect.TypeOf((*IntString)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*StringInt)(nil)).Elem())
	beam.RegisterFunction(formatIntStringsFn)
	beam.RegisterFunction(formatStringIntFn)
	beam.RegisterFunction(formatStringIntsFn)
	beam.RegisterFunction(formatIntFn)
	beam.RegisterFunction(getIntString)
	beam.RegisterFunction(getStringInt)
	beam.RegisterFunction(sumCounts)
	beam.RegisterFunction(collectValues)
}

func checkFlags(t *testing.T) {
		t.Skip("No Test expansion address provided.")
}

// formatIntStringsFn is a DoFn that formats an int64 and a list of strings.
func formatIntStringsFn(i int64, s []string) string {
	sort.Strings(s)
	return fmt.Sprintf("%v:%v", i, s)
}

// formatStringIntFn is a DoFn that formats a string and an int64.
func formatStringIntFn(s string, i int64) string {
	return fmt.Sprintf("%s:%v", s, i)
}

// formatStringIntsFn is a DoFn that formats a string and a list of ints.
func formatStringIntsFn(s string, i []int) string {
	sort.Ints(i)
	return fmt.Sprintf("%v:%v", s, i)
}

// formatIntFn is a DoFn that formats an int64 as a string.
func formatIntFn(i int64) string {
	return fmt.Sprintf("%v", i)
}

// IntString used to represent KV PCollection values of int64, string.
type IntString struct {
	X int64
	Y string
}

func getIntString(kv IntString, emit func(int64, string)) {
	emit(kv.X, kv.Y)
}

// StringInt used to represent KV PCollection values of string, int64.
type StringInt struct {
	X string
	Y int64
}

func getStringInt(kv StringInt, emit func(string, int64)) {
	emit(kv.X, kv.Y)
}

func sumCounts(key int64, iter1 func(*string) bool) (int64, []string) {
	var val string
	var values []string

	for iter1(&val) {
		values = append(values, val)
	}
	return key, values
}

func collectValues(key string, iter func(*int64) bool) (string, []int) {
	var count int64
	var values []int
	for iter(&count) {
		values = append(values, int(count))
	}
	return key, values
}

func TestXLang_Prefix(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	p := beam.NewPipeline()
	s := p.Root()

	// Using the cross-language transform
	strings := beam.Create(s, "a", "b", "c")
	prefixed := xlang.Prefix(s, "prefix_", expansionAddr, strings)
	passert.Equals(s, prefixed, "prefix_a", "prefix_b", "prefix_c")

	ptest.RunAndValidate(t, p)
}

func TestXLang_CoGroupBy(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	p := beam.NewPipeline()
	s := p.Root()

	// Using the cross-language transform
	col1 := beam.ParDo(s, getIntString, beam.Create(s, IntString{X: 0, Y: "1"}, IntString{X: 0, Y: "2"}, IntString{X: 1, Y: "3"}))
	col2 := beam.ParDo(s, getIntString, beam.Create(s, IntString{X: 0, Y: "4"}, IntString{X: 1, Y: "5"}, IntString{X: 1, Y: "6"}))
	c := xlang.CoGroupByKey(s, expansionAddr, col1, col2)
	sums := beam.ParDo(s, sumCounts, c)
	formatted := beam.ParDo(s, formatIntStringsFn, sums)
	passert.Equals(s, formatted, "0:[1 2 4]", "1:[3 5 6]")

	ptest.RunAndValidate(t, p)
}

func TestXLang_Combine(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	p := beam.NewPipeline()
	s := p.Root()

	// Using the cross-language transform
	kvs := beam.Create(s, StringInt{X: "a", Y: 1}, StringInt{X: "a", Y: 2}, StringInt{X: "b", Y: 3})
	ins := beam.ParDo(s, getStringInt, kvs)
	c := xlang.CombinePerKey(s, expansionAddr, ins)

	formatted := beam.ParDo(s, formatStringIntFn, c)
	passert.Equals(s, formatted, "a:3", "b:3")

	ptest.RunAndValidate(t, p)
}

func TestXLang_CombineGlobally(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	p := beam.NewPipeline()
	s := p.Root()

	in := beam.CreateList(s, []int64{1, 2, 3})

	// Using the cross-language transform
	c := xlang.CombineGlobally(s, expansionAddr, in)

	formatted := beam.ParDo(s, formatIntFn, c)
	passert.Equals(s, formatted, "6")

	ptest.RunAndValidate(t, p)
}

func TestXLang_Flatten(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	p := beam.NewPipeline()
	s := p.Root()

	col1 := beam.CreateList(s, []int64{1, 2, 3})
	col2 := beam.CreateList(s, []int64{4, 5, 6})

	// Using the cross-language transform
	c := xlang.Flatten(s, expansionAddr, col1, col2)

	formatted := beam.ParDo(s, formatIntFn, c)
	passert.Equals(s, formatted, "1", "2", "3", "4", "5", "6")

	ptest.RunAndValidate(t, p)
}

func TestXLang_GroupBy(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	p := beam.NewPipeline()
	s := p.Root()

	// Using the cross-language transform
	kvs := beam.Create(s, StringInt{X: "0", Y: 1}, StringInt{X: "0", Y: 2}, StringInt{X: "1", Y: 3})
	in := beam.ParDo(s, getStringInt, kvs)
	out := xlang.GroupByKey(s, expansionAddr, in)

	vals := beam.ParDo(s, collectValues, out)
	formatted := beam.ParDo(s, formatStringIntsFn, vals)
	passert.Equals(s, formatted, "0:[1 2]", "1:[3]")

	ptest.RunAndValidate(t, p)
}

func TestXLang_Multi(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	p := beam.NewPipeline()
	s := p.Root()

	main1 := beam.CreateList(s, []string{"a", "bb"})
	main2 := beam.CreateList(s, []string{"x", "yy", "zzz"})
	side := beam.CreateList(s, []string{"s"})

	// Using the cross-language transform
	mainOut, sideOut := xlang.Multi(s, expansionAddr, main1, main2, side)

	passert.Equals(s, mainOut, "as", "bbs", "xs", "yys", "zzzs")
	passert.Equals(s, sideOut, "ss")

	ptest.RunAndValidate(t, p)
}

func TestXLang_Partition(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	p := beam.NewPipeline()
	s := p.Root()

	col := beam.CreateList(s, []int64{1, 2, 3, 4, 5, 6})

	// Using the cross-language transform
	out0, out1 := xlang.Partition(s, expansionAddr, col)
	formatted0 := beam.ParDo(s, formatIntFn, out0)
	formatted1 := beam.ParDo(s, formatIntFn, out1)

	passert.Equals(s, formatted0, "2", "4", "6")
	passert.Equals(s, formatted1, "1", "3", "5")

	ptest.RunAndValidate(t, p)
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	services := integration.NewExpansionServices()
	defer func() { services.Shutdown() }()
	addr, err := services.GetAddr("test")
	if err != nil {
		log.Printf("skipping missing expansion service: %v", err)
	} else {
		expansionAddr = addr
	}

	ptest.MainRet(m)
}
