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

// Package xlang contains functionality for testing cross-language transforms.
package xlang

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func CoGroupByKey(s beam.Scope, addr string, col1, col2 beam.PCollection) beam.PCollection {
	s = s.Scope("XLangTest.CoGroupByKey")
	namedInputs := map[string]beam.PCollection{"col1": col1, "col2": col2}
	outT := beam.UnnamedOutput(typex.NewCoGBK(typex.New(reflectx.Int64), typex.New(reflectx.String)))
	outs := beam.CrossLanguage(s, "beam:transforms:xlang:test:cgbk", nil, addr, namedInputs, outT)
	return outs[graph.UnnamedOutputTag]
}

func CombinePerKey(s beam.Scope, addr string, col beam.PCollection) beam.PCollection {
	s = s.Scope("XLangTest.CombinePerKey")
	outT := beam.UnnamedOutput(typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int64)))
	outs := beam.CrossLanguage(s, "beam:transforms:xlang:test:compk", nil, addr, beam.UnnamedInput(col), outT)
	return outs[graph.UnnamedOutputTag]
}

func CombineGlobally(s beam.Scope, addr string, col beam.PCollection) beam.PCollection {
	s = s.Scope("XLangTest.CombineGlobally")
	outT := beam.UnnamedOutput(typex.New(reflectx.Int64))
	outs := beam.CrossLanguage(s, "beam:transforms:xlang:test:comgl", nil, addr, beam.UnnamedInput(col), outT)
	return outs[graph.UnnamedOutputTag]
}

func Flatten(s beam.Scope, addr string, col1, col2 beam.PCollection) beam.PCollection {
	s = s.Scope("XLangTest.Flatten")
	namedInputs := map[string]beam.PCollection{"col1": col1, "col2": col2}
	outT := beam.UnnamedOutput(typex.New(reflectx.Int64))
	outs := beam.CrossLanguage(s, "beam:transforms:xlang:test:flatten", nil, addr, namedInputs, outT)
	return outs[graph.UnnamedOutputTag]
}

func GroupByKey(s beam.Scope, addr string, col beam.PCollection) beam.PCollection {
	s = s.Scope("XLangTest.GroupByKey")
	outT := beam.UnnamedOutput(typex.NewCoGBK(typex.New(reflectx.String), typex.New(reflectx.Int64)))
	outs := beam.CrossLanguage(s, "beam:transforms:xlang:test:gbk", nil, addr, beam.UnnamedInput(col), outT)
	return outs[graph.UnnamedOutputTag]
}

func Multi(s beam.Scope, addr string, main1, main2, side beam.PCollection) (mainOut, sideOut beam.PCollection) {
	s = s.Scope("XLangTest.Multi")
	namedInputs := map[string]beam.PCollection{"main1": main1, "main2": main2, "side": side}
	outT := typex.New(reflectx.String)
	namedOutputs := map[string]typex.FullType{"main": outT, "side": outT}
	multi := beam.CrossLanguage(s, "beam:transforms:xlang:test:multi", nil, addr, namedInputs, namedOutputs)
	return multi["main"], multi["side"]
}

func Partition(s beam.Scope, addr string, col beam.PCollection) (out0, out1 beam.PCollection) {
	s = s.Scope("XLangTest.Partition")
	outT := typex.New(reflectx.Int64)
	namedOutputs := map[string]typex.FullType{"0": outT, "1": outT}
	c := beam.CrossLanguage(s, "beam:transforms:xlang:test:partition", nil, addr, beam.UnnamedInput(col), namedOutputs)
	return c["0"], c["1"]
}

func Count(s beam.Scope, addr string, col beam.PCollection) beam.PCollection {
	s = s.Scope("XLang.Count")
	outT := beam.UnnamedOutput(typex.NewKV(typex.New(reflectx.String), typex.New(reflectx.Int64)))
	c := beam.CrossLanguage(s, "beam:transforms:xlang:count", nil, addr, beam.UnnamedInput(col), outT)
	return c[graph.UnnamedOutputTag]
}
