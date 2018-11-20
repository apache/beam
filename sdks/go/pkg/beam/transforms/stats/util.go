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

// We use the almost vestigial util_gen.tmpl to be able to similarly generate all
// the same function identifiers more easily for shim generation.
// The generate statements live here since `go:generate` operates in filename order,
// and this is the last file in the package.

//go:generate specialize --input=util_gen.tmpl --x=integers,floats
//go:generate gofmt -w util_gen.go
//go:generate go generate util_gen.go

package stats

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func combine(s beam.Scope, makeCombineFn func(reflect.Type) interface{}, col beam.PCollection) beam.PCollection {
	t := beam.ValidateNonCompositeType(col)
	validateNonComplexNumber(t.Type())

	// Do a pipeline-construction-time type switch to select the right
	// runtime operation.
	return beam.Combine(s, makeCombineFn(t.Type()), col)
}

func combinePerKey(s beam.Scope, makeCombineFn func(reflect.Type) interface{}, col beam.PCollection) beam.PCollection {
	_, t := beam.ValidateKVType(col)
	validateNonComplexNumber(t.Type())

	// Do a pipeline-construction-time type switch to select the right
	// runtime operation.
	return beam.CombinePerKey(s, makeCombineFn(t.Type()), col)
}

func validateNonComplexNumber(t reflect.Type) {
	if !reflectx.IsNumber(t) || reflectx.IsComplex(t) {
		panic(fmt.Sprintf("type must be a non-complex number: %v", t))
	}
}
