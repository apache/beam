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

// Package regression contains pipeline regression tests.
package regression

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func init() {
	register.Function1x1(directFn)
	register.Function2x0(emitFn)
	register.Function3x0(emit2Fn)
	register.Function2x1(mixedFn)
	register.Function2x2(directCountFn)
	register.Function3x1(emitCountFn)
	register.Emitter1[int]()
	register.Iter1[int]()
}

func directFn(elm int) int {
	return elm + 1
}

// DirectParDo tests direct form output DoFns.
func DirectParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	direct := beam.ParDo(s, directFn, beam.Create(s, 1, 2, 3))
	passert.Sum(s, direct, "direct", 3, 9)

	return p
}

func emitFn(elm int, emit func(int)) {
	emit(elm + 1)
}

// EmitParDo tests emit form output DoFns.
func EmitParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	emit := beam.ParDo(s, emitFn, beam.Create(s, 1, 2, 3))
	passert.Sum(s, emit, "emit", 3, 9)

	return p
}

func emit2Fn(elm int, emit, emit2 func(int)) {
	emit(elm + 1)
	emit2(elm + 2)
}

// MultiEmitParDo tests double emit form output DoFns.
func MultiEmitParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	emit1, emit2 := beam.ParDo2(s, emit2Fn, beam.Create(s, 1, 2, 3))
	passert.Sum(s, emit1, "emit2_1", 3, 9)
	passert.Sum(s, emit2, "emit2_2", 3, 12)

	return p
}

func mixedFn(elm int, emit func(int)) int {
	emit(elm + 2)
	return elm + 1
}

// MixedOutputParDo tests mixed direct + emit form output DoFns.
func MixedOutputParDo() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	mixed1, mixed2 := beam.ParDo2(s, mixedFn, beam.Create(s, 1, 2, 3))
	passert.Sum(s, mixed1, "mixed_1", 3, 9)
	passert.Sum(s, mixed2, "mixed_2", 3, 12)

	return p
}

func directCountFn(_ int, values func(*int) bool) (int, error) {
	sum := 0
	var i int
	for values(&i) {
		sum += i
	}
	return sum, nil
}

// DirectParDoAfterGBK generates a pipeline with a direct-form
// ParDo after a GBK. See: BEAM-3978 and BEAM-4175.
func DirectParDoAfterGBK() *beam.Pipeline {
	p, s, col := ptest.Create([]any{1, 2, 3, 4})

	keyed := beam.GroupByKey(s, beam.AddFixedKey(s, col))
	sum := beam.ParDo(s, directCountFn, keyed)
	passert.Equals(s, beam.DropKey(s, beam.AddFixedKey(s, sum)), 10)

	return p
}

func emitCountFn(_ int, values func(*int) bool, emit func(int)) error {
	sum := 0
	var i int
	for values(&i) {
		sum += i
	}
	emit(sum)
	return nil
}

// EmitParDoAfterGBK generates a pipeline with a emit-form
// ParDo after a GBK. See: BEAM-3978 and BEAM-4175.
func EmitParDoAfterGBK() *beam.Pipeline {
	p, s, col := ptest.Create([]any{1, 2, 3, 4})

	keyed := beam.GroupByKey(s, beam.AddFixedKey(s, col))
	sum := beam.ParDo(s, emitCountFn, keyed)
	passert.Equals(s, beam.DropKey(s, beam.AddFixedKey(s, sum)), 10)

	return p
}
