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
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
)

func emit3Fn(elm int, emit, emit2, emit3 func(int)) {
	emit(elm + 1)
	emit2(elm + 2)
	emit3(elm + 3)
}

// ParDoMultiOutput test a DoFn with multiple output.
func ParDoMultiOutput() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, 1)
	emit1, emit2, emit3 := beam.ParDo3(s, emit3Fn, in)
	passert.Sum(s, emit1, "emit1", 1, 2)
	passert.Sum(s, emit2, "emit2", 1, 3)
	passert.Sum(s, emit3, "emit3", 1, 4)

	return p
}

func sumValuesFn(_ []byte, values func(*int) bool) int {
	sum := 0
	var i int
	for values(&i) {
		sum += i
	}
	return sum
}

// ParDoSideInput computes the sum of ints using a side input.
func ParDoSideInput() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	sub := s.Scope("subscope") // Ensure scoping works with side inputs. See: BEAM-5354
	out := beam.ParDo(sub, sumValuesFn, beam.Impulse(s), beam.SideInput{Input: in})
	passert.Sum(s, out, "out", 1, 45)

	return p
}

func sumKVValuesFn(_ []byte, values func(*int, *int) bool) int {
	sum := 0
	var i, k int
	for values(&i, &k) {
		sum += i
		sum += k
	}
	return sum
}

// ParDoKVSideInput computes the sum of ints using a KV side input.
func ParDoKVSideInput() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	kv := beam.AddFixedKey(s, in) // i -> (0,i)
	out := beam.ParDo(s, sumKVValuesFn, beam.Impulse(s), beam.SideInput{Input: kv})
	passert.Sum(s, out, "out", 1, 45)

	return p
}
