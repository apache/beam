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

// Package primitives contains integration tests for primitives in beam.
package primitives

import (
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func init() {
	register.Function2x0(genA)
	register.Function2x0(genB)
	register.Function2x0(genC)
	register.Function2x0(genD)
	register.Function3x0(shortFn)
	register.Function5x0(joinFn)
	register.Function6x0(splitFn)
	register.Emitter2[string, int]()
	register.Emitter2[string, string]()
	register.Iter1[int]()
}

func genA(_ []byte, emit func(string, int)) {
	emit("a", 1)
	emit("a", 2)
	emit("a", 3)
	emit("b", 4)
	emit("b", 5)
	emit("c", 6)
}

func genB(_ []byte, emit func(string, int)) {
	emit("a", 7)
	emit("b", 8)
	emit("d", 9)
}

func genC(_ []byte, emit func(string, string)) {
	emit("a", "alpha")
	emit("c", "charlie")
	emit("d", "delta")
}

func genD(_ []byte, emit func(string, int)) {
	emit("a", 1)
	emit("a", 1)
	emit("a", 1)
	emit("b", 4)
	emit("b", 4)
	emit("c", 6)
	emit("c", 6)
	emit("c", 6)
	emit("c", 6)
	emit("c", 6)
}

func shortFn(_ string, ds func(*int) bool, emit func(int)) {
	var v int
	ds(&v)
	emit(v)
}

func sum(nums func(*int) bool) int {
	var ret, i int
	for nums(&i) {
		ret += i
	}
	return ret
}

func lenSum(strings func(*string) bool) int {
	var ret int
	var s string
	for strings(&s) {
		ret += len(s)
	}
	return ret
}

func joinFn(key string, as, bs func(*int) bool, cs func(*string) bool, emit func(string, int)) {
	emit(key, sum(as)+sum(bs)+lenSum(cs))
}

func splitFn(key string, v int, a, b, c, d func(int)) {
	switch key {
	case "a":
		a(v)
	case "b":
		b(v)
	case "c":
		c(v)
	case "d":
		d(v)
	default:
		panic(fmt.Sprintf("bad key: %v", key))
	}
}

// CoGBK tests CoGBK.
func CoGBK(s beam.Scope) {
	s2 := s.Scope("SubScope")
	as := beam.ParDo(s2, genA, beam.Impulse(s))
	bs := beam.ParDo(s2, genB, beam.Impulse(s))
	cs := beam.ParDo(s2, genC, beam.Impulse(s))
	grouped := beam.CoGroupByKey(s2, as, bs, cs)
	joined := beam.ParDo(s2, joinFn, grouped)

	a, b, c, d := beam.ParDo4(s, splitFn, joined)

	passert.Sum(s, a, "a", 1, 18)
	passert.Sum(s, b, "b", 1, 17)
	passert.Sum(s, c, "c", 1, 13)
	passert.Sum(s, d, "d", 1, 14)
}

// GBKShortRead tests GBK with a short read on the iterator.
func GBKShortRead(s beam.Scope) {
	ds := beam.ParDo(s, genD, beam.Impulse(s))
	grouped := beam.GroupByKey(s, ds)
	short := beam.ParDo(s, shortFn, grouped)

	passert.Sum(s, short, "shorted", 3, 11)
}

// Reshuffle tests Reshuffle.
func Reshuffle(s beam.Scope) {
	in := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	in = beam.Reshuffle(s, in)
	passert.Sum(s, in, "reshuffled", 9, 45)
}

// ReshuffleKV tests Reshuffle with KV PCollections.
func ReshuffleKV(s beam.Scope) {
	s2 := s.Scope("SubScope")
	as := beam.ParDo(s2, genA, beam.Impulse(s))
	bs := beam.ParDo(s2, genB, beam.Impulse(s))
	cs := beam.ParDo(s2, genC, beam.Impulse(s))

	as = beam.Reshuffle(s2, as)
	cs = beam.Reshuffle(s2, cs)

	grouped := beam.CoGroupByKey(s2, as, bs, cs)
	joined := beam.ParDo(s2, joinFn, grouped)

	joined = beam.Reshuffle(s, joined)

	a, b, c, d := beam.ParDo4(s, splitFn, joined)

	passert.Sum(s, a, "a", 1, 18)
	passert.Sum(s, b, "b", 1, 17)
	passert.Sum(s, c, "c", 1, 13)
	passert.Sum(s, d, "d", 1, 14)
}
