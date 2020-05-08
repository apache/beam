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

package beam_test

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

func init() {
	beam.RegisterFunction(identity)
	beam.RegisterFunction(identityMinus2)
	beam.RegisterFunction(mod2)
	beam.RegisterFunction(less3)
	beam.RegisterFunction(extractKV)
	beam.RegisterFunction(combineKV)
	beam.RegisterFunction(mod2Keys)
}

func identity(n int) int { return n }

func identityMinus2(n int) int { return n - 2 }

func mod2(n int) int { return n % 2 }

func less3(n int) int {
	if n < 3 {
		return 0
	}
	return 1
}

func TestPartition(t *testing.T) {
	tests := []struct {
		in   []int
		n    int
		fn   interface{}
		out0 []int
	}{
		{
			[]int{1, 2, 3, 4, 5, 6},
			2,
			mod2,
			[]int{2, 4, 6},
		},
		{
			[]int{1, 2, 3, 4, 5, 6, 7},
			11,
			mod2,
			[]int{2, 4, 6},
		},
		{
			[]int{1, 2, 3, 4, 5, 6, 7, 8},
			2,
			less3,
			[]int{1, 2},
		},
		{
			[]int{2, 3, 4, 5},
			4,
			identityMinus2,
			[]int{2},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.out0)
		out := beam.Partition(s, test.n, test.fn, in)
		passert.Equals(s, out[0], exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Partition(%v)[0] != %v: %v", test.in, test.out0, err)
		}
	}
}

type kvIntInt struct {
	K, V int
}

func extractKV(v kvIntInt) (int, int) {
	return v.K, v.V
}

func combineKV(k, v int) kvIntInt {
	return kvIntInt{k, v}
}

func mod2Keys(k, v int) int {
	return mod2(k)
}

func TestPartitionKV(t *testing.T) {
	tests := []struct {
		in   []kvIntInt
		n    int
		fn   interface{}
		out0 []kvIntInt
	}{
		{
			[]kvIntInt{{1, 1}, {1, 2}, {2, 3}, {3, 4}},
			2,
			mod2Keys,
			[]kvIntInt{{2, 3}},
		},
		{
			[]kvIntInt{{1, 1}, {1, 2}, {2, 3}, {3, 4}},
			11,
			mod2Keys,
			[]kvIntInt{{2, 3}},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.out0)
		kvs := beam.ParDo(s, extractKV, in)
		parts := beam.Partition(s, test.n, test.fn, kvs)
		out := beam.ParDo(s, combineKV, parts[0])
		passert.Equals(s, out, exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Partition(%v)[0] != %v: %v", test.in, test.out0, err)
		}
	}
}

func TestPartitionFailures(t *testing.T) {
	tests := []struct {
		in []int
		n  int
		fn interface{}
	}{
		{
			[]int{1, 2},
			1, // too small: 1 -> index 1
			mod2,
		},
		{
			[]int{1, 2, 3, 4, 5, 6},
			6, // too small: 6 -> index 6
			identity,
		},
		{
			[]int{1, 2, 3, 4},
			5,
			identityMinus2, // bad fn: 1 => index -1
		},
	}

	for _, test := range tests {
		p, s, in := ptest.CreateList(test.in)
		beam.Partition(s, test.n, test.fn, in)

		if err := ptest.Run(p); err == nil {
			t.Errorf("Partition(%v) succeeded, want error", in)
		}
	}
}

func TestPartitionFlattenIdentity(t *testing.T) {
	tests := []struct {
		in []int
		n  int
		fn interface{}
	}{
		{
			[]int{1, 2, 3, 4},
			2,
			mod2,
		},
		{
			[]int{1, 2, 3, 4, 5},
			2,
			less3,
		},
		{
			[]int{1, 2, 3, 4, 5, 6},
			7,
			identity,
		},
	}

	for _, test := range tests {
		p, s, in := ptest.CreateList(test.in)
		out := beam.Partition(s, test.n, test.fn, in)
		passert.Equals(s, beam.Flatten(s, out...), in)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Flatten(Partition(%v)) != %v: %v", test.in, test.in, err)
		}
	}
}
