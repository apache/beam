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

package filter_test

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func init() {
	register.Function1x1(alwaysTrue)
	register.Function1x1(alwaysFalse)
	register.Function1x1(isOne)
	register.Function1x1(greaterThanOne)
}

func alwaysTrue(a int) bool     { return true }
func alwaysFalse(a int) bool    { return false }
func isOne(a int) bool          { return a == 1 }
func greaterThanOne(a int) bool { return a > 1 }

func TestInclude(t *testing.T) {
	tests := []struct {
		in  []int
		fn  any
		exp []int
	}{
		{
			[]int{1, 2, 3},
			func(a int) bool { return true },
			[]int{1, 2, 3},
		},
		{
			[]int{1, 2, 3},
			func(a int) bool { return a == 1 },
			[]int{1},
		},
		{
			[]int{1, 2, 3},
			func(a int) bool { return a > 1 },
			[]int{2, 3},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(s, filter.Include(s, in, test.fn), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Include(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}

func TestExclude(t *testing.T) {
	tests := []struct {
		in  []int
		fn  any
		exp []int
	}{
		{
			[]int{1, 2, 3},
			func(a int) bool { return false },
			[]int{1, 2, 3},
		},
		{
			[]int{1, 2, 3},
			func(a int) bool { return a == 1 },
			[]int{2, 3},
		},
		{
			[]int{1, 2, 3},
			func(a int) bool { return a > 1 },
			[]int{1},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.CreateList2(test.in, test.exp)
		passert.Equals(s, filter.Exclude(s, in, test.fn), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Exclude(%v) != %v: %v", test.in, test.exp, err)
		}
	}
}
