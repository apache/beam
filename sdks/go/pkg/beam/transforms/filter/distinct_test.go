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

	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/filter"
)

type s struct {
	A int
	B string
}

func TestDedup(t *testing.T) {
	tests := []struct {
		dups []interface{}
		exp  []interface{}
	}{
		{
			[]interface{}{1, 2, 3},
			[]interface{}{1, 2, 3},
		},
		{
			[]interface{}{3, 2, 1},
			[]interface{}{1, 2, 3},
		},
		{
			[]interface{}{1, 1, 1, 2, 3},
			[]interface{}{1, 2, 3},
		},
		{
			[]interface{}{1, 2, 3, 2, 2, 2, 3, 1, 1, 1, 2, 3, 1},
			[]interface{}{1, 2, 3},
		},
		{
			[]interface{}{"1", "2", "3", "2", "1"},
			[]interface{}{"1", "2", "3"},
		},
		{
			[]interface{}{s{1, "a"}, s{2, "a"}, s{1, "a"}},
			[]interface{}{s{1, "a"}, s{2, "a"}},
		},
	}

	for _, test := range tests {
		p, s, in, exp := ptest.Create2(test.dups, test.exp)
		passert.Equals(s, filter.Distinct(s, in), exp)

		if err := ptest.Run(p); err != nil {
			t.Errorf("Distinct(%v) failed: %v", test.dups, err)
		}
	}
}
