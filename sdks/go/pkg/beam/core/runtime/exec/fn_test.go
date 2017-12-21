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

package exec

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// TestInvoke verifies the the various forms of input to Invoke are handled correctly.
func TestInvoke(t *testing.T) {
	tests := []struct {
		Fn       interface{}
		Opt      *MainInput
		Args     []interface{}
		Expected interface{}
	}{
		{
			// Void function
			Fn: func() {},
		},
		{
			// Contexts and errors are allowed and handled.
			Fn: func(ctx context.Context) error { return nil },
		},
		{
			// Sum
			Fn:       func(a, b, c int) int { return a + b + c },
			Args:     []interface{}{1, 2, 3},
			Expected: 6,
		},
		{
			// Concat
			Fn:       func(a, b, c string) string { return a + b + c },
			Args:     []interface{}{"a", "b", "c"},
			Expected: "abc",
		},
		{
			// Length (slice type)
			Fn:       func(list []int) (int, error) { return len(list), nil },
			Args:     []interface{}{[]int{1, 2, 3}},
			Expected: 3,
		},
		{
			// Emitter
			Fn:   func(emit func(int)) { emit(1) },
			Args: []interface{}{func(int) {}},
		},
		{
			// Side input
			Fn: func(a int, get func(*int) bool) int {
				var ret int
				if !get(&ret) {
					return a
				}
				return ret
			},
			Args:     []interface{}{1, func(out *int) bool { *out = 2; return true }},
			Expected: 2,
		},
		{
			// Sum as Main
			Fn:       func(a, b, c int) int { return a + b + c },
			Opt:      &MainInput{Key: FullValue{Elm: reflect.ValueOf(1)}},
			Args:     []interface{}{2, 3},
			Expected: 6,
		},
		{
			// Sum as Main KV
			Fn:       func(a, b, c int) int { return a + b + c },
			Opt:      &MainInput{Key: FullValue{Elm: reflect.ValueOf(1), Elm2: reflect.ValueOf(2)}},
			Args:     []interface{}{3},
			Expected: 6,
		},
		{
			// EventTime
			Fn:       func(ts typex.EventTime, a int) int { return time.Time(ts).Nanosecond() + a },
			Opt:      &MainInput{Key: FullValue{Timestamp: typex.EventTime(time.Unix(0, 2)), Elm: reflect.ValueOf(1)}},
			Expected: 3,
		},
	}

	for _, test := range tests {
		fn, err := funcx.New(test.Fn)
		if err != nil {
			t.Fatalf("function not valid: %v", err)
		}

		val, err := Invoke(context.Background(), fn, test.Opt, makeArgs(test.Args)...)
		if err != nil {
			t.Fatalf("Invoke(%v,%v) failed: %v", fn.Name, test.Args, err)
		}
		if val != nil && val.Elm.Interface() != test.Expected {
			t.Errorf("Invoke(%v,%v) = %v, want %v", fn.Name, test.Args, val.Elm.Interface(), test.Expected)
		}
	}
}

func makeArgs(list []interface{}) []reflect.Value {
	var ret []reflect.Value
	for _, elm := range list {
		ret = append(ret, reflect.ValueOf(elm))
	}
	return ret
}
