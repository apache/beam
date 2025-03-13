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

package funcx

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func TestIsIter(t *testing.T) {
	tests := []struct {
		Fn  any
		Exp bool
	}{
		{func(*int) {}, false},                                // no return
		{func() bool { return false }, false},                 // no value
		{func(*int) int { return 0 }, false},                  // no bool return
		{func(int) bool { return false }, false},              // no ptr value
		{func(*typex.EventTime) bool { return false }, false}, // EventTimes disallowed
		{func(*int) bool { return false }, true},
		{func(*typex.EventTime, *int) bool { return false }, false}, // EventTimes disallowed
		{func(*int, *string) bool { return false }, true},
		{func(*typex.Y, *typex.Z) bool { return false }, true},
		{func(*typex.EventTime, *int, *string) bool { return false }, false},            // EventTimes disallowed
		{func(*int, *typex.Y, *typex.Z) bool { return false }, false},                   // too many values
		{func(*typex.EventTime, *int, *typex.Y, *typex.Z) bool { return false }, false}, // too many values, EventTimes disallowed
		{func(*any) bool { return false }, false},                                       // *any is not allowed as a param
	}

	for _, test := range tests {
		val := reflect.TypeOf(test.Fn)
		if actual := IsIter(val); actual != test.Exp {
			t.Errorf("IsIter(%v) = %v, want %v", val, actual, test.Exp)
		}
	}
}

func TestIsReIter(t *testing.T) {
	tests := []struct {
		Fn  any
		Exp bool
	}{
		{func() bool { return false }, false},                                      // not returning an Iter
		{func(*int) func(*int) bool { return nil }, false},                         // takes parameters
		{func(*int) (func(*int) bool, func(*int) bool) { return nil, nil }, false}, // too many iterators
		{func() func(*int) bool { return nil }, true},
		{func() func(*typex.EventTime, *int, *string) bool { return nil }, false}, // EventTimes disallowed
	}

	for _, test := range tests {
		val := reflect.TypeOf(test.Fn)
		if actual := IsReIter(val); actual != test.Exp {
			t.Errorf("IsReIter(%v) = %v, want %v", val, actual, test.Exp)
		}
	}
}

func TestIsMultiMap(t *testing.T) {
	tests := []struct {
		Fn  any
		Exp bool
	}{
		{func(int) func(*int) bool { return nil }, true},
		{func() func(*int) bool { return nil }, false},                         // Doesn't take an input (is a ReIter)
		{func(*int) bool { return false }, false},                              // Doesn't return an iterator (is an iterator)
		{func(int) int { return 0 }, false},                                    // Doesn't return an iterator (returns a value)
		{func(string) func(*int) int { return nil }, false},                    // Returned iterator isn't a boolean return
		{func(string) func(int) bool { return nil }, false},                    // Returned iterator doesn't have a pointer receiver
		{func(string) func(*typex.EventTime, *int) bool { return nil }, false}, // EventTimes disallowed
		{func(string) func(*typex.EventTime, *int) { return nil }, false},      // Returned iterator does not have a bool return, EventTimes disallowed
	}
	for _, test := range tests {
		val := reflect.TypeOf(test.Fn)
		if actual := IsMultiMap(val); actual != test.Exp {
			t.Errorf("IsMultiMap(%v) = %v, want %v", val, actual, test.Exp)
		}
	}
}
