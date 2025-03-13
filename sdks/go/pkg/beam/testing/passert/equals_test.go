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

package passert

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestEquals_Good(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	wantC := beam.Create(s, "c", "b", "a")
	gotC := beam.Create(s, "a", "b", "c")

	Equals(s, wantC, gotC)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

func TestEqualsList_Good(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	wantL := [3]string{"c", "b", "a"}
	gotC := beam.Create(s, "a", "b", "c")

	EqualsList(s, gotC, wantL)
	if err := ptest.Run(p); err != nil {
		t.Errorf("Pipeline failed: %v", err)
	}
}

var badEqualsTests = []struct {
	name       string
	actual     []string
	expected   []string
	errorParts []string
}{
	{
		"missing entry",
		[]string{"a", "b"},
		[]string{"a", "b", "MISSING"},
		[]string{"2 correct entries", "0 unexpected entries", "1 missing entries", "MISSING"},
	},
	{
		"unexpected entry",
		[]string{"a", "b", "UNEXPECTED"},
		[]string{"a", "b"},
		[]string{"2 correct entries", "1 unexpected entries", "0 missing entries", "UNEXPECTED"},
	},
	{
		"both kinds of problem",
		[]string{"a", "b", "UNEXPECTED"},
		[]string{"a", "b", "MISSING"},
		[]string{"2 correct entries", "1 unexpected entries", "1 missing entries", "UNEXPECTED", "MISSING"},
	},
	{
		"not enough",
		[]string{"not enough"},
		[]string{"not enough", "not enough"},
		[]string{"1 correct entries", "0 unexpected entries", "1 missing entries", "not enough"},
	},
	{
		"too many",
		[]string{"too many", "too many"},
		[]string{"too many"},
		[]string{"1 correct entries", "1 unexpected entries", "0 missing entries", "too many"},
	},
	{
		"both kinds of wrong count",
		[]string{"too many", "too many", "not enough"},
		[]string{"not enough", "too many", "not enough"},
		[]string{"2 correct entries", "1 unexpected entries", "1 missing entries", "too many", "not enough"},
	},
}

func TestEquals_Bad(t *testing.T) {
	for _, tc := range badEqualsTests {
		p, s := beam.NewPipelineWithRoot()
		out := Equals(s, beam.CreateList(s, tc.actual), beam.CreateList(s, tc.expected))
		if err := ptest.Run(p); err == nil {
			t.Errorf("%v: pipeline SUCCEEDED but should have failed; got %v", tc.name, out)
		} else {
			str := err.Error()
			missing := []string{}
			for _, part := range tc.errorParts {
				if !strings.Contains(str, part) {
					missing = append(missing, part)
				}
			}
			if len(missing) != 0 {
				t.Errorf("%v: pipeline failed correctly, but substrings %#v are not present in message:\n%v", tc.name, missing, str)
			}
		}
	}
}

func TestEqualsList_Bad(t *testing.T) {
	for _, tc := range badEqualsTests {
		p, s := beam.NewPipelineWithRoot()
		out := EqualsList(s, beam.CreateList(s, tc.actual), tc.expected)
		if err := ptest.Run(p); err == nil {
			t.Errorf("%v: pipeline SUCCEEDED but should have failed; got %v", tc.name, out)
		} else {
			str := err.Error()
			missing := []string{}
			for _, part := range tc.errorParts {
				if !strings.Contains(str, part) {
					missing = append(missing, part)
				}
			}
			if len(missing) != 0 {
				t.Errorf("%v: pipeline failed correctly, but substrings %#v are not present in message:\n%v", tc.name, missing, str)
			}
		}
	}
}

func ExampleEquals() {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "some", "example", "strings")

	Equals(s, col, "example", "some", "strings")
	err := ptest.Run(p)
	fmt.Println(err == nil)

	// Output: true
}

func ExampleEquals_pcollection() {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "some", "example", "strings")
	exp := beam.Create(s, "example", "some", "strings")

	Equals(s, col, exp)
	err := ptest.Run(p)
	fmt.Println(err == nil)

	// Output: true
}

func ExampleEqualsList() {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "example", "inputs", "here")
	list := [3]string{"here", "example", "inputs"}

	EqualsList(s, col, list)
	err := ptest.Run(p)
	fmt.Println(err == nil)

	// Output: true
}

func unwrapError(err error) error {
	if wrapper, ok := err.(interface{ Unwrap() error }); ok {
		return wrapper.Unwrap()
	}
	return err
}

func ExampleEqualsList_mismatch() {
	p, s := beam.NewPipelineWithRoot()
	col := beam.Create(s, "example", "inputs", "here")
	list := [3]string{"wrong", "inputs", "here"}

	EqualsList(s, col, list)
	err := ptest.Run(p)
	err = unwrapError(err)

	// Process error for cleaner example output, demonstrating the diff.
	processedErr := strings.SplitAfter(err.Error(), ".failIfBadEntries] failed:")
	fmt.Println(processedErr[1])

	// Output:
	// actual PCollection does not match expected values
	// =========
	// 2 correct entries (present in both)
	// =========
	// 1 unexpected entries (present in actual, missing in expected)
	// +++
	// example
	// =========
	// 1 missing entries (missing in actual, present in expected)
	// ---
	// wrong
}
