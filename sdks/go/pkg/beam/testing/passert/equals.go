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
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

// Equals verifies the given collection has the same values as the given
// values, under coder equality. The values can be provided as single
// PCollection.
func Equals(s beam.Scope, col beam.PCollection, values ...interface{}) beam.PCollection {
	subScope := s.Scope("passert.Equals")
	if len(values) == 0 {
		return Empty(subScope, col)
	}
	if other, ok := values[0].(beam.PCollection); ok && len(values) == 1 {
		return equals(subScope, col, other)
	}

	other := beam.Create(subScope, values...)
	return equals(subScope, col, other)
}

// equals verifies that the actual values match the expected ones.
func equals(s beam.Scope, actual, expected beam.PCollection) beam.PCollection {
	unexpected, correct, missing := Diff(s, actual, expected)
	beam.ParDo0(s, failIfBadEntries, beam.Impulse(s), beam.SideInput{Input: unexpected}, beam.SideInput{Input: correct}, beam.SideInput{Input: missing})
	return actual
}

const (
	partSeparator = "========="
)

// failIfBadEntries checks if there are any entries in the 'unexpected' or
// 'missing' PCollections, and fails if so. The returned error message contains
// a full list of each unexpected or missing entry.
// If all the entries are in place, returns nil.
func failIfBadEntries(_ []byte, unexpected, correct, missing func(*beam.T) bool) error {
	goodCount := 0
	var dummy beam.T
	for correct(&dummy) {
		goodCount++
	}

	unexpectedStrings := readToStrings(unexpected)
	missingStrings := readToStrings(missing)

	if len(unexpectedStrings)+len(missingStrings) == 0 {
		// Hooray! No out-of-place entries; the test passes.
		return nil
	}
	outStrings := []string{
		"actual PCollection does not match expected values",
		partSeparator,
		fmt.Sprintf("%d correct entries (present in both)", goodCount),
		partSeparator,
		fmt.Sprintf("%d unexpected entries (present in actual, missing in expected)", len(unexpectedStrings)),
	}
	for _, entry := range unexpectedStrings {
		outStrings = append(outStrings, "+++", entry)
	}

	outStrings = append(
		outStrings,
		partSeparator,
		fmt.Sprintf("%d missing entries (missing in actual, present in expected)", len(missingStrings)),
	)
	for _, entry := range missingStrings {
		outStrings = append(outStrings, "---", entry)
	}
	return errors.New(strings.Join(outStrings, "\n"))
}

func readToStrings(iter func(*beam.T) bool) []string {
	out := []string{}
	var inVal beam.T
	for iter(&inVal) {
		out = append(out, fmt.Sprint(inVal))
	}
	sort.Strings(out)
	return out
}
