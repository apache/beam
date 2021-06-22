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

package snippets

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

// [START cogroupbykey_input_helpers]

type stringPair struct {
	K, V string
}

func splitStringPair(e stringPair) (string, string) {
	return e.K, e.V
}

func init() {
	// Register element types and DoFns.
	beam.RegisterType(reflect.TypeOf((*stringPair)(nil)).Elem())
	beam.RegisterFunction(splitStringPair)
}

// CreateAndSplit is a helper function that creates
func CreateAndSplit(s beam.Scope, input []stringPair) beam.PCollection {
	initial := beam.CreateList(s, input)
	return beam.ParDo(s, splitStringPair, initial)
}

// [END cogroupbykey_input_helpers]

// [START cogroupbykey_output_helpers]

func formatCoGBKResults(key string, emailIter, phoneIter func(*string) bool) string {
	var s string
	var emails, phones []string
	for emailIter(&s) {
		emails = append(emails, s)
	}
	for phoneIter(&s) {
		phones = append(phones, s)
	}
	// Values have no guaranteed order, sort for deterministic output.
	sort.Strings(emails)
	sort.Strings(phones)
	return fmt.Sprintf("%s; %s; %s", key, formatStringIter(emails), formatStringIter(phones))
}

func init() {
	beam.RegisterFunction(formatCoGBKResults)
}

// [END cogroupbykey_output_helpers]

func formatStringIter(vs []string) string {
	var b strings.Builder
	b.WriteRune('[')
	for i, v := range vs {
		b.WriteRune('\'')
		b.WriteString(v)
		b.WriteRune('\'')
		if i < len(vs)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteRune(']')
	return b.String()
}

func coGBKExample(s beam.Scope) beam.PCollection {
	// [START cogroupbykey_inputs]
	var emailSlice = []stringPair{
		{"amy", "amy@example.com"},
		{"carl", "carl@example.com"},
		{"julia", "julia@example.com"},
		{"carl", "carl@email.com"},
	}

	var phoneSlice = []stringPair{
		{"amy", "111-222-3333"},
		{"james", "222-333-4444"},
		{"amy", "333-444-5555"},
		{"carl", "444-555-6666"},
	}
	emails := CreateAndSplit(s.Scope("CreateEmails"), emailSlice)
	phones := CreateAndSplit(s.Scope("CreatePhones"), phoneSlice)
	// [END cogroupbykey_inputs]

	// [START cogroupbykey_outputs]
	results := beam.CoGroupByKey(s, emails, phones)

	contactLines := beam.ParDo(s, formatCoGBKResults, results)
	// [END cogroupbykey_outputs]

	return contactLines
}
