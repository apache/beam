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

import "testing"

func TestFormatCoGBKResults(t *testing.T) {
	// [START cogroupbykey_outputs]
	// Synthetic example results of a cogbk.
	results := []struct {
		Key            string
		Emails, Phones []string
	}{
		{
			Key:    "amy",
			Emails: []string{"amy@example.com"},
			Phones: []string{"111-222-3333", "333-444-5555"},
		}, {
			Key:    "carl",
			Emails: []string{"carl@email.com", "carl@example.com"},
			Phones: []string{"444-555-6666"},
		}, {
			Key:    "james",
			Emails: []string{},
			Phones: []string{"222-333-4444"},
		}, {
			Key:    "julia",
			Emails: []string{"julia@example.com"},
			Phones: []string{},
		},
	}
	// [END cogroupbykey_outputs]

	// [START cogroupbykey_formatted_outputs]
	formattedResults := []string{
		"amy; ['amy@example.com']; ['111-222-3333', '333-444-5555']",
		"carl; ['carl@email.com', 'carl@example.com']; ['444-555-6666']",
		"james; []; ['222-333-4444']",
		"julia; ['julia@example.com']; []",
	}
	// [END cogroupbykey_formatted_outputs]

	// Helper to fake iterators for testing.
	makeIter := func(vs []string) func(*string) bool {
		i := 0
		return func(v *string) bool {
			if i >= len(vs) {
				return false
			}
			*v = vs[i]
			i++
			return true
		}
	}

	for i, result := range results {
		got := formatCoGBKResults(result.Key, makeIter(result.Emails), makeIter(result.Phones))
		want := formattedResults[i]
		if got != want {
			t.Errorf("%d.%v, got %q, want %q", i, result.Key, got, want)
		}
	}
}
