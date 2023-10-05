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
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestParDo(t *testing.T) {
	p, s, input := ptest.CreateList([]string{"one", "two", "three"})
	lens := applyWordLen(s, input)
	passert.Equals(s, lens, 3, 3, 5)
	ptest.RunAndValidate(t, p)
}

func TestParDo_anon(t *testing.T) {
	p, s, input := ptest.CreateList([]string{"one", "two", "three"})
	lens := applyWordLenAnon(s, input)
	passert.Equals(s, lens, 3, 3, 5)
	ptest.RunAndValidate(t, p)
}

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

	// Helper to fake iterators for unit testing.
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

	p, s := beam.NewPipelineWithRoot()
	formattedCoGBK := coGBKExample(s)
	passert.Equals(s, formattedCoGBK, formattedResults[0], formattedResults[1], formattedResults[2], formattedResults[3])
	ptest.RunAndValidate(t, p)
}

func TestCombine(t *testing.T) {
	p, s, input := ptest.CreateList([]int{1, 2, 3})
	avg := globallyAverage(s, input)
	passert.Equals(s, avg, float64(2.0))
	ptest.RunAndValidate(t, p)
}

func TestCombineWithDefault_useDefault(t *testing.T) {
	p, s, input := ptest.CreateList([]int{})
	avg := globallyAverageWithDefault(s, input)
	passert.Equals(s, avg, float64(0))
	ptest.RunAndValidate(t, p)
}

func TestCombineWithDefault_useAverage(t *testing.T) {
	p, s, input := ptest.CreateList([]int{1, 2, 3})
	avg := globallyAverageWithDefault(s, input)
	passert.Equals(s, avg, float64(2.0))
	ptest.RunAndValidate(t, p)
}

func TestCombine_sum(t *testing.T) {
	p, s, input := ptest.CreateList([]int{1, 2, 3})
	avg := globallySumInts(s, input)
	passert.Equals(s, avg, int(6))
	ptest.RunAndValidate(t, p)
}

func TestCombine_sum_bounded(t *testing.T) {
	p, s, input := ptest.CreateList([]int{1, 2, 3})
	bound := int(4)
	avg := globallyBoundedSumInts(s, bound, input)
	passert.Equals(s, avg, bound)
	ptest.RunAndValidate(t, p)
}

type player struct {
	Name     string
	Accuracy float64
}

func splitPlayer(e player) (string, float64) {
	return e.Name, e.Accuracy
}

func mergePlayer(k string, v float64) player {
	return player{Name: k, Accuracy: v}
}

func init() {
	beam.RegisterFunction(splitPlayer)
	beam.RegisterFunction(mergePlayer)
}

func TestCombinePerKey(t *testing.T) {
	p, s, input := ptest.CreateList([]player{{"fred", 0.2}, {"velma", 0.4}, {"fred", 0.5}, {"velma", 1.0}, {"shaggy", 0.1}})
	kvs := beam.ParDo(s, splitPlayer, input)
	avg := perKeyAverage(s, kvs)
	results := beam.ParDo(s, mergePlayer, avg)
	passert.Equals(s, results, player{"fred", 0.35}, player{"velma", 0.7}, player{"shaggy", 0.1})
	ptest.RunAndValidate(t, p)
}

func TestFlatten(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	a := beam.CreateList(s, []int{1, 2, 3})
	b := beam.CreateList(s, []int{5, 7, 9})
	c := beam.CreateList(s, []int{4, 6, 8})
	merged := applyFlatten(s, a, b, c)
	passert.Equals(s, merged, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	ptest.RunAndValidate(t, p)
}

func TestPartition(t *testing.T) {
	p, s, input := ptest.CreateList([]Student{{42}, {57}, {23}, {89}, {99}, {5}})
	avg := applyPartition(s, input)
	passert.Equals(s, avg, Student{42})
	ptest.RunAndValidate(t, p)
}

func TestMultipleOutputs(t *testing.T) {
	p, s, words := ptest.CreateList([]string{"a", "the", "pjamas", "art", "candy", "MARKERmarked"})
	below, above, marked, lengths, mixedMarked := applyMultipleOut(s, words)

	passert.Equals(s, below, "a", "the", "art")
	passert.Equals(s, above, "pjamas", "candy", "MARKERmarked")
	passert.Equals(s, marked, "MARKERmarked")
	passert.Equals(s, lengths, 1, 3, 6, 3, 5, 12)
	passert.Equals(s, mixedMarked, "MARKERmarked")

	ptest.RunAndValidate(t, p)
}

func TestSideInputs(t *testing.T) {
	p, s, words := ptest.CreateList([]string{"a", "the", "pjamas", "art", "candy", "garbage"})
	above, below := addSideInput(s, words)
	passert.Equals(s, above, "pjamas", "candy", "garbage")
	passert.Equals(s, below, "a", "the", "art")
	ptest.RunAndValidate(t, p)
}

func emitOnTestKey(k string, v int, emit func(int)) {
	if k == "test" {
		emit(v)
	}
}

func init() { register.Function3x0(emitOnTestKey) }

func TestComposite(t *testing.T) {
	p, s, lines := ptest.CreateList([]string{
		"this test dataset has the word test",
		"at least twice, because to test the Composite",
		"CountWords, one needs test data to run it with",
	})
	// [START countwords_composite_call]
	// A Composite PTransform function is called like any other function.
	wordCounts := CountWords(s, lines) // returns a PCollection<KV<string,int>>
	// [END countwords_composite_call]
	testCount := beam.ParDo(s, emitOnTestKey, wordCounts)
	passert.Equals(s, testCount, 4)
	ptest.RunAndValidate(t, p)
}
