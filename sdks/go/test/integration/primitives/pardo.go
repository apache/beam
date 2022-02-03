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

package primitives

import (
	"flag"
	"fmt"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func init() {
	beam.RegisterFunction(splitStringPair)
	beam.RegisterFunction(asymJoinFn)
	beam.RegisterFunction(splitByName)
}

func emit3Fn(elm int, emit, emit2, emit3 func(int)) {
	emit(elm + 1)
	emit2(elm + 2)
	emit3(elm + 3)
}

// ParDoMultiOutput test a DoFn with multiple output.
func ParDoMultiOutput() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, 1)
	emit1, emit2, emit3 := beam.ParDo3(s, emit3Fn, in)
	passert.Sum(s, emit1, "emit1", 1, 2)
	passert.Sum(s, emit2, "emit2", 1, 3)
	passert.Sum(s, emit3, "emit3", 1, 4)

	return p
}

func sumValuesFn(_ []byte, values func(*int) bool) int {
	sum := 0
	var i int
	for values(&i) {
		sum += i
	}
	return sum
}

// ParDoSideInput computes the sum of ints using a side input.
func ParDoSideInput() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	sub := s.Scope("subscope") // Ensure scoping works with side inputs. See: BEAM-5354
	out := beam.ParDo(sub, sumValuesFn, beam.Impulse(s), beam.SideInput{Input: in})
	passert.Sum(s, out, "out", 1, 45)

	return p
}

func sumKVValuesFn(_ []byte, values func(*int, *int) bool) int {
	sum := 0
	var i, k int
	for values(&i, &k) {
		sum += i
		sum += k
	}
	return sum
}

// ParDoKVSideInput computes the sum of ints using a KV side input.
func ParDoKVSideInput() *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	in := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	kv := beam.AddFixedKey(s, in) // i -> (0,i)
	out := beam.ParDo(s, sumKVValuesFn, beam.Impulse(s), beam.SideInput{Input: kv})
	passert.Sum(s, out, "out", 1, 45)

	return p
}

type stringPair struct {
	K, V string
}

func splitStringPair(e stringPair) (string, string) {
	return e.K, e.V
}

var emailSlice = []stringPair{
	{"amy", "amy@example.com"},
	{"james", "james@email.com"},
	{"carl", "carl@example.com"},
	{"julia", "julia@example.com"},
	{"carl", "carl@email.com"},
	{"james", "james@example.com"},
}

var phoneSlice = []stringPair{
	{"amy", "111-222-3333"},
	{"james", "222-333-4444"},
}

// CreateAndSplit makes a KV PCollection from a list of stringPair types
func CreateAndSplit(s beam.Scope, input []stringPair) beam.PCollection {
	initial := beam.CreateList(s, input)
	return beam.ParDo(s, splitStringPair, initial)
}

// ParDoMultiMapSideInput checks that the multimap side input access pattern
// works correctly, properly producing the correct output with an asymmetric join.
func ParDoMultiMapSideInput() *beam.Pipeline {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	emailsKV := CreateAndSplit(s.Scope("CreateEmails"), emailSlice)
	phonesKV := CreateAndSplit(s.Scope("CreatePhones"), phoneSlice)
	output := beam.ParDo(s, asymJoinFn, phonesKV, beam.SideInput{Input: emailsKV})
	passert.Count(s, output, "post-join", 2)
	amyOut, jamesOut, noMatch := beam.ParDo3(s, splitByName, output)
	passert.Equals(s, amyOut, "amy@example.com", "111-222-3333")
	passert.Equals(s, jamesOut, "james@email.com", "james@example.com", "222-333-4444")
	passert.Empty(s, noMatch)
	return p
}

func asymJoinFn(k, v string, mapSide func(string) func(*string) bool) (string, []string) {
	var out string
	results := []string{v}
	iter := mapSide(k)
	for iter(&out) {
		results = append(results, out)
	}
	return k, results
}

func splitByName(key string, vals []string, a, j, d func(string)) {
	var emitter func(string)
	switch key {
	case "amy":
		emitter = a
	case "james":
		emitter = j
	default:
		emitter = d
	}
	for _, val := range vals {
		emitter(val)
	}
}

// ParDoMultiOutput tests that a DoFn can access PipelineOptions.
func ParDoPipelineOptions() *beam.Pipeline {
	// Setup some fake flags
	flag.String("A", "", "Flag for testing.")
	flag.String("B", "", "Flag for testing.")
	flag.String("C", "", "Flag for testing.")
	flag.CommandLine.Parse([]string{"--A=123", "--B=456", "--C=789"})

	p, s := beam.NewPipelineWithRoot()

	emitted := beam.ParDo(s, emitPipelineOptions, beam.Impulse(s))
	passert.Equals(s, emitted, "A: 123", "B: 456", "C: 789")

	return p
}

func emitPipelineOptions(_ []byte, emit func(string)) {
	emit(fmt.Sprintf("%s: %s", "A", beam.PipelineOptions.Get("A")))
	emit(fmt.Sprintf("%s: %s", "B", beam.PipelineOptions.Get("B")))
	emit(fmt.Sprintf("%s: %s", "C", beam.PipelineOptions.Get("C")))
}
