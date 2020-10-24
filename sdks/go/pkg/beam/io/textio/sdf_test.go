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

package textio

import (
	"context"
	"testing"
)

// TestReadSdf tests that readSdf successfully reads a test text file, and
// outputs the correct number of lines for it, even for an exceedingly long
// line.
func TestReadSdf(t *testing.T) {
	f := "../../../../data/textio_test.txt"
	f, size, err := sizeFn(context.Background(), f)
	if err != nil {
		t.Fatalf("sizing failed: %v", err)
	}

	lines := fakeReadSdfFn(t, f, size)
	want := 1
	if len(lines) != 1 {
		t.Fatalf("received %v lines, want %v", len(lines), want)
	}
}

// fakeReadSdfFn calls the methods in readSdfFn on a single input to simulate
// executing an SDF, and outputs all elements produced by that input.
func fakeReadSdfFn(t *testing.T, f string, size int64) []string {
	t.Helper()

	// emit func output
	var receivedLines []string
	getLines := func(line string) {
		receivedLines = append(receivedLines, line)
	}

	sdf := &readSdfFn{}
	rest := sdf.CreateInitialRestriction(f, size)
	splits := sdf.SplitRestriction(f, size, rest)
	for _, split := range splits {
		rt := sdf.CreateTracker(split)
		err := sdf.ProcessElement(context.Background(), rt, f, size, getLines)
		if err != nil {
			t.Fatalf("error executing readSdf.ProcessElement: %v", err)
		}
	}

	return receivedLines
}
