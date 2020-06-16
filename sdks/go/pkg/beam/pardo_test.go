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

package beam

import (
	"testing"
)

func TestRecommendParDo(t *testing.T) {
	var tests = []struct {
		name      string
		outputDim int
		want      string
	}{
		{"zero outputs", 0, "ParDo0"},
		{"one output", 1, "ParDo"},
		{"more than 7 outputs", 10, "ParDoN"},
	}

	for _, tt := range tests {
		testName := tt.name
		t.Run(testName, func(t *testing.T) {
			got := parDoForSize(tt.outputDim)
			if got != tt.want {
				t.Errorf("RecommendParDo(%v) = %v, want %v", tt.outputDim, got, tt.want)
			}
		})
	}
}

func testFunction() int64 {
	return 42
}

func TestFormatParDoError(t *testing.T) {
	got := formatParDoError(testFunction, 2, 1)
	want := "DoFn github.com/apache/beam/sdks/go/pkg/beam.testFunction has 2 outputs, but ParDo requires 1 outputs, use ParDo2 instead."
	if got != want {
		t.Errorf("formatParDoError(testFunction,2,1) = %v, want = %v", got, want)
	}
}
