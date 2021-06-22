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
	"math"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
)

// [START combine_simple_sum]
func sumInts(a, v int) int {
	return a + v
}

func init() {
	beam.RegisterFunction(sumInts)
}

func globallySumInts(s beam.Scope, ints beam.PCollection) beam.PCollection {
	return beam.Combine(s, sumInts, ints)
}

// [END combine_simple_sum]

// [START combine_custom_average]

type averageFn struct{}

type averageAccum struct {
	Count, Sum int
}

func (fn *averageFn) CreateAccumulator() averageAccum {
	return averageAccum{0, 0}
}

func (fn *averageFn) AddInput(a averageAccum, v int) averageAccum {
	return averageAccum{Count: a.Count + 1, Sum: a.Sum + v}
}

func (fn *averageFn) MergeAccumulators(a, v averageAccum) averageAccum {
	return averageAccum{Count: a.Count + v.Count, Sum: a.Sum + v.Sum}
}

func (fn *averageFn) ExtractOutput(a averageAccum) float64 {
	if a.Count == 0 {
		return math.NaN()
	}
	return float64(a.Sum) / float64(a.Count)
}

func init() {
	beam.RegisterType(reflect.TypeOf((*averageFn)(nil)))
}

// [END combine_custom_average]

func globallyAverage(s beam.Scope, ints beam.PCollection) beam.PCollection {
	// [START combine_global_average]
	average := beam.Combine(s, &averageFn{}, ints)
	// [END combine_global_average]
	return average
}

func globallyAverageWithDefault(s beam.Scope, ints beam.PCollection) beam.PCollection {
	// [START combine_global_with_default]
	// Setting combine defaults has requires no helper function in the Go SDK.
	average := beam.Combine(s, &averageFn{}, ints)

	// To add a default value:
	defaultValue := beam.Create(s, float64(0))
	avgWithDefault := beam.ParDo(s, func(d float64, iter func(*float64) bool) float64 {
		var c float64
		if iter(&c) {
			// Side input has a value, so return it.
			return c
		}
		// Otherwise, return the default
		return d
	}, defaultValue, beam.SideInput{Input: average})
	// [END combine_global_with_default]
	return avgWithDefault
}

func perKeyAverage(s beam.Scope, playerAccuracies beam.PCollection) beam.PCollection {
	// [START combine_per_key]
	avgAccuracyPerPlayer := stats.MeanPerKey(s, playerAccuracies)
	// [END combine_per_key]
	return avgAccuracyPerPlayer
}
