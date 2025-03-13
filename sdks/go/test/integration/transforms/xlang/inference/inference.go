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

package inference

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/inference"
)

func SklearnInference(expansionAddr string) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	inputRow := [][]int64{{0, 0}, {1, 1}}
	input := beam.CreateList(s, inputRow)
	output := []inference.PredictionResult{
		{
			Example:   []int64{0, 0},
			Inference: 0,
		},
		{
			Example:   []int64{1, 1},
			Inference: 1,
		},
	}
	outCol := inference.SklearnModel("/tmp/staged/sklearn_model").RunInference(s, input, inference.WithExpansionAddr(expansionAddr))
	passert.Equals(s, outCol, output[0], output[1])
	return p
}
