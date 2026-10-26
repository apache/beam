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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/python"
)

// stagedModelName is the name under which the test expansion service
// (sdks/python/apache_beam/runners/portability/expansion_service_test.py)
// stages the test model with the Python environment.
const stagedModelName = "sklearn_model"

// stagedSklearnModelHandler is a SklearnModelHandlerNumpy whose model_uri is
// the name of a file staged with the Python environment. Where the Python SDK
// harness puts staged files is only known on the worker, so the full path is
// resolved in load_model(), after the harness has set SEMI_PERSISTENT_DIRECTORY.
const stagedSklearnModelHandler = `import os
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy

class StagedSklearnModelHandlerNumpy(SklearnModelHandlerNumpy):
  def __init__(self, model_uri, *args, **kwargs):
    super().__init__(model_uri, *args, **kwargs)
    self._staged_name = model_uri

  def load_model(self):
    staged_dir = os.environ.get('SEMI_PERSISTENT_DIRECTORY')
    if not staged_dir:
      raise RuntimeError(
          'SEMI_PERSISTENT_DIRECTORY is not set, cannot locate staged file '
          + self._staged_name)
    self.update_model_path(
        os.path.join(staged_dir, 'staged', self._staged_name))
    return super().load_model()
`

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
	model := inference.SklearnModel(stagedModelName)
	model.ModelHandlerProvider = python.CallableSource(stagedSklearnModelHandler)
	outCol := model.RunInference(s, input, inference.WithExpansionAddr(expansionAddr))
	passert.Equals(s, outCol, output[0], output[1])
	return p
}
