#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import apache_beam as beam
from apache_beam.ml.inference.model import (
    PytorchModel, RunInferenceModel, SklearnModel)


# TODO
#@beam.typehints.with_input_types(Union[_INPUT_TYPE, Tuple[_K, _INPUT_TYPE]])
#@beam.typehints.with_output_types(Union[_OUTPUT_TYPE, Tuple[_K, _OUTPUT_TYPE]])
class RunInference(beam.PTransform):
  def __init__(self, model: RunInferenceModel, batch_size=None, **kwargs):
    self._model = model
    self._batch_size = batch_size

  def expand(self, examples: beam.PCollection) -> beam.PCollection:

    if isinstance(self._model, PytorchModel):
      # TODO defer to pytorch runinference class
      pass
    elif isinstance(self._model, SklearnModel):
      # TODO defer to sklearn runinference class
      pass
    elif isinstance(self._model, RunInferenceModel):
      # TODO:
      raise ValueError('Please pass in subclass of RunInferenceModel')
    else:
      raise ValueError('model needs to be a RunInferenceModel object')
