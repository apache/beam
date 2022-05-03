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

from dataclasses import dataclass
from typing import Tuple
from typing import TypeVar
from typing import Union

import apache_beam as beam
from apache_beam.ml.inference import base

_K = TypeVar('_K')
_INPUT_TYPE = TypeVar('_INPUT_TYPE')
_OUTPUT_TYPE = TypeVar('_OUTPUT_TYPE')


@dataclass
class PredictionResult:
  example: _INPUT_TYPE
  inference: _OUTPUT_TYPE


@beam.typehints.with_input_types(Union[_INPUT_TYPE, Tuple[_K, _INPUT_TYPE]])
@beam.typehints.with_output_types(Union[PredictionResult, Tuple[_K, PredictionResult]])  # pylint: disable=line-too-long
class RunInference(beam.PTransform):
  """
  A transform that takes a PCollection of examples (or features) to be used on
  an ML model. It will then output inferences (or predictions) for those
  examples in a PCollection of PredictionResults, containing the input examples
  and output inferences.

  If examples are paired with keys, it will output a tuple
  (key, PredictionResult) for each (key, example) input.

  Models for supported frameworks can be loaded via a URI. Supported services
  can also be used.

  TODO(BEAM-14046): Add and link to help documentation
  """
  def __init__(self, model_loader: base.ModelLoader) -> beam.pvalue.PCollection:
    self._model_loader = model_loader

  def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
    return pcoll | base.RunInference(self._model_loader)
