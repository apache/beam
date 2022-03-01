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
import apache_beam as beam
from typing import Tuple, TypeVar, Union
# TODO: implement RunInferenceImpl
# from apache_beam.ml.inference.base import RunInferenceImpl


@dataclass
class BaseModelSpec:
  model_url: str


@dataclass
class PyTorchModel(BaseModelSpec):
  device: str

  def __post_init__(self):
    self.device = self.device.upper()


@dataclass
class SklearnModel(BaseModelSpec):
  serialization_type: str

  def __post_init__(self):
    self.serialization_type = self.serialization_type.upper()


_K = TypeVar('_K')
_INPUT_TYPE = TypeVar('_INPUT_TYPE')
_OUTPUT_TYPE = TypeVar('_OUTPUT_TYPE')


@dataclass
class PredictionResult:
  key: _K
  example: _INPUT_TYPE
  inference: _OUTPUT_TYPE


@beam.ptransform_fn
@beam.typehints.with_input_types(Union[_INPUT_TYPE, Tuple[_K, _INPUT_TYPE]])
@beam.typehints.with_output_types(PredictionResult)
def RunInference(
    examples: beam.pvalue.PCollection,
    model: BaseModelSpec) -> beam.pvalue.PCollection:
  """Run inference with a model.

  There one type of inference you can perform using this PTransform:
    1. In-process inference from a SavedModel instance.
    TODO: Add remote inference by using a service endpoint.

  Args:
    examples: A PCollection containing examples of the following possible kinds,
      each with their corresponding return type.
        - PCollection[Example]                 -> PCollection[PredictionResult]
        - PCollection[Tuple[K, Example]]       -> PCollection[
                                                    Tuple[K, PredictionResult]]
    model: Model inference endpoint.
  Returns:
    A PCollection (possibly keyed) containing PredictionResults.
  """
  pass
  # TODO: implement RunInferenceImpl
  # return (
  #     examples |
  #     'RunInferenceImpl' >> RunInferenceImpl(model))
