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

import enum
import pickle
import sys
from typing import Any
from typing import Iterable
from typing import List

import numpy

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.api import PredictionResult
from apache_beam.ml.inference.base import InferenceRunner
from apache_beam.ml.inference.base import ModelLoader

try:
  import joblib
except ImportError:
  # joblib is an optional dependency.
  pass


class ModelFileType(enum.Enum):
  PICKLE = 1
  JOBLIB = 2


class SklearnInferenceRunner(InferenceRunner):
  def run_inference(self, batch: List[numpy.array],
                    model: Any) -> Iterable[numpy.array]:
    # vectorize data for better performance
    vectorized_batch = numpy.stack(batch, axis=0)
    predictions = model.predict(vectorized_batch)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def get_num_bytes(self, batch: List[numpy.array]) -> int:
    """Returns the number of bytes of data for a batch."""
    return sum(sys.getsizeof(element) for element in batch)


class SklearnModelLoader(ModelLoader):
  def __init__(
      self,
      model_file_type: ModelFileType = ModelFileType.PICKLE,
      model_uri: str = ''):
    self._model_file_type = model_file_type
    self._model_uri = model_uri
    self._inference_runner = SklearnInferenceRunner()

  def load_model(self):
    """Loads and initializes a model for processing."""
    file = FileSystems.open(self._model_uri, 'rb')
    if self._model_file_type == ModelFileType.PICKLE:
      return pickle.load(file)
    elif self._model_file_type == ModelFileType.JOBLIB:
      if not joblib:
        raise ImportError(
            'Could not import joblib in this execution'
            ' environment. For help with managing dependencies on Python workers see https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/'
        )
      return joblib.load(file)
    raise AssertionError('Unsupported serialization type.')

  def get_inference_runner(self) -> SklearnInferenceRunner:
    return self._inference_runner
