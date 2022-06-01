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
import pandas
import pickle
import sys
from typing import Any
from typing import Iterable
from typing import List
from typing import Union

import numpy
from sklearn.base import BaseEstimator

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


class SklearnInferenceRunner(InferenceRunner[Union[numpy.ndarray,
                                                   pandas.DataFrame],
                                             PredictionResult,
                                             BaseEstimator]):
  def run_inference(
      self,
      batch: List[Union[numpy.ndarray, pandas.DataFrame]],
      model: BaseEstimator) -> Iterable[PredictionResult]:
    if isinstance(batch[0], numpy.ndarray):
      return SklearnInferenceRunner._predict_np_array(batch, model)
    elif isinstance(batch[0], pandas.DataFrame):
      return SklearnInferenceRunner._predict_pandas_dataframe(batch, model)

  @staticmethod
  def _predict_np_array(batch: List[numpy.ndarray],
                        model: Any) -> Iterable[PredictionResult]:
    # vectorize data for better performance
    vectorized_batch = numpy.stack(batch, axis=0)
    predictions = model.predict(vectorized_batch)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  @staticmethod
  def _predict_pandas_dataframe(batch: List[pandas.DataFrame],
                                model: Any) -> Iterable[PredictionResult]:
    # vectorize data for better performance
    vectorized_batch = pandas.concat(batch, axis=0)
    predictions = model.predict(vectorized_batch)
    splits = [vectorized_batch.loc[[i]] for i in vectorized_batch.index]
    return [
        PredictionResult(example, inference) for example,
        inference in zip(splits, predictions)
    ]

  def get_num_bytes(
      self, batch: List[Union[numpy.ndarray, pandas.DataFrame]]) -> int:
    """Returns the number of bytes of data for a batch."""
    if isinstance(batch[0], numpy.ndarray):
      return sum(sys.getsizeof(element) for element in batch)
    elif isinstance(batch[0], pandas.DataFrame):
      return sum(df.memory_usage(deep=True).sum() for df in batch)


class SklearnModelLoader(ModelLoader[numpy.ndarray,
                                     PredictionResult,
                                     BaseEstimator]):
  """ Implementation of the ModelLoader interface for scikit learn.

      NOTE: This API and its implementation are under development and
      do not provide backward compatibility guarantees.
  """
  def __init__(
      self,
      model_file_type: ModelFileType = ModelFileType.PICKLE,
      model_uri: str = ''):
    self._model_file_type = model_file_type
    self._model_uri = model_uri

  def load_model(self) -> BaseEstimator:
    """Loads and initializes a model for processing."""
    file = FileSystems.open(self._model_uri, 'rb')
    if self._model_file_type == ModelFileType.PICKLE:
      return pickle.load(file)
    elif self._model_file_type == ModelFileType.JOBLIB:
      if not joblib:
        raise ImportError(
            'Could not import joblib in this execution environment. '
            'For help with managing dependencies on Python workers.'
            'see https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/'  # pylint: disable=line-too-long
        )
      return joblib.load(file)
    raise AssertionError('Unsupported serialization type.')

  def get_inference_runner(self) -> SklearnInferenceRunner:
    return SklearnInferenceRunner()
