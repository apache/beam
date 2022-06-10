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
from typing import Sequence
from typing import Union

import numpy
import pandas
from sklearn.base import BaseEstimator

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.api import PredictionResult
from apache_beam.ml.inference.base import ModelHandler

try:
  import joblib
except ImportError:
  # joblib is an optional dependency.
  pass


class ModelFileType(enum.Enum):
  PICKLE = 1
  JOBLIB = 2


class SklearnModelHandler(ModelHandler[Union[numpy.ndarray, pandas.DataFrame],
                                       PredictionResult,
                                       BaseEstimator]):
  """ Implementation of the ModelHandler interface for scikit-learn.

      NOTE: This API and its implementation are under development and
      do not provide backward compatibility guarantees.
  """
  def __init__(
      self,
      model_uri: str,
      model_file_type: ModelFileType = ModelFileType.PICKLE):
    self._model_uri = model_uri
    self._model_file_type = model_file_type

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

  def run_inference(
      self,
      batch: Sequence[Union[numpy.ndarray, pandas.DataFrame]],
      model: BaseEstimator,
      **kwargs) -> Iterable[PredictionResult]:
    # TODO(github.com/apache/beam/issues/21769): Use supplied input type hint.
    if isinstance(batch[0], numpy.ndarray):
      return SklearnModelHandler._predict_np_array(batch, model)
    elif isinstance(batch[0], pandas.DataFrame):
      return SklearnModelHandler._predict_pandas_dataframe(batch, model)
    raise ValueError('Unsupported data type.')

  @staticmethod
  def _predict_np_array(batch: Sequence[numpy.ndarray],
                        model: Any) -> Iterable[PredictionResult]:
    # vectorize data for better performance
    vectorized_batch = numpy.stack(batch, axis=0)
    predictions = model.predict(vectorized_batch)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  @staticmethod
  def _predict_pandas_dataframe(batch: Sequence[pandas.DataFrame],
                                model: Any) -> Iterable[PredictionResult]:
    # sklearn_inference currently only supports single rowed dataframes.
    for dataframe in batch:
      if dataframe.shape[0] != 1:
        raise ValueError('Only dataframes with single rows are supported.')

    # vectorize data for better performance
    vectorized_batch = pandas.concat(batch, axis=0)
    predictions = model.predict(vectorized_batch)
    splits = [
        vectorized_batch.iloc[[i]] for i in range(vectorized_batch.shape[0])
    ]
    return [
        PredictionResult(example, inference) for example,
        inference in zip(splits, predictions)
    ]

  def get_num_bytes(
      self, batch: Sequence[Union[numpy.ndarray, pandas.DataFrame]]) -> int:
    """Returns the number of bytes of data for a batch."""
    if isinstance(batch[0], numpy.ndarray):
      return sum(sys.getsizeof(element) for element in batch)
    elif isinstance(batch[0], pandas.DataFrame):
      data_frames: Sequence[pandas.DataFrame] = batch
      return sum(df.memory_usage(deep=True).sum() for df in data_frames)
    raise ValueError('Unsupported data type.')
