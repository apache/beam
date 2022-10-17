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
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

import numpy
import pandas
from sklearn.base import BaseEstimator

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils.annotations import experimental

try:
  import joblib
except ImportError:
  # joblib is an optional dependency.
  pass

__all__ = [
    'SklearnModelHandlerNumpy',
    'SklearnModelHandlerPandas',
]


class ModelFileType(enum.Enum):
  """Defines how a model file is serialized. Options are pickle or joblib."""
  PICKLE = 1
  JOBLIB = 2


def _load_model(model_uri, file_type):
  file = FileSystems.open(model_uri, 'rb')
  if file_type == ModelFileType.PICKLE:
    return pickle.load(file)
  elif file_type == ModelFileType.JOBLIB:
    if not joblib:
      raise ImportError(
          'Could not import joblib in this execution environment. '
          'For help with managing dependencies on Python workers.'
          'see https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/'  # pylint: disable=line-too-long
      )
    return joblib.load(file)
  raise AssertionError('Unsupported serialization type.')


def _convert_to_result(
    batch: Iterable, predictions: Union[Iterable, Dict[Any, Iterable]]
) -> Iterable[PredictionResult]:
  if isinstance(predictions, dict):
    # Go from one dictionary of type: {key_type1: Iterable<val_type1>,
    # key_type2: Iterable<val_type2>, ...} where each Iterable is of
    # length batch_size, to a list of dictionaries:
    # [{key_type1: value_type1, key_type2: value_type2}]
    predictions_per_tensor = [
        dict(zip(predictions.keys(), v)) for v in zip(*predictions.values())
    ]
    return [
        PredictionResult(x, y) for x, y in zip(batch, predictions_per_tensor)
    ]
  return [PredictionResult(x, y) for x, y in zip(batch, predictions)]


class SklearnModelHandlerNumpy(ModelHandler[numpy.ndarray,
                                            PredictionResult,
                                            BaseEstimator]):
  def __init__(
      self,
      model_uri: str,
      model_file_type: ModelFileType = ModelFileType.PICKLE):
    """ Implementation of the ModelHandler interface for scikit-learn
    using numpy arrays as input.

    Example Usage::

      pcoll | RunInference(SklearnModelHandlerNumpy(model_uri="my_uri"))

    Args:
      model_uri: The URI to where the model is saved.
      model_file_type: The method of serialization of the argument.
        default=pickle
    """
    self._model_uri = model_uri
    self._model_file_type = model_file_type

  def load_model(self) -> BaseEstimator:
    """Loads and initializes a model for processing."""
    return _load_model(self._model_uri, self._model_file_type)

  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      model: BaseEstimator,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of numpy arrays.

    Args:
      batch: A sequence of examples as numpy arrays. They should
        be single examples.
      model: A numpy model or pipeline. Must implement predict(X).
        Where the parameter X is a numpy array.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    # vectorize data for better performance
    vectorized_batch = numpy.stack(batch, axis=0)
    predictions = model.predict(vectorized_batch)

    return _convert_to_result(batch, predictions)

  def get_num_bytes(self, batch: Sequence[pandas.DataFrame]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return sum(sys.getsizeof(element) for element in batch)

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_Sklearn'


@experimental(extra_message="No backwards-compatibility guarantees.")
class SklearnModelHandlerPandas(ModelHandler[pandas.DataFrame,
                                             PredictionResult,
                                             BaseEstimator]):
  def __init__(
      self,
      model_uri: str,
      model_file_type: ModelFileType = ModelFileType.PICKLE):
    """Implementation of the ModelHandler interface for scikit-learn that
    supports pandas dataframes.

    Example Usage::

      pcoll | RunInference(SklearnModelHandlerPandas(model_uri="my_uri"))

    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.

    Args:
      model_uri: The URI to where the model is saved.
      model_file_type: The method of serialization of the argument.
        default=pickle
    """
    self._model_uri = model_uri
    self._model_file_type = model_file_type

  def load_model(self) -> BaseEstimator:
    """Loads and initializes a model for processing."""
    return _load_model(self._model_uri, self._model_file_type)

  def run_inference(
      self,
      batch: Sequence[pandas.DataFrame],
      model: BaseEstimator,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of pandas dataframes.

    Args:
      batch: A sequence of examples as numpy arrays. They should
        be single examples.
      model: A dataframe model or pipeline. Must implement predict(X).
        Where the parameter X is a pandas dataframe.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    # sklearn_inference currently only supports single rowed dataframes.
    for dataframe in iter(batch):
      if dataframe.shape[0] != 1:
        raise ValueError('Only dataframes with single rows are supported.')

    # vectorize data for better performance
    vectorized_batch = pandas.concat(batch, axis=0)
    predictions = model.predict(vectorized_batch)
    splits = [
        vectorized_batch.iloc[[i]] for i in range(vectorized_batch.shape[0])
    ]

    return _convert_to_result(splits, predictions)

  def get_num_bytes(self, batch: Sequence[pandas.DataFrame]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return sum(df.memory_usage(deep=True).sum() for df in batch)

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_Sklearn'
