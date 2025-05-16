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
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional

import numpy
import pandas
from sklearn.base import BaseEstimator

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

try:
  import joblib
except ImportError:
  # joblib is an optional dependency.
  pass

__all__ = [
    'SklearnModelHandlerNumpy',
    'SklearnModelHandlerPandas',
]

NumpyInferenceFn = Callable[
    [BaseEstimator, Sequence[numpy.ndarray], Optional[dict[str, Any]]], Any]


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


def _default_numpy_inference_fn(
    model: BaseEstimator,
    batch: Sequence[numpy.ndarray],
    inference_args: Optional[dict[str, Any]] = None) -> Any:
  # vectorize data for better performance
  vectorized_batch = numpy.stack(batch, axis=0)
  return model.predict(vectorized_batch)


class SklearnModelHandlerNumpy(ModelHandler[numpy.ndarray,
                                            PredictionResult,
                                            BaseEstimator]):
  def __init__(
      self,
      model_uri: str,
      model_file_type: ModelFileType = ModelFileType.PICKLE,
      *,
      inference_fn: NumpyInferenceFn = _default_numpy_inference_fn,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      **kwargs):
    """ Implementation of the ModelHandler interface for scikit-learn
    using numpy arrays as input.

    Example Usage::

      pcoll | RunInference(SklearnModelHandlerNumpy(model_uri="my_uri"))

    Args:
      model_uri: The URI to where the model is saved.
      model_file_type: The method of serialization of the argument.
        default=pickle
      inference_fn: The inference function to use.
        default=_default_numpy_inference_fn
      min_batch_size: the minimum batch size to use when batching inputs. This
        batch will be fed into the inference_fn as a Sequence of Numpy
        ndarrays.
      max_batch_size: the maximum batch size to use when batching inputs. This
        batch will be fed into the inference_fn as a Sequence of Numpy
        ndarrays.
      max_batch_duration_secs: the maximum amount of time to buffer a batch
          before emitting; used in streaming contexts.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.
    """
    self._model_uri = model_uri
    self._model_file_type = model_file_type
    self._model_inference_fn = inference_fn
    self._batching_kwargs = {}
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs
    self._env_vars = kwargs.get('env_vars', {})
    self._share_across_processes = large_model or (model_copies is not None)
    self._model_copies = model_copies or 1

  def load_model(self) -> BaseEstimator:
    """Loads and initializes a model for processing."""
    return _load_model(self._model_uri, self._model_file_type)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_uri = model_path if model_path else self._model_uri

  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      model: BaseEstimator,
      inference_args: Optional[dict[str, Any]] = None
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
    predictions = self._model_inference_fn(
        model,
        batch,
        inference_args,
    )

    return utils._convert_to_result(
        batch, predictions, model_id=self._model_uri)

  def get_num_bytes(self, batch: Sequence[numpy.ndarray]) -> int:
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

  def batch_elements_kwargs(self):
    return self._batching_kwargs

  def share_model_across_processes(self) -> bool:
    return self._share_across_processes

  def model_copies(self) -> int:
    return self._model_copies


PandasInferenceFn = Callable[
    [BaseEstimator, Sequence[pandas.DataFrame], Optional[dict[str, Any]]], Any]


def _default_pandas_inference_fn(
    model: BaseEstimator,
    batch: Sequence[pandas.DataFrame],
    inference_args: Optional[dict[str, Any]] = None) -> Any:
  # vectorize data for better performance
  vectorized_batch = pandas.concat(batch, axis=0)
  predictions = model.predict(vectorized_batch)
  splits = [
      vectorized_batch.iloc[[i]] for i in range(vectorized_batch.shape[0])
  ]
  return predictions, splits


class SklearnModelHandlerPandas(ModelHandler[pandas.DataFrame,
                                             PredictionResult,
                                             BaseEstimator]):
  def __init__(
      self,
      model_uri: str,
      model_file_type: ModelFileType = ModelFileType.PICKLE,
      *,
      inference_fn: PandasInferenceFn = _default_pandas_inference_fn,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      **kwargs):
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
      inference_fn: The inference function to use.
        default=_default_pandas_inference_fn
      min_batch_size: the minimum batch size to use when batching inputs. This
        batch will be fed into the inference_fn as a Sequence of Pandas
        Dataframes.
      max_batch_size: the maximum batch size to use when batching inputs. This
        batch will be fed into the inference_fn as a Sequence of Pandas
        Dataframes.
      max_batch_duration_secs: the maximum amount of time to buffer a batch
          before emitting; used in streaming contexts.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.
    """
    self._model_uri = model_uri
    self._model_file_type = model_file_type
    self._model_inference_fn = inference_fn
    self._batching_kwargs = {}
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs
    self._env_vars = kwargs.get('env_vars', {})
    self._share_across_processes = large_model or (model_copies is not None)
    self._model_copies = model_copies or 1

  def load_model(self) -> BaseEstimator:
    """Loads and initializes a model for processing."""
    return _load_model(self._model_uri, self._model_file_type)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_uri = model_path if model_path else self._model_uri

  def run_inference(
      self,
      batch: Sequence[pandas.DataFrame],
      model: BaseEstimator,
      inference_args: Optional[dict[str, Any]] = None
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

    predictions, splits = self._model_inference_fn(model, batch, inference_args)

    return utils._convert_to_result(
        splits, predictions, model_id=self._model_uri)

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

  def batch_elements_kwargs(self):
    return self._batching_kwargs

  def share_model_across_processes(self) -> bool:
    return self._share_across_processes

  def model_copies(self) -> int:
    return self._model_copies
