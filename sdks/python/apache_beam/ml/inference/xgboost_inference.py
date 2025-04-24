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

import sys
from abc import ABC
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import Union

import numpy
import pandas
import scipy

import datatable
import xgboost
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ExampleT
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import ModelT
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import PredictionT

__all__ = [
    'XGBoostModelHandler',
    'XGBoostModelHandlerNumpy',
    'XGBoostModelHandlerPandas',
    'XGBoostModelHandlerSciPy',
    'XGBoostModelHandlerDatatable'
]

XGBoostInferenceFn = Callable[[
    Sequence[object],
    Union[xgboost.Booster, xgboost.XGBModel],
    Optional[dict[str, Any]]
],
                              Iterable[PredictionResult]]


def default_xgboost_inference_fn(
    batch: Sequence[object],
    model: Union[xgboost.Booster, xgboost.XGBModel],
    inference_args: Optional[dict[str,
                                  Any]] = None) -> Iterable[PredictionResult]:
  inference_args = {} if not inference_args else inference_args

  if type(model) == xgboost.Booster:
    batch = [xgboost.DMatrix(array) for array in batch]
  predictions = [model.predict(el, **inference_args) for el in batch]

  return [PredictionResult(x, y) for x, y in zip(batch, predictions)]


class XGBoostModelHandler(ModelHandler[ExampleT, PredictionT, ModelT], ABC):
  def __init__(
      self,
      model_class: Union[Callable[..., xgboost.Booster],
                         Callable[..., xgboost.XGBModel]],
      model_state: str,
      inference_fn: XGBoostInferenceFn = default_xgboost_inference_fn,
      *,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for XGBoost.

    Example Usage::

        pcoll | RunInference(
                    XGBoostModelHandler(
                        model_class="XGBoost Model Class",
                        model_state="my_model_state.json")))

    See https://xgboost.readthedocs.io/en/stable/tutorials/saving_model.html
    for details

    Args:
      model_class: class of the XGBoost model that defines the model
        structure.
      model_state: path to a json file that contains the model's
        configuration.
      inference_fn: the inference function to use during RunInference.
        default=default_xgboost_inference_fn
      min_batch_size: optional. the minimum batch size to use when batching
        inputs.
      max_batch_size: optional. the maximum batch size to use when batching
        inputs.
      max_batch_duration_secs: optional. the maximum amount of time to buffer 
        a batch before emitting; used in streaming contexts.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.

    **Supported Versions:** RunInference APIs in Apache Beam have been tested
    with XGBoost 1.6.0 and 1.7.0

    XGBoost 1.0.0 introduced support for using JSON to save and load
    XGBoost models. XGBoost 1.6.0, additional support for Universal Binary JSON.
    It is recommended to use a model trained in XGBoost 1.6.0 or higher.
    While you should be able to load models created in older versions, there
    are no guarantees this will work as expected.

    This class is the superclass of all the various XGBoostModelhandlers
    and should not be instantiated directly. (See instead
    XGBoostModelHandlerNumpy, XGBoostModelHandlerPandas, etc.)
    """
    self._model_class = model_class
    self._model_state = model_state
    self._inference_fn = inference_fn
    self._env_vars = kwargs.get('env_vars', {})
    self._batching_kwargs = {}
    if min_batch_size is not None:
      self._batching_kwargs["min_batch_size"] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs["max_batch_size"] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs

  def load_model(self) -> Union[xgboost.Booster, xgboost.XGBModel]:
    model = self._model_class()
    model_state_file_handler = FileSystems.open(self._model_state, 'rb')
    model_state_bytes = model_state_file_handler.read()
    # Convert into a bytearray so that the
    # model state can be loaded in XGBoost
    model_state_bytearray = bytearray(model_state_bytes)
    model.load_model(model_state_bytearray)
    return model

  def get_metrics_namespace(self) -> str:
    return 'BeamML_XGBoost'

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    return self._batching_kwargs


class XGBoostModelHandlerNumpy(XGBoostModelHandler[numpy.ndarray,
                                                   PredictionResult,
                                                   Union[xgboost.Booster,
                                                         xgboost.XGBModel]]):
  """Implementation of the ModelHandler interface for XGBoost
  using numpy arrays as input.

  Example Usage::

      pcoll | RunInference(
                  XGBoostModelHandlerNumpy(
                      model_class="XGBoost Model Class",
                      model_state="my_model_state.json")))

  Args:
    model_class: class of the XGBoost model that defines the model
      structure.
    model_state: path to a json file that contains the model's
      configuration.
    inference_fn: the inference function to use during RunInference.
      default=default_xgboost_inference_fn
  """
  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      model: Union[xgboost.Booster, xgboost.XGBModel],
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of 2d numpy arrays.

    Args:
      batch: A sequence of examples as 2d numpy arrays. Each
        row in an array is a single example. The dimensions
        must match the dimensions of the data used to train
        the model.
      model: XGBoost booster or XBGModel (sklearn interface). Must
        implement predict(X). Where the parameter X is a 2d numpy array.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return self._inference_fn(batch, model, inference_args)

  def get_num_bytes(self, batch: Sequence[numpy.ndarray]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return sum(sys.getsizeof(element) for element in batch)


class XGBoostModelHandlerPandas(XGBoostModelHandler[pandas.DataFrame,
                                                    PredictionResult,
                                                    Union[xgboost.Booster,
                                                          xgboost.XGBModel]]):
  """Implementation of the ModelHandler interface for XGBoost
  using pandas dataframes as input.

  Example Usage::

      pcoll | RunInference(
                  XGBoostModelHandlerPandas(
                      model_class="XGBoost Model Class",
                      model_state="my_model_state.json")))

  Args:
    model_class: class of the XGBoost model that defines the model
      structure.
    model_state: path to a json file that contains the model's
      configuration.
    inference_fn: the inference function to use during RunInference.
      default=default_xgboost_inference_fn
  """
  def run_inference(
      self,
      batch: Sequence[pandas.DataFrame],
      model: Union[xgboost.Booster, xgboost.XGBModel],
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of pandas dataframes.

    Args:
      batch: A sequence of examples as pandas dataframes. Each
        row in a dataframe is a single example. The dimensions
        must match the dimensions of the data used to train
        the model.
      model: XGBoost booster or XBGModel (sklearn interface). Must
        implement predict(X). Where the parameter X is a pandas dataframe.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return self._inference_fn(batch, model, inference_args)

  def get_num_bytes(self, batch: Sequence[pandas.DataFrame]) -> int:
    """
    Returns:
        The number of bytes of data for a batch of Numpy arrays.
    """
    return sum(df.memory_usage(deep=True).sum() for df in batch)


class XGBoostModelHandlerSciPy(XGBoostModelHandler[scipy.sparse.csr_matrix,
                                                   PredictionResult,
                                                   Union[xgboost.Booster,
                                                         xgboost.XGBModel]]):
  """ Implementation of the ModelHandler interface for XGBoost
  using scipy matrices as input.

  Example Usage::

      pcoll | RunInference(
                  XGBoostModelHandlerSciPy(
                      model_class="XGBoost Model Class",
                      model_state="my_model_state.json")))

  Args:
    model_class: class of the XGBoost model that defines the model
      structure.
    model_state: path to a json file that contains the model's
      configuration.
    inference_fn: the inference function to use during RunInference.
      default=default_xgboost_inference_fn
  """
  def run_inference(
      self,
      batch: Sequence[scipy.sparse.csr_matrix],
      model: Union[xgboost.Booster, xgboost.XGBModel],
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of SciPy sparse matrices.

    Args:
      batch: A sequence of examples as Scipy sparse matrices.
       The dimensions must match the dimensions of the data
       used to train the model.
      model: XGBoost booster or XBGModel (sklearn interface). Must implement
        predict(X). Where the parameter X is a SciPy sparse matrix.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return self._inference_fn(batch, model, inference_args)

  def get_num_bytes(self, batch: Sequence[scipy.sparse.csr_matrix]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return sum(sys.getsizeof(element) for element in batch)


class XGBoostModelHandlerDatatable(XGBoostModelHandler[datatable.Frame,
                                                       PredictionResult,
                                                       Union[xgboost.Booster,
                                                             xgboost.XGBModel]]
                                   ):
  """Implementation of the ModelHandler interface for XGBoost
  using datatable dataframes as input.

  Example Usage::

      pcoll | RunInference(
                  XGBoostModelHandlerDatatable(
                      model_class="XGBoost Model Class",
                      model_state="my_model_state.json")))

  Args:
    model_class: class of the XGBoost model that defines the model
      structure.
    model_state: path to a json file that contains the model's
      configuration.
    inference_fn: the inference function to use during RunInference.
      default=default_xgboost_inference_fn
  """
  def run_inference(
      self,
      batch: Sequence[datatable.Frame],
      model: Union[xgboost.Booster, xgboost.XGBModel],
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of datatable dataframe.

    Args:
      batch: A sequence of examples as datatable dataframes. Each
        row in a dataframe is a single example. The dimensions
        must match the dimensions of the data used to train
        the model.
      model: XGBoost booster or XBGModel (sklearn interface). Must implement
        predict(X). Where the parameter X is a datatable dataframe.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return self._inference_fn(batch, model, inference_args)

  def get_num_bytes(self, batch: Sequence[datatable.Frame]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return sum(sys.getsizeof(element) for element in batch)
