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
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

import datatable
import numpy
import pandas
import scipy
import xgboost

from apache_beam.ml.inference.base import ExampleT
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import ModelT
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import PredictionT


class XGBoostModelHandler(ModelHandler[ExampleT, PredictionT, ModelT], ABC):
  def __init__(
      self,
      model_class: Union[Callable[..., xgboost.Booster],
                         Callable[..., xgboost.XGBModel]],
      model_state: str):
    self.model_class = model_class
    self.model_state = model_state

  def load_model(self) -> Union[xgboost.Booster, xgboost.XGBModel]:
    model = self.model_class()
    model.load_model(self.model_state)
    return model

  def get_metrics_namespace(self) -> str:
    return 'BeamML_XGBoost'


class XGBoostModelHandlerNumpy(XGBoostModelHandler[numpy.ndarray,
                                                   PredictionResult,
                                                   Union[xgboost.Booster,
                                                         xgboost.XGBModel]]):
  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      model: Union[xgboost.Booster, xgboost.XGBModel],
      inference_args: Optional[Dict[str, Any]] = None) -> Iterable[PredictionT]:
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
    inference_args = {} if not inference_args else inference_args

    if type(model) == xgboost.Booster:
      batch = (xgboost.DMatrix(array) for array in batch)
    predictions = [model.predict(el, **inference_args) for el in batch]

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

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
  def run_inference(
      self,
      batch: Sequence[pandas.DataFrame],
      model: Union[xgboost.Booster, xgboost.XGBModel],
      inference_args: Optional[Dict[str, Any]] = None) -> Iterable[PredictionT]:
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
    inference_args = {} if not inference_args else inference_args

    if type(model) == xgboost.Booster:
      batch = [xgboost.DMatrix(dataframe) for dataframe in batch]
    predictions = [model.predict(el, **inference_args) for el in batch]

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

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
  def run_inference(
      self,
      batch: Sequence[scipy.sparse.csr_matrix],
      model: Union[xgboost.Booster, xgboost.XGBModel],
      inference_args: Optional[Dict[str, Any]] = None) -> Iterable[PredictionT]:
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
    inference_args = {} if not inference_args else inference_args

    if type(model) == xgboost.Booster:
      batch = [xgboost.DMatrix(sparse_matrix) for sparse_matrix in batch]
    predictions = [model.predict(el, **inference_args) for el in batch]

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

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
  def run_inference(
      self,
      batch: Sequence[datatable.Frame],
      model: Union[xgboost.Booster, xgboost.XGBModel],
      inference_args: Optional[Dict[str, Any]] = None) -> Iterable[PredictionT]:
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
    inference_args = {} if not inference_args else inference_args

    if type(model) == xgboost.Booster:
      batch = [xgboost.DMatrix(frame) for frame in batch]
    predictions = [model.predict(el, **inference_args) for el in batch]

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def get_num_bytes(self, batch: Sequence[datatable.Frame]) -> int:
    """
        Returns:
            The number of bytes of data for a batch of Numpy arrays.
        """
    return sum(sys.getsizeof(element) for element in batch)
