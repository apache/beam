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

# pytype: skip-file

import logging
from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

import sys
from apache_beam.ml.inference import utils 
import tensorflow as tf
import numpy
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils.annotations import experimental

__all__ = [
    'TFModelHandlerNumpy',
    'TFModelHandlerTensor',
]

TensorInferenceFn = Callable[
    [tf.Module, Sequence[numpy.ndarray], Optional[Dict[str, Any]], Optional[str]],
    Iterable[PredictionResult]]

def _load_model(model_uri):
  return tf.keras.models.load_model(model_uri)


def default_numpy_inference_fn(
    model: tf.Module,
    batch: Sequence[numpy.ndarray],
    inference_args: Optional[Dict[str,Any]] = None,
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  vectorized_batch = numpy.stack(batch, axis=0)
  return  utils._convert_to_result(batch, model.predict(vectorized_batch), model_id)


def default_tensor_inference_fn(
    model: tf.Module,
    batch: Sequence[tf.Tensor],
    inference_args: Optional[Dict[str,Any]] = None,
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  vectorized_batch = tf.stack(batch, axis=0)
  return utils._convert_to_result(batch, model.predict(vectorized_batch), model_id)

class TFModelHandlerNumpy(ModelHandler[numpy.ndarray,
                                             PredictionResult,
                                             tf.Module]):
  def __init__(
      self,
      model_uri: str,
      *,
      inference_fn: TensorInferenceFn = default_numpy_inference_fn):
    self._model_uri = model_uri
    self._inference_fn = inference_fn

  def load_model(self) -> tf.Module:
    """Loads and initializes a Tensorflow model for processing."""
    return _load_model(self._model_uri)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_uri = model_path if model_path else self._model_uri
    
  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      model: tf.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of numpy array and returns an Iterable of
    numpy array Predictions.

    This method stacks the n-dimensional np-array in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of numpy nd-array. These should be batchable, as this
        method will call `numpy.stack()` and pass in batched numpy nd-array with
        dimensions (batch_size, n_features, etc.) into the model's forward()
        function.
      model: A TF model.
      inference_args: any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return self._inference_fn(model, batch, inference_args, self._model_uri)

  def get_num_bytes(self, batch: Sequence[numpy.ndarray]) -> int:
    """
    Returns:
      The number of bytes of data for a batch of numpy arrays.
    """
    return sum(sys.getsizeof(element) for element in batch)

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_TF_Numpy'

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    pass


class TFModelHandlerTensor(ModelHandler[tf.Tensor,
                                             PredictionResult,
                                             tf.Module]):
  def __init__(
      self,
      model_uri: str,
      *,
      inference_fn: TensorInferenceFn = default_tensor_inference_fn):
    self._model_uri = model_uri
    self._inference_fn = inference_fn

  def load_model(self) -> tf.Module:
    """Loads and initializes a tensorflow model for processing."""
    return _load_model(self._model_uri)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_uri = model_path if model_path else self._model_uri
    
  def run_inference(
      self,
      batch: Sequence[tf.Tensor],
      model: tf.Module,
      inference_args: Optional[Dict[str, Any]] = None,
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of tf.Tensor and returns an Iterable of
    Tensor Predictions.
    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.
    Args:
      batch: A sequence of Tensors. These Tensors should be batchable, as this
        method will call `tf.stack()` and pass in batched Tensors with
        dimensions (batch_size, n_features, etc.) into the model's forward()
        function.
      model: A Tensorflow model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched
    Returns:
      An Iterable of type PredictionResult.
    """
    return self._inference_fn(model, batch, inference_args, self._model_uri)

  def get_num_bytes(self, batch: Sequence[tf.Tensor]) -> int:
    """
    Returns:
      The number of bytes of data for a batch of Tensors.
    """
    return sum(sys.getsizeof(element) for element in batch)

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_TF_Tensors'

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    pass