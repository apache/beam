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

import enum
import sys
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

import numpy

import tensorflow as tf
import tensorflow_hub as hub
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

__all__ = [
    'TFModelHandlerNumpy',
    'TFModelHandlerTensor',
]

TensorInferenceFn = Callable[[
    tf.Module,
    Sequence[Union[numpy.ndarray, tf.Tensor]],
    Dict[str, Any],
    Optional[str]
],
                             Iterable[PredictionResult]]


class ModelType(enum.Enum):
  """Defines how a model file should be loaded."""
  SAVED_MODEL = 1
  SAVED_WEIGHTS = 2


def _load_model(model_uri, model_type):
  if model_type == ModelType.SAVED_MODEL:
    return tf.keras.models.load_model(hub.resolve(model_uri))
  else:
    raise AssertionError('Unsupported model type for loading.')


def _load_model_from_weights(create_model_fn, weights_path):
  model = create_model_fn()
  model.load_weights(weights_path)
  return model


def default_numpy_inference_fn(
    model: tf.Module,
    batch: Sequence[numpy.ndarray],
    inference_args: Dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  vectorized_batch = numpy.stack(batch, axis=0)
  predictions = model(vectorized_batch, **inference_args)
  return utils._convert_to_result(batch, predictions, model_id)


def default_tensor_inference_fn(
    model: tf.Module,
    batch: Sequence[tf.Tensor],
    inference_args: Dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  vectorized_batch = tf.stack(batch, axis=0)
  predictions = model(vectorized_batch, **inference_args)
  return utils._convert_to_result(batch, predictions, model_id)


class TFModelHandlerNumpy(ModelHandler[numpy.ndarray,
                                       PredictionResult,
                                       tf.Module]):
  def __init__(
      self,
      model_uri: str,
      model_type: ModelType = ModelType.SAVED_MODEL,
      create_model_fn: Optional[Callable] = None,
      *,
      inference_fn: TensorInferenceFn = default_numpy_inference_fn):
    """Implementation of the ModelHandler interface for Tensorflow.

    Example Usage::

      pcoll | RunInference(TFModelHandlerNumpy(model_uri="my_uri"))

    See https://www.tensorflow.org/tutorials/keras/save_and_load for details.

    Args:
        model_uri (str): path to the trained model.
        model_type: type of model to be loaded. Defaults to SAVED_MODEL.
        create_model_fn: a function that creates and returns a new
          tensorflow model to load the saved weights.
          It should be used with ModelType.SAVED_WEIGHTS.
        inference_fn: inference function to use during RunInference.
          Defaults to default_numpy_inference_fn.

    **Supported Versions:** RunInference APIs in Apache Beam have been tested
    with Tensorflow 2.9, 2.10, 2.11.
    """
    self._model_uri = model_uri
    self._model_type = model_type
    self._inference_fn = inference_fn
    self._create_model_fn = create_model_fn

  def load_model(self) -> tf.Module:
    """Loads and initializes a Tensorflow model for processing."""
    if self._model_type == ModelType.SAVED_WEIGHTS:
      if not self._create_model_fn:
        raise ValueError(
            "Callable create_model_fn must be passed"
            "with ModelType.SAVED_WEIGHTS")
      return _load_model_from_weights(self._create_model_fn, self._model_uri)
    return _load_model(self._model_uri, self._model_type)

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

    This method stacks the n-dimensional numpy array in a vectorized format to
    optimize the inference call.

    Args:
      batch: A sequence of numpy nd-array. These should be batchable, as this
        method will call `numpy.stack()` and pass in batched numpy nd-array
        with dimensions (batch_size, n_features, etc.) into the model's
        predict() function.
      model: A Tensorflow model.
      inference_args: any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args
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


class TFModelHandlerTensor(ModelHandler[tf.Tensor, PredictionResult,
                                        tf.Module]):
  def __init__(
      self,
      model_uri: str,
      model_type: ModelType = ModelType.SAVED_MODEL,
      create_model_fn: Optional[Callable] = None,
      *,
      inference_fn: TensorInferenceFn = default_tensor_inference_fn):
    """Implementation of the ModelHandler interface for Tensorflow.

    Example Usage::

      pcoll | RunInference(TFModelHandlerTensor(model_uri="my_uri"))

    See https://www.tensorflow.org/tutorials/keras/save_and_load for details.

    Args:
        model_uri (str): path to the trained model.
        model_type: type of model to be loaded.
          Defaults to SAVED_MODEL.
        create_model_fn: a function that creates and returns a new
          tensorflow model to load the saved weights.
          It should be used with ModelType.SAVED_WEIGHTS.
        inference_fn: inference function to use during RunInference.
          Defaults to default_numpy_inference_fn.

    **Supported Versions:** RunInference APIs in Apache Beam have been tested
    with Tensorflow 2.11.
    """
    self._model_uri = model_uri
    self._model_type = model_type
    self._inference_fn = inference_fn
    self._create_model_fn = create_model_fn

  def load_model(self) -> tf.Module:
    """Loads and initializes a tensorflow model for processing."""
    if self._model_type == ModelType.SAVED_WEIGHTS:
      if not self._create_model_fn:
        raise ValueError(
            "Callable create_model_fn must be passed"
            "with ModelType.SAVED_WEIGHTS")
      return _load_model_from_weights(self._create_model_fn, self._model_uri)
    return _load_model(self._model_uri, self._model_type)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_uri = model_path if model_path else self._model_uri

  def run_inference(
      self,
      batch: Sequence[tf.Tensor],
      model: tf.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of tf.Tensor and returns an Iterable of
    Tensor Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of Tensors. These Tensors should be batchable, as this
        method will call `tf.stack()` and pass in batched Tensors with
        dimensions (batch_size, n_features, etc.) into the model's predict()
        function.
      model: A Tensorflow model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched
    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args
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
    return 'BeamML_TF_Tensor'

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    pass
