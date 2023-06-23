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

from collections import defaultdict
import sys
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

import numpy
import tensorflow as tf
import torch
from transformers import AutoModel

from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference import utils


def _run_inference_torch_keyed_tensor(
    batch: Sequence[torch.Tensor], model: Any,
    inference_args: Dict[str, Any]) -> Iterable[PredictionResult]:
  key_to_tensor_list = defaultdict(list)
  with torch.no_grad():
    for example in batch:
      for key, tensor in example.items():
        key_to_tensor_list[key].append(tensor)
    key_to_batched_tensors = {}
    for key in key_to_tensor_list:
      batched_tensors = torch.stack(key_to_tensor_list[key])
      batched_tensors = key_to_tensor_list[key]
      key_to_batched_tensors[key] = batched_tensors
    return utils._convert_to_result(
        batch, model(**key_to_batched_tensors, **inference_args))


def _run_inference_tensorflow_keyed_tensor(
    batch: Sequence[tf.Tensor], model: Any,
    inference_args: Dict[str, Any]) -> Iterable[PredictionResult]:
  key_to_tensor_list = defaultdict(list)
  for example in batch:
    for key, tensor in example.items():
      key_to_tensor_list[key].append(tensor)
  key_to_batched_tensors = {}
  for key in key_to_tensor_list:
    batched_tensors = torch.stack(key_to_tensor_list[key])
    batched_tensors = key_to_tensor_list[key]
    key_to_batched_tensors[key] = batched_tensors
  return utils._convert_to_result(
      batch, model(**key_to_batched_tensors, **inference_args))


class HuggingFaceModelHandlerKeyedTensor(ModelHandler[Dict[str,
                                                           Union[tf.Tensor,
                                                                 torch.Tensor]],
                                                      PredictionResult,
                                                      Any]):
  def __init__(
      self,
      model_uri: str,
      model_class: AutoModel,
      model_config_args: Dict[str, Any] = None,
      inference_args: Optional[Dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for HuggingFace with
      Keyed Tensors for PyTorch/Tensorflow backend.

      Depending on the type of tensors,
      the model framework is determined automatically.

      Example Usage model::
      pcoll | RunInference(HuggingFaceModelHandlerKeyedTensor(
        model_uri="bert-base-uncased"))

    Args:
      model_uri (str): path to the pretrained model on the hugging face
        models hub.
      model_config_args (Dict[str, Any]): keyword arguments to provide load
        options while loading from Hugging Face Hub. Defaults to None.
      inference_args (Optional[Dict[str, Any]]): Non-batchable arguments
        required as inputs to the model's forward() function. Unlike Tensors in
        `batch`, these parameters will not be dynamically batched.
        Defaults to None.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
    """
    self._model_uri = model_uri
    self._model_class = model_class
    self._model_path = model_uri
    self._model_config_args = model_config_args if model_config_args else {}
    self._inference_args = inference_args if inference_args else {}
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size

  def load_model(self):
    """Loads and initializes the model for processing."""
    return self._model_class.from_pretrained(
        self._model_uri, **self._model_config_args)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_path = model_path if model_path else self._model_path

  def run_inference(
      self,
      batch: Sequence[Dict[str, Union[tf.Tensor, torch.Tensor]]],
      model: Any,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Keyed Tensors and returns an Iterable of
    Tensors Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of Keyed Tensors. These Tensors should be batchable,
        as this method will call `tf.stack()`/`torch.stack()` and pass in
        batched Tensors with dimensions (batch_size, n_features, etc.) into the
        model's predict() function.
      model: A Tensorflow/PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched
    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args
    # torch.no_grad() mitigates GPU memory issues
    # https://github.com/apache/beam/issues/22811
    if isinstance(batch[0], tf.Tensor):
      return _run_inference_tensorflow_keyed_tensor(
          batch, model, inference_args)
    else:
      return _run_inference_torch_keyed_tensor(batch, model, inference_args)

  def get_num_bytes(
      self, batch: Sequence[Union[tf.Tensor, torch.Tensor]]) -> int:
    """
    Returns:
      The number of bytes of data for the Tensors batch.
    """
    if self._framework == "tf":
      return sum(sys.getsizeof(element) for element in batch)
    else:
      return sum(
          (el.element_size() for tensor in batch for el in tensor.values()))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_HuggingFaceModelHandler_KeyedTensor'

  def batch_elements_kwargs(self):
    return self._batching_kwargs


class HuggingFaceModelHandlerTensor(ModelHandler[Union[tf.Tensor, torch.Tensor],
                                                 PredictionResult,
                                                 Any]):
  def __init__(
      self,
      model_uri: str,
      model_class: AutoModel,
      model_config_args: Dict[str, Any] = None,
      inference_args: Optional[Dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for HuggingFace with
      Tensors for PyTorch/Tensorflow backend.

      Depending on the type of tensors,
      the model framework is determined automatically.

      Example Usage model:
      pcoll | RunInference(HuggingFaceModelHandlerTensor(
        model_uri="bert-base-uncased"))

    Args:
      model_uri (str): path to the pretrained model on the
      Hugging Face models hub.
      model_config_args (Dict[str, Any]): keyword arguments to provide load
        options while loading from Hugging Face Hub. Defaults to None.
      inference_args (Optional[Dict[str, Any]]): Non-batchable arguments
        required as inputs to the model's forward() function. Unlike Tensors in
        `batch`, these parameters will not be dynamically batched.
        Defaults to None.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
    """
    self._model_uri = model_uri
    self._model_class = model_class
    self._model_path = model_uri
    self._model_config_args = model_config_args if model_config_args else {}
    self._inference_args = inference_args if inference_args else {}
    self._batching_kwargs = {}
    self._framework = "torch"
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size

  def load_model(self):
    """Loads and initializes the model for processing."""
    return self._model_class.from_pretrained(
        self._model_uri, **self._model_config_args)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_path = model_path if model_path else self._model_path

  def run_inference(
      self,
      batch: Sequence[Union[tf.Tensor, torch.Tensor]],
      model: Any,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Tensors and returns an Iterable of
    Tensors Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of Tensors. These Tensors should be batchable, as this
        method will call `tf.stack()`/`torch.stack()` and pass in batched
        Tensors with dimensions (batch_size, n_features, etc.) into the model's
        predict() function.
      model: A Tensorflow/PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched
    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args

    if isinstance(batch[0], tf.Tensor):
      self._framework = "tf"
      predictions = model(batch, **inference_args)
    else:
      # torch.no_grad() mitigates GPU memory issues
      # https://github.com/apache/beam/issues/22811
      with torch.no_grad():
        predictions = model(batch, **inference_args)

    return utils._convert_to_result(batch, predictions)

  def get_num_bytes(
      self, batch: Sequence[Union[tf.Tensor, torch.Tensor]]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    if self._framework == "tf":
      return sum(sys.getsizeof(element) for element in batch)
    else:
      return sum(
          (el.element_size() for tensor in batch for el in tensor.values()))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_HuggingFaceModelHandler_Tensor'

  def batch_elements_kwargs(self):
    return self._batching_kwargs


class HuggingFaceModelHandlerNumpy(ModelHandler[numpy.ndarray,
                                                PredictionResult,
                                                Any]):
  def __init__(
      self,
      model_uri: str,
      model_class: AutoModel,
      model_config_args: Dict[str, Any] = None,
      inference_args: Optional[Dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for HuggingFace with
      numpy.ndarray for PyTorch/Tensorflow backend.

      Example Usage model::
      pcoll | RunInference(HuggingFaceModelHandlerNumpy(
        model_uri="bert-base-uncased"))

    Args:
      model_uri (str): path to the pretrained model
        on the Hugging Face Models hub.
      model_config_args (Dict[str, Any]): keyword arguments to provide load
        options while loading from Hugging Face Hub. Defaults to None.
      inference_args (Optional[Dict[str, Any]]): Non-batchable arguments
        required as inputs to the model's forward() function. Unlike numpy
        ndarray in `batch`, these parameters will not be dynamically batched.
        Defaults to None.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
    """
    self._model_uri = model_uri
    self._model_class = model_class
    self._model_path = model_uri
    self._model_config_args = model_config_args if model_config_args else {}
    self._inference_args = inference_args if inference_args else {}
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size

  def load_model(self):
    """Loads and initializes the model for processing."""
    return self._model_class.from_pretrained(
        self._model_uri, **self._model_config_args)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_path = model_path if model_path else self._model_path

  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      model: Any,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of numpy ndarray and returns an Iterable of
    numpy Predictions.

    This method stacks the list of numpy ndarray in a vectorized format to
    optimize the inference call.

    Args:
      batch: A sequence of numpy ndarray. These arrays should be batchable, as
        this method will call `numpy.stack()` and pass in batched arrays with
        dimensions (batch_size, n_features, etc.) into the model's
        predict() function.
      model: A pretrained model compatible with numpy input.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched
    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args
    predictions = model(batch, **inference_args)
    return utils._convert_to_result(batch, predictions)

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
    return 'BeamML_HuggingFaceModelHandler_Numpy'

  def batch_elements_kwargs(self):
    return self._batching_kwargs
