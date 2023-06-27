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
import sys
from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

import tensorflow as tf
import torch
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.pytorch_inference import _convert_to_device
from transformers import AutoModel
from transformers import TFAutoModel

__all__ = [
    'HuggingFaceModelHandlerTensor',
    'HuggingFaceModelHandlerKeyedTensor',
]

TensorInferenceFn = Callable[[
    Sequence[Union[torch.Tensor, tf.Tensor]],
    Union[AutoModel, TFAutoModel],
    torch.device,
    Optional[Dict[str, Any]],
    Optional[str]
],
                             Iterable[PredictionResult]]

KeyedTensorInferenceFn = Callable[[
    Sequence[Dict[str, Union[torch.Tensor, tf.Tensor]]],
    Union[AutoModel, TFAutoModel],
    torch.device,
    Optional[Dict[str, Any]],
    Optional[str]
],
                                  Iterable[PredictionResult]]


def _validate_constructor_args(model_uri, model_class):
  message = (
      "Please provide both model class and model uri to load the model."
      "Got params as model_uri={model_uri} and "
      "model_class={model_class}.")
  if not model_uri and not model_class:
    raise RuntimeError(
        message.format(model_uri=model_uri, model_class=model_class))
  elif not model_uri:
    raise RuntimeError(
        message.format(model_uri=model_uri, model_class=model_class))
  elif not model_class:
    raise RuntimeError(
        message.format(model_uri=model_uri, model_class=model_class))


def _run_inference_torch_keyed_tensor(
    batch: Sequence[Dict[str, Union[tf.Tensor, torch.Tensor]]],
    model: AutoModel,
    device,
    inference_args: Dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  key_to_tensor_list = defaultdict(list)
  # torch.no_grad() mitigates GPU memory issues
  # https://github.com/apache/beam/issues/22811
  with torch.no_grad():
    for example in batch:
      for key, tensor in example.items():
        key_to_tensor_list[key].append(tensor)
    key_to_batched_tensors = {}
    for key in key_to_tensor_list:
      batched_tensors = torch.stack(key_to_tensor_list[key])
      batched_tensors = _convert_to_device(batched_tensors, device)
      key_to_batched_tensors[key] = batched_tensors
    return utils._convert_to_result(
        batch, model(**key_to_batched_tensors, **inference_args))


def _run_inference_tensorflow_keyed_tensor(
    batch: Sequence[Dict[str, Union[tf.Tensor, torch.Tensor]]],
    model: TFAutoModel,
    device,
    inference_args: Dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
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
                                                      Union[AutoModel,
                                                            TFAutoModel]]):
  def __init__(
      self,
      model_uri: str,
      model_class: Union[AutoModel, TFAutoModel],
      device: str = 'CPU',
      *,
      inference_fn: KeyedTensorInferenceFn = None,
      load_model_args: Optional[Dict[str, Any]] = None,
      inference_args: Optional[Dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      large_model: bool = False,
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
      model_class: model class to load the repository from model_uri.
      device: For torch tensors, specify device on which you wish to
        run the model. Defaults to CPU.
      inference_fn: the inference function to use during RunInference.
        Default is _run_inference_torch_keyed_tensor or
        _run_inference_tensorflow_keyed_tensor depending on the input type.
      load_model_args (Dict[str, Any]): keyword arguments to provide load
        options while loading from Hugging Face Hub. Defaults to None.
      inference_args (Optional[Dict[str, Any]]): Non-batchable arguments
        required as inputs to the model's forward() function. Unlike Tensors in
        `batch`, these parameters will not be dynamically batched.
        Defaults to None.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.

    **Supported Versions:** RunInference APIs in Apache Beam
    supports transformers>=4.18.0.
    """
    self._model_uri = model_uri
    self._model_class = model_class
    if device == 'GPU':
      self._device = torch.device('cuda')
    else:
      self._device = torch.device('cpu')
    self._inference_fn = inference_fn
    self._model_config_args = load_model_args if load_model_args else {}
    self._inference_args = inference_args if inference_args else {}
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size
    self._large_model = large_model
    self._framework = None

    _validate_constructor_args(
        model_uri=self._model_uri, model_class=self._model_class)

  def load_model(self):
    """Loads and initializes the model for processing."""
    model = self._model_class.from_pretrained(
        self._model_uri, **self._model_config_args)
    if self._device == torch.device('cuda'):
      if not torch.cuda.is_available():
        logging.warning(
            "Model handler specified a 'GPU' device, "
            "but GPUs are not available. Switching to CPU.")
        self._device = torch.device('cpu')
      model.to(self._device)
    return model

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_path = model_path if model_path else self._model_uri

  def run_inference(
      self,
      batch: Sequence[Dict[str, Union[tf.Tensor, torch.Tensor]]],
      model: Union[AutoModel, TFAutoModel],
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
    if not self._framework:
      self._framework = "tf" if isinstance(batch[0], tf.Tensor) else "torch"

    if self._inference_fn:
      return self._inference_fn(
          batch, model, self._device, inference_args, self._model_uri)

    if not self._framework:
      self._framework = "tf" if isinstance(batch[0], tf.Tensor) else "torch"

    if self._framework == "tf":
      return _run_inference_tensorflow_keyed_tensor(
          batch, model, self._device, inference_args, self._model_uri)
    else:
      return _run_inference_torch_keyed_tensor(
          batch, model, self._device, inference_args, self._model_uri)

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

  def share_model_across_processes(self) -> bool:
    return self._large_model


def _default_inference_fn_torch(
    batch: Sequence[Union[tf.Tensor, torch.Tensor]],
    model: Union[AutoModel, TFAutoModel],
    device,
    inference_args: Dict[str, Any] = None,
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  # torch.no_grad() mitigates GPU memory issues
  # https://github.com/apache/beam/issues/22811
  with torch.no_grad():
    batched_tensors = torch.stack(batch)
    batched_tensors = _convert_to_device(batched_tensors, device)
    predictions = model(batched_tensors, **inference_args)

  return utils._convert_to_result(batch, predictions, model_id)


def _default_inference_fn_tensorflow(
    batch: Sequence[Union[tf.Tensor, torch.Tensor]],
    model: Union[AutoModel, TFAutoModel],
    device,
    inference_args: Dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  batched_tensors = tf.stack(batch, axis=0)
  predictions = model(batched_tensors, **inference_args)
  return utils._convert_to_result(batch, predictions, model_id)


class HuggingFaceModelHandlerTensor(ModelHandler[Union[tf.Tensor, torch.Tensor],
                                                 PredictionResult,
                                                 Union[AutoModel,
                                                       TFAutoModel]]):
  def __init__(
      self,
      model_uri: str,
      model_class: Union[AutoModel, TFAutoModel],
      device: str = 'CPU',
      *,
      inference_fn: TensorInferenceFn = None,
      load_model_args: Optional[Dict[str, Any]] = None,
      inference_args: Optional[Dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      large_model: bool = False,
      **kwargs):
    """Implementation of the ModelHandler interface for HuggingFace with
      Tensors for PyTorch/Tensorflow backend.

      Depending on the type of tensors,
      the model framework is determined automatically.

      Example Usage model:
      pcoll | RunInference(HuggingFaceModelHandlerTensor(
        model_uri="bert-base-uncased"))

    Args:
      model_uri (str): path to the pretrained model on the hugging face
        models hub.
      model_class: model class to load the repository from model_uri.
      device: For torch tensors, specify device on which you wish to
        run the model. Defaults to CPU.
      inference_fn: the inference function to use during RunInference.
        Default is _default_inference_fn_tensor.
      load_model_args (Dict[str, Any]): keyword arguments to provide load
        options while loading from Hugging Face Hub. Defaults to None.
      inference_args (Optional[Dict[str, Any]]): Non-batchable arguments
        required as inputs to the model's forward() function. Unlike Tensors in
        `batch`, these parameters will not be dynamically batched.
        Defaults to None.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.

    **Supported Versions:** RunInference APIs in Apache Beam
    supports transformers>=4.18.0.
    """
    self._model_uri = model_uri
    self._model_class = model_class
    if device == 'GPU':
      self._device = torch.device('cuda')
    else:
      self._device = torch.device('cpu')
    self._inference_fn = inference_fn
    self._model_config_args = load_model_args if load_model_args else {}
    self._inference_args = inference_args if inference_args else {}
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size
    self._large_model = large_model
    self._framework = None

    _validate_constructor_args(
        model_uri=self._model_uri, model_class=self._model_class)

  def load_model(self):
    """Loads and initializes the model for processing."""
    model = self._model_class.from_pretrained(
        self._model_uri, **self._model_config_args)
    if self._device == torch.device('cuda'):
      if not torch.cuda.is_available():
        logging.warning(
            "Model handler specified a 'GPU' device, "
            "but GPUs are not available. Switching to CPU.")
        self._device = torch.device('cpu')
      model.to(self._device)
    return model

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_path = model_path if model_path else self._model_path

  def run_inference(
      self,
      batch: Sequence[Union[tf.Tensor, torch.Tensor]],
      model: Union[AutoModel, TFAutoModel],
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
    if not self._framework:
      self._framework = "tf" if isinstance(batch[0], tf.Tensor) else "torch"
    if self._inference_fn:
      return self._inference_fn(
          batch, model, inference_args, inference_args, self._model_uri)

    if self._framework == "tf":
      return _default_inference_fn_tensorflow(
          batch, model, self._device, inference_args, self._model_uri)
    else:
      return _default_inference_fn_torch(
          batch, model, self._device, inference_args, self._model_uri)

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

  def share_model_across_processes(self) -> bool:
    return self._large_model
