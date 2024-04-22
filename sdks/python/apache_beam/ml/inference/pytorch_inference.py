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

import torch
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

__all__ = [
    'PytorchModelHandlerTensor',
    'PytorchModelHandlerKeyedTensor',
]

TensorInferenceFn = Callable[[
    Sequence[torch.Tensor],
    torch.nn.Module,
    torch.device,
    Optional[Dict[str, Any]],
    Optional[str]
],
                             Iterable[PredictionResult]]

KeyedTensorInferenceFn = Callable[[
    Sequence[Dict[str, torch.Tensor]],
    torch.nn.Module,
    torch.device,
    Optional[Dict[str, Any]],
    Optional[str]
],
                                  Iterable[PredictionResult]]


def _validate_constructor_args(
    state_dict_path, model_class, torch_script_model_path):
  message = (
      "A {param1} has been supplied to the model "
      "handler, but the required {param2} is missing. "
      "Please provide the {param2} in order to "
      "successfully load the {param1}.")
  # state_dict_path and model_class are coupled with each other
  # raise RuntimeError if user forgets to pass any one of them.
  if state_dict_path and not model_class:
    raise RuntimeError(
        message.format(param1="state_dict_path", param2="model_class"))

  if not state_dict_path and model_class:
    raise RuntimeError(
        message.format(param1="model_class", param2="state_dict_path"))

  if torch_script_model_path and state_dict_path:
    raise RuntimeError(
        "Please specify either torch_script_model_path or "
        "(state_dict_path, model_class) to successfully load the model.")


def _load_model(
    model_class: Optional[Callable[..., torch.nn.Module]],
    state_dict_path: Optional[str],
    device: torch.device,
    model_params: Optional[Dict[str, Any]],
    torch_script_model_path: Optional[str],
    load_model_args: Optional[Dict[str, Any]]):
  if device == torch.device('cuda') and not torch.cuda.is_available():
    logging.warning(
        "Model handler specified a 'GPU' device, but GPUs are not available. "
        "Switching to CPU.")
    device = torch.device('cpu')

  try:
    logging.info(
        "Loading state_dict_path %s onto a %s device", state_dict_path, device)
    if not torch_script_model_path:
      file = FileSystems.open(state_dict_path, 'rb')
      model = model_class(**model_params)  # type: ignore[arg-type,misc]
      state_dict = torch.load(file, map_location=device, **load_model_args)
      model.load_state_dict(state_dict)
      model.requires_grad_(False)
    else:
      file = FileSystems.open(torch_script_model_path, 'rb')
      model = torch.jit.load(file, map_location=device, **load_model_args)
  except RuntimeError as e:
    if device == torch.device('cuda'):
      message = "Loading the model onto a GPU device failed due to an " \
        f"exception:\n{e}\nAttempting to load onto a CPU device instead."
      logging.warning(message)
      return _load_model(
          model_class,
          state_dict_path,
          torch.device('cpu'),
          model_params,
          torch_script_model_path,
          load_model_args)
    else:
      raise e

  model.to(device)
  model.eval()
  logging.info("Finished loading PyTorch model.")
  return model, device


def _convert_to_device(examples: torch.Tensor, device) -> torch.Tensor:
  """
  Converts samples to a style matching given device.

  **NOTE:** A user may pass in device='GPU' but if GPU is not detected in the
  environment it must be converted back to CPU.
  """
  if examples.device != device:
    examples = examples.to(device)
  return examples


def default_tensor_inference_fn(
    batch: Sequence[torch.Tensor],
    model: torch.nn.Module,
    device: str,
    inference_args: Optional[Dict[str, Any]] = None,
    model_id: Optional[str] = None,
) -> Iterable[PredictionResult]:
  # torch.no_grad() mitigates GPU memory issues
  # https://github.com/apache/beam/issues/22811
  with torch.no_grad():
    batched_tensors = torch.stack(batch)
    batched_tensors = _convert_to_device(batched_tensors, device)
    predictions = model(batched_tensors, **inference_args)
    return utils._convert_to_result(batch, predictions, model_id)


def make_tensor_model_fn(model_fn: str) -> TensorInferenceFn:
  """
  Produces a TensorInferenceFn that uses a method of the model other that
  the forward() method.

  Args:
    model_fn: A string name of the method to be used. This is accessed through
      getattr(model, model_fn)
  """
  def attr_fn(
      batch: Sequence[torch.Tensor],
      model: torch.nn.Module,
      device: str,
      inference_args: Optional[Dict[str, Any]] = None,
      model_id: Optional[str] = None,
  ) -> Iterable[PredictionResult]:
    with torch.no_grad():
      batched_tensors = torch.stack(batch)
      batched_tensors = _convert_to_device(batched_tensors, device)
      pred_fn = getattr(model, model_fn)
      predictions = pred_fn(batched_tensors, **inference_args)
      return utils._convert_to_result(batch, predictions, model_id)

  return attr_fn


class PytorchModelHandlerTensor(ModelHandler[torch.Tensor,
                                             PredictionResult,
                                             torch.nn.Module]):
  def __init__(
      self,
      state_dict_path: Optional[str] = None,
      model_class: Optional[Callable[..., torch.nn.Module]] = None,
      model_params: Optional[Dict[str, Any]] = None,
      device: str = 'CPU',
      *,
      inference_fn: TensorInferenceFn = default_tensor_inference_fn,
      torch_script_model_path: Optional[str] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      load_model_args: Optional[Dict[str, Any]] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for PyTorch.

    Example Usage for torch model::
      pcoll | RunInference(PytorchModelHandlerTensor(state_dict_path="my_uri",
                                                     model_class="my_class"))
    Example Usage for torchscript model::
      pcoll | RunInference(PytorchModelHandlerTensor(
        torch_script_model_path="my_uri"))

    See https://pytorch.org/tutorials/beginner/saving_loading_models.html
    for details

    Args:
      state_dict_path: path to the saved dictionary of the model state.
      model_class: class of the Pytorch model that defines the model
        structure.
      model_params: A dictionary of arguments required to instantiate the model
        class.
      device: the device on which you wish to run the model. If
        ``device = GPU`` then a GPU device will be used if it is available.
        Otherwise, it will be CPU.
      inference_fn: the inference function to use during RunInference.
        default=_default_tensor_inference_fn
      torch_script_model_path: Path to the torch script model.
         the model will be loaded using `torch.jit.load()`.
        `state_dict_path`, `model_class` and `model_params`
         arguments will be disregarded.
      min_batch_size: the minimum batch size to use when batching inputs. This
        batch will be fed into the inference_fn as a Sequence of Tensors.
      max_batch_size: the maximum batch size to use when batching inputs. This
        batch will be fed into the inference_fn as a Sequence of Tensors.
      max_batch_duration_secs: the maximum amount of time to buffer a batch
          before emitting; used in streaming contexts.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      load_model_args: a dictionary of parameters passed to the torch.load
        function to specify custom config for loading models.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.

    **Supported Versions:** RunInference APIs in Apache Beam have been tested
    with PyTorch 1.9 and 1.10.
    """
    self._state_dict_path = state_dict_path
    if device == 'GPU':
      logging.info("Device is set to CUDA")
      self._device = torch.device('cuda')
    else:
      logging.info("Device is set to CPU")
      self._device = torch.device('cpu')
    self._model_class = model_class
    self._model_params = model_params if model_params else {}
    self._inference_fn = inference_fn
    self._batching_kwargs = {}
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs
    self._torch_script_model_path = torch_script_model_path
    self._load_model_args = load_model_args if load_model_args else {}
    self._env_vars = kwargs.get('env_vars', {})
    self._share_across_processes = large_model or (model_copies is not None)
    self._model_copies = model_copies or 1

    _validate_constructor_args(
        state_dict_path=self._state_dict_path,
        model_class=self._model_class,
        torch_script_model_path=self._torch_script_model_path)

  def load_model(self) -> torch.nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    model, device = _load_model(
        model_class=self._model_class,
        state_dict_path=self._state_dict_path,
        device=self._device,
        model_params=self._model_params,
        torch_script_model_path=self._torch_script_model_path,
        load_model_args=self._load_model_args
    )
    self._device = device
    return model

  def update_model_path(self, model_path: Optional[str] = None):
    if self._torch_script_model_path:
      self._torch_script_model_path = (
          model_path if model_path else self._torch_script_model_path)
    else:
      self._state_dict_path = (
          model_path if model_path else self._state_dict_path)

  def run_inference(
      self,
      batch: Sequence[torch.Tensor],
      model: torch.nn.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Tensors and returns an Iterable of
    Tensor Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of Tensors. These Tensors should be batchable, as this
        method will call `torch.stack()` and pass in batched Tensors with
        dimensions (batch_size, n_features, etc.) into the model's forward()
        function.
      model: A PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched

    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args
    model_id = (
        self._state_dict_path
        if not self._torch_script_model_path else self._torch_script_model_path)
    return self._inference_fn(
        batch, model, self._device, inference_args, model_id)

  def get_num_bytes(self, batch: Sequence[torch.Tensor]) -> int:
    """
    Returns:
      The number of bytes of data for a batch of Tensors.
    """
    return sum((el.element_size() for tensor in batch for el in tensor))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_PyTorch'

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    pass

  def batch_elements_kwargs(self):
    return self._batching_kwargs

  def share_model_across_processes(self) -> bool:
    return self._share_across_processes

  def model_copies(self) -> int:
    return self._model_copies


def default_keyed_tensor_inference_fn(
    batch: Sequence[Dict[str, torch.Tensor]],
    model: torch.nn.Module,
    device: str,
    inference_args: Optional[Dict[str, Any]] = None,
    model_id: Optional[str] = None,
) -> Iterable[PredictionResult]:
  # If elements in `batch` are provided as a dictionaries from key to Tensors,
  # then iterate through the batch list, and group Tensors to the same key
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
    predictions = model(**key_to_batched_tensors, **inference_args)

    return utils._convert_to_result(batch, predictions, model_id)


def make_keyed_tensor_model_fn(model_fn: str) -> KeyedTensorInferenceFn:
  """
  Produces a KeyedTensorInferenceFn that uses a method of the model other that
  the forward() method.

  Args:
    model_fn: A string name of the method to be used. This is accessed through
      getattr(model, model_fn)
  """
  def attr_fn(
      batch: Sequence[Dict[str, torch.Tensor]],
      model: torch.nn.Module,
      device: str,
      inference_args: Optional[Dict[str, Any]] = None,
      model_id: Optional[str] = None,
  ) -> Iterable[PredictionResult]:
    # If elements in `batch` are provided as a dictionaries from key to Tensors,
    # then iterate through the batch list, and group Tensors to the same key
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
      pred_fn = getattr(model, model_fn)
      predictions = pred_fn(**key_to_batched_tensors, **inference_args)
    return utils._convert_to_result(batch, predictions, model_id)

  return attr_fn


class PytorchModelHandlerKeyedTensor(ModelHandler[Dict[str, torch.Tensor],
                                                  PredictionResult,
                                                  torch.nn.Module]):
  def __init__(
      self,
      state_dict_path: Optional[str] = None,
      model_class: Optional[Callable[..., torch.nn.Module]] = None,
      model_params: Optional[Dict[str, Any]] = None,
      device: str = 'CPU',
      *,
      inference_fn: KeyedTensorInferenceFn = default_keyed_tensor_inference_fn,
      torch_script_model_path: Optional[str] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      load_model_args: Optional[Dict[str, Any]] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for PyTorch.

     Example Usage for torch model::
      pcoll | RunInference(PytorchModelHandlerKeyedTensor(
        state_dict_path="my_uri",
        model_class="my_class"))

    Example Usage for torchscript model::
      pcoll | RunInference(PytorchModelHandlerKeyedTensor(
        torch_script_model_path="my_uri"))

    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.

    See https://pytorch.org/tutorials/beginner/saving_loading_models.html
    for details

    Args:
      state_dict_path: path to the saved dictionary of the model state.
      model_class: class of the Pytorch model that defines the model
        structure.
      model_params: A dictionary of arguments required to instantiate the model
        class.
      device: the device on which you wish to run the model. If
        ``device = GPU`` then a GPU device will be used if it is available.
        Otherwise, it will be CPU.
      inference_fn: the function to invoke on run_inference.
        default = default_keyed_tensor_inference_fn
      torch_script_model_path: Path to the torch script model.
         the model will be loaded using `torch.jit.load()`.
        `state_dict_path`, `model_class` and `model_params`
         arguments will be disregarded.
      min_batch_size: the minimum batch size to use when batching inputs. This
        batch will be fed into the inference_fn as a Sequence of Keyed Tensors.
      max_batch_size: the maximum batch size to use when batching inputs. This
        batch will be fed into the inference_fn as a Sequence of Keyed Tensors.
      max_batch_duration_secs: the maximum amount of time to buffer a batch
          before emitting; used in streaming contexts.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      load_model_args: a dictionary of parameters passed to the torch.load
        function to specify custom config for loading models.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.

    **Supported Versions:** RunInference APIs in Apache Beam have been tested
    on torch>=1.9.0,<1.14.0.
    """
    self._state_dict_path = state_dict_path
    if device == 'GPU':
      logging.info("Device is set to CUDA")
      self._device = torch.device('cuda')
    else:
      logging.info("Device is set to CPU")
      self._device = torch.device('cpu')
    self._model_class = model_class
    self._model_params = model_params if model_params else {}
    self._inference_fn = inference_fn
    self._batching_kwargs = {}
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs
    self._torch_script_model_path = torch_script_model_path
    self._load_model_args = load_model_args if load_model_args else {}
    self._env_vars = kwargs.get('env_vars', {})
    self._share_across_processes = large_model or (model_copies is not None)
    self._model_copies = model_copies or 1

    _validate_constructor_args(
        state_dict_path=self._state_dict_path,
        model_class=self._model_class,
        torch_script_model_path=self._torch_script_model_path)

  def load_model(self) -> torch.nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    model, device = _load_model(
        model_class=self._model_class,
        state_dict_path=self._state_dict_path,
        device=self._device,
        model_params=self._model_params,
        torch_script_model_path=self._torch_script_model_path,
        load_model_args=self._load_model_args
    )
    self._device = device
    return model

  def update_model_path(self, model_path: Optional[str] = None):
    if self._torch_script_model_path:
      self._torch_script_model_path = (
          model_path if model_path else self._torch_script_model_path)
    else:
      self._state_dict_path = (
          model_path if model_path else self._state_dict_path)

  def run_inference(
      self,
      batch: Sequence[Dict[str, torch.Tensor]],
      model: torch.nn.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Keyed Tensors and returns an Iterable of
    Tensor Predictions.

    For the same key across all examples, this will stack all Tensors values
    in a vectorized format to optimize the inference call.

    Args:
      batch: A sequence of keyed Tensors. These Tensors should be batchable,
        as this method will call `torch.stack()` and pass in batched Tensors
        with dimensions (batch_size, n_features, etc.) into the model's
        forward() function.
      model: A PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched

    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args
    model_id = (
        self._state_dict_path
        if not self._torch_script_model_path else self._torch_script_model_path)
    return self._inference_fn(
        batch, model, self._device, inference_args, model_id)

  def get_num_bytes(self, batch: Sequence[torch.Tensor]) -> int:
    """
    Returns:
       The number of bytes of data for a batch of Dict of Tensors.
    """
    # If elements in `batch` are provided as a dictionaries from key to Tensors
    return sum(
        (el.element_size() for tensor in batch for el in tensor.values()))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_PyTorch'

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    pass

  def batch_elements_kwargs(self):
    return self._batching_kwargs

  def share_model_across_processes(self) -> bool:
    return self._share_across_processes

  def model_copies(self) -> int:
    return self._model_copies
