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

from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Union

import torch
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.api import PredictionResult
from apache_beam.ml.inference.base import InferenceRunner
from apache_beam.ml.inference.base import ModelLoader


class PytorchInferenceRunner(InferenceRunner[torch.Tensor,
                                             PredictionResult,
                                             torch.nn.Module]):
  """
  This class runs Pytorch inferences with the run_inference method. It also has
  other methods to get the bytes of a batch of Tensors as well as the namespace
  for Pytorch models.
  """
  def __init__(self, device: torch.device):
    self._device = device

  def _convert_to_device(self, examples: torch.Tensor) -> torch.Tensor:
    """
    Converts samples to a style matching given device.

    Note: A user may pass in device='GPU' but if GPU is not detected in the
    environment it must be converted back to CPU.
    """
    if examples.device != self._device:
      examples = examples.to(self._device)
    return examples

  def run_inference(
      self,
      batch: List[Union[torch.Tensor, Dict[str, torch.Tensor]]],
      model: torch.nn.Module,
      **kwargs) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Tensors and returns an Iterable of
    Tensor Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.
    """
    prediction_params = kwargs.get('prediction_params', {})

    # If elements in `batch` are provided as a dictionaries from key to Tensors,
    # then iterate through the batch list, and group Tensors to the same key
    if isinstance(batch[0], dict):
      key_to_tensor_list = defaultdict(list)
      for example in batch:
        for key, tensor in example.items():
          key_to_tensor_list[key].append(tensor)
      key_to_batched_tensors = {}
      for key in key_to_tensor_list:
        batched_tensors = torch.stack(key_to_tensor_list[key])
        batched_tensors = self._convert_to_device(batched_tensors)
        key_to_batched_tensors[key] = batched_tensors
      predictions = model(**key_to_batched_tensors, **prediction_params)
    else:
      # If elements in `batch` are provided as Tensors, then do a regular stack
      batched_tensors = torch.stack(batch)
      batched_tensors = self._convert_to_device(batched_tensors)
      predictions = model(batched_tensors, **prediction_params)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def get_num_bytes(self, batch: List[torch.Tensor]) -> int:
    """Returns the number of bytes of data for a batch of Tensors."""
    # If elements in `batch` are provided as a dictionaries from key to Tensors
    if isinstance(batch[0], dict):
      return sum(
          (el.element_size() for tensor in batch for el in tensor.values()))
    else:
      # If elements in `batch` are provided as Tensors
      return sum((el.element_size() for tensor in batch for el in tensor))

  def get_metrics_namespace(self) -> str:
    """
    Returns a namespace for metrics collected by the RunInference transform.
    """
    return 'RunInferencePytorch'


class PytorchModelLoader(ModelLoader[torch.Tensor,
                                     PredictionResult,
                                     torch.nn.Module]):
  """ Implementation of the ModelLoader interface for PyTorch.

      NOTE: This API and its implementation are under development and
      do not provide backward compatibility guarantees.
  """
  def __init__(
      self,
      state_dict_path: str,
      model_class: Callable[..., torch.nn.Module],
      model_params: Dict[str, Any],
      device: str = 'CPU'):
    """
    Initializes a PytorchModelLoader
    :param state_dict_path: path to the saved dictionary of the model state.
    :param model_class: class of the Pytorch model that defines the model
    structure.
    :param device: the device on which you wish to run the model. If
    ``device = GPU`` then a GPU device will be used if it is available.
    Otherwise, it will be CPU.

    See https://pytorch.org/tutorials/beginner/saving_loading_models.html
    for details
    """
    self._state_dict_path = state_dict_path
    if device == 'GPU' and torch.cuda.is_available():
      self._device = torch.device('cuda')
    else:
      self._device = torch.device('cpu')
    self._model_class = model_class
    self._model_params = model_params

  def load_model(self) -> torch.nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    model = self._model_class(**self._model_params)
    model.to(self._device)
    file = FileSystems.open(self._state_dict_path, 'rb')
    model.load_state_dict(torch.load(file))
    model.eval()
    return model

  def get_inference_runner(self) -> PytorchInferenceRunner:
    """Returns a Pytorch implementation of InferenceRunner."""
    return PytorchInferenceRunner(device=self._device)
