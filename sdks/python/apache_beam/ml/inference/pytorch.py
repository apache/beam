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

from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List

import torch
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.api import PredictionResult
from apache_beam.ml.inference.base import InferenceRunner
from apache_beam.ml.inference.base import ModelLoader


class PytorchInferenceRunner(InferenceRunner):
  """
  This class runs Pytorch inferences with the run_inference method. It also has
  other methods to get the bytes of a batch of Tensors as well as the namespace
  for Pytorch models.
  """
  def __init__(self, device: torch.device):
    self._device = device

  def run_inference(self, batch: List[torch.Tensor],
                    model: torch.nn.Module) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Tensors and returns an Iterable of
    Tensor Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.
    """

    batch = torch.stack(batch)
    if batch.device != self._device:
      batch = batch.to(self._device)
    predictions = model(batch)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def get_num_bytes(self, batch: List[torch.Tensor]) -> int:
    """Returns the number of bytes of data for a batch of Tensors."""
    return sum((el.element_size() for tensor in batch for el in tensor))

  def get_metrics_namespace(self) -> str:
    """
    Returns a namespace for metrics collected by the RunInference transform.
    """
    return 'RunInferencePytorch'


class PytorchModelLoader(ModelLoader):
  """Loads a Pytorch Model."""
  def __init__(
      self,
      state_dict_path: str,
      model_class: Callable[..., torch.nn.Module],
      model_params: Dict[str, Any],
      device: str = 'CPU'):
    """
    state_dict_path: path to the saved dictionary of the model state.
    model_class: class of the Pytorch model that defines the model structure.
    device: the device on which you wish to run the model. If ``device = GPU``
        then device will be cuda if it is available. Otherwise, it will be cpu.

    See https://pytorch.org/tutorials/beginner/saving_loading_models.html
    for details
    """
    self._state_dict_path = state_dict_path
    if device == 'GPU' and torch.cuda.is_available():
      self._device = torch.device('cuda')
    else:
      self._device = torch.device('cpu')
    self._model_class = model_class
    self.model_params = model_params
    self._inference_runner = PytorchInferenceRunner(device=self._device)

  def load_model(self) -> torch.nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    model = self._model_class(**self.model_params)
    model.to(self._device)
    file = FileSystems.open(self._state_dict_path, 'rb')
    model.load_state_dict(torch.load(file))
    model.eval()
    return model

  def get_inference_runner(self) -> InferenceRunner:
    """Returns a Pytorch implementation of InferenceRunner."""
    return self._inference_runner
