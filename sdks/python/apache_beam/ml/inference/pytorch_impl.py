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

from typing import Iterable

import torch
from torch import nn
from apache_beam.ml.inference.run_inference_base import InferenceRunner
from apache_beam.ml.inference.run_inference_base import ModelLoader


class PytorchInferenceRunner(InferenceRunner):
  """
  Implements Pytorch inference method
  """
  def run_inference(self, batch: torch.Tensor,
                    model: nn.Module) -> Iterable[torch.Tensor]:
    """
    Runs inferences on a batch of examples and returns an Iterable of
    Predictions."""
    return model(batch)

  def get_num_bytes(self, batch: torch.Tensor) -> int:
    """Returns the number of bytes of data for a batch."""
    return batch.storage()

  def get_metrics_namespace(self) -> str:
    """
    Returns a namespace for metrics collected by the RunInference transform.
    """
    return 'RunInferencePytorch'


class PytorchModelLoader(ModelLoader):
  """Loads a Pytorch Model."""
  def __init__(self, state_dict_path: str, model_class: nn.Module):
    """
    state_dict_path: path to the saved dictionary of the model state.
    model_class: class of the Pytorch model that defines the model structure.

    See https://pytorch.org/tutorials/beginner/saving_loading_models.html
    for details
    """
    self._state_dict_path = state_dict_path
    self._model_class = model_class

  def load_model(self) -> nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    model = self._model_class
    model.load_state_dict(torch.load(self._state_dict_path))
    model.eval()
    return model

  def get_inference_runner(self) -> InferenceRunner:
    """Returns a Pytorch implementation of InferenceRunner."""
    return PytorchInferenceRunner()
