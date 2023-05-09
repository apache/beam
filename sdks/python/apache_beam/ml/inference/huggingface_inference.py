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

from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

from huggingface_hub import hf_hub_download
import torch
import tensorflow as tf

from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.tensorflow_inference import ModelType, TFModelHandlerTensor

__all__ = [
    'HuggingFaceModelHandlerTensor',
]


class HuggingFaceModelHandlerTensor(ModelHandler[Union[torch.Tensor, tf.Tensor],
                                                 PredictionResult,
                                                 Any]):
  def __init__(
      self,
      repo_id: str,
      filename: str,
      model_download_args: Dict[str, Any] = None,
      kwargs: Dict[str, Any] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None):
    self._model_handler = None
    self._model_path = None
    self._repo_id = repo_id
    self._filename = filename
    self._model_download_args = model_download_args if model_download_args else {}  # pylint: disable=line-too-long
    self._kwargs = kwargs if kwargs else {}
    self._batching_kwargs = {}
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size

  def load_model(self):
    """Loads and initializes a model for processing."""
    self._model_path = hf_hub_download(
        repo_id=self._repo_id,
        filename=self._filename,
        **self._model_download_args)
    if self._filename.startswith('pytorch_'):
      self._model_handler = PytorchModelHandlerTensor(
          self._model_path, **self._kwargs)
    elif self._filename.startswith('tf_'):
      self._model_handler = TFModelHandlerTensor(
          self._model_path, ModelType.SAVED_WEIGHTS)
    return self._model_handler.load_model()

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_path = model_path if model_path else self._model_path

  def run_inference(
      self,
      batch: Sequence[Union[torch.Tensor, tf.Tensor]],
      model: Any,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    return self._model_handler.run_inference(batch, model, inference_args)

  def get_num_bytes(
      self, batch: Sequence[Union[torch.Tensor, tf.Tensor]]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return self._model_handler.get_num_bytes(batch)

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_HuggingFace_Tensor'

  def batch_elements_kwargs(self):
    return self._batching_kwargs
