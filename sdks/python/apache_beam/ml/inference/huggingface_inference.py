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
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence

import torch
from transformers import AutoModel

from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference import utils


class HuggingFaceModelHandler(ModelHandler[Dict[str, torch.Tensor],
                                           PredictionResult,
                                           Any]):
  def __init__(
      self,
      model_name: str,
      model_download_args: Dict[str, Any] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      **kwargs):
    self._model_handler = None
    self._model_name = model_name
    self._model_path = None
    self._model = None
    self._model_download_args = model_download_args if model_download_args else {}  # pylint: disable=line-too-long
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size

  def load_model(self):
    """Loads and initializes a model for processing."""
    self._model = AutoModel.from_pretrained(self._model_name)
    return self._model

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_path = model_path if model_path else self._model_path

  def run_inference(
      self,
      batch: Sequence[Dict[str, torch.Tensor]],
      model: Any,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    key_to_tensor_list = defaultdict(list)

    inference_args = {} if not inference_args else inference_args
    # torch.no_grad() mitigates GPU memory issues
    # https://github.com/apache/beam/issues/22811

    with torch.no_grad():
      for example in batch:
        for key, tensor in example.items():
          key_to_tensor_list[key].append(tensor)
      key_to_batched_tensors = {}
      for key in key_to_tensor_list:
        batched_tensors = torch.stack(key_to_tensor_list[key])
        # batched_tensors = key_to_tensor_list[key]
        key_to_batched_tensors[key] = batched_tensors
      predictions = model(**key_to_batched_tensors, **inference_args)

      return utils._convert_to_result(batch, predictions)

  def get_num_bytes(self, batch: Sequence[torch.Tensor]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return sum(
        (el.element_size() for tensor in batch for el in tensor.values()))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_HuggingFace_Tensor'

  def batch_elements_kwargs(self):
    return self._batching_kwargs
