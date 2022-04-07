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

import abc


from dataclasses import dataclass
import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.ml.experimental.run_inference_internal import BaseSavedModelBatchDoFn

import sklearn
import joblib
import numpy as np
from typing import Any

@dataclass
class SciKitLearnParams:
  model_url: str


class SciKitLearnDoFn(BaseSavedModelBatchDoFn):
  def __init__(self, params: SciKitLearnParams, shared_model_handle: shared.Shared):
    self._shared_model_handle = shared_model_handle
    self._params = params
    self._session = None

  def setup(self):
    super().setup()
    self._session = self._load_model()

  def _load_model(self):
    def load():
      """Function for constructing shared LoadedModel."""
#      start_time = self._clock.get_current_time_in_microseconds()
#      memory_before = _get_current_process_memory_in_bytes()
      # TODO the model can also be pickled
      model = joblib.load(self._params.model_url)

#      end_time = self._clock.get_current_time_in_microseconds()
#      memory_after = _get_current_process_memory_in_bytes()
#      self._metrics_collector.load_model_latency_milli_secs_cache = (
#          (end_time - start_time) / _MILLISECOND_TO_MICROSECOND)
#      self._metrics_collector.model_byte_size_cache = (
#          memory_after - memory_before)
      return model
    return self._shared_model_handle.acquire(load)

  def _check_examples(self, examples: Any):
    pass

  def _run_inference(self, examples, serialized_examples: Any):
    return self.model.predict()

  def _post_process(self, examples: Any, serialized_examples: Any, outputs):
    return outputs
