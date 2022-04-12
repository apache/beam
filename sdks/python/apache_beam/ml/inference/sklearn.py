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
#TODO this PR is waiting for base run_inference PR to be merged
import apache_beam.ml.inference.base_todo_remove as base

from apache_beam.io.filesystems import FileSystems
import enum
import joblib
import pickle
import numpy as np
import sklearn
from typing import Any
from typing import Iterable
from typing import List

SERIALIZATION_TYPE = Enum('PICKLE','JOBLIB')

class SKLearnInferenceRunner(base.InferenceRunner):
  def run_inference(self, batch: List[np.Array], model: Any) -> Iterable[np.Array]:
    for example in batch:
      yield self.model.predict(example)

  def get_num_bytes(self, batch: List[np.Array]) -> int:
    """Returns the number of bytes of data for a batch."""
    return sum(element.np_size for element in batch)

class SKLearnModelLoader(base.ModelLoader):
  def __init__(self, serialization:SERIALIZATION_TYPE=SERIALIZATION_TYPE.PICKLE,
               model_uri:str=''):
    self._serialization = serialization
    self._model_uri = model_uri
    self._inference_runner = SKLearnInferenceRunner()

  def load_model(self):
    """Loads and initializes a model for processing."""
    file = FileSystems.open(self._state_dict_path, 'rb')
    if self._serialization == SERIALIZATION_TYPE.PICKLE:
      return pickle.load(file)
    elif self._serialization == SERIALIZATION_TYPE.JOBLIB:
      return joblib.load(file)
    raise ValueError('No supported serialization type.')

  def get_inference_runner(self) -> SKLearnInferenceRunner:
    return self._inference_runner
