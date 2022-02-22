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

VALID_PYTORCH_DEVICE_TYPES = ['CPU', 'GPU']
VALID_SKLEARN_SERIALIZATION_TYPES = ['JOBLIB', 'PICKLE']


class RunInferenceModel(object):
  '''
  Base class for the RunInference Model.
  This class contains information about
  a model that is needed
  '''
  def __init__(self, model_url):
    self._model_url = model_url
    self._validate_model()

  @abc.abstractmethod
  def _validate_model(self):
    raise NotImplementedError("Please implement _validate_model")


class PytorchModel(RunInferenceModel):
  '''
  This class wraps the PyTorch model, and other
  PyTorch-specific parameters
  '''
  def __init__(self, model_url: str, device: str):
    super().__init__(model_url)
    self._device = device
    self._validate_device()

  def _validate_model(self):
    pass

  def _validate_device(self):
    if self._device not in VALID_PYTORCH_DEVICE_TYPES:
      raise ValueError(
          'Device type must be one of ' + VALID_PYTORCH_DEVICE_TYPES)


class SklearnModel(RunInferenceModel):
  '''
  This class wraps the scikit-learn model, and other
  scikit-learn-specific parameters
  '''
  def __init__(self, model_url, serialization_method):
    super().__init__(model_url)
    self._serialization_method = serialization_method

  def _validate_model(self):
    pass

  def _validate_serialization(self):
    if self._serialization_method not in VALID_SKLEARN_SERIALIZATION_TYPES:
      raise ValueError(
          'Serialization type must be one of ' +
          VALID_SKLEARN_SERIALIZATION_TYPES)
