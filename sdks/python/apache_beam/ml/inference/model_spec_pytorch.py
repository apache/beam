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

from apache_beam.ml.inference.model_spec import ModelSpec


class PytorchModelSpec(ModelSpec):
  '''
  This class wraps the PyTorch model, and other
  PyTorch-specific parameters
  '''
  VALID_DEVICE_TYPES = ['CPU', 'GPU']

  def __init__(self, model_url: str, device: str):
    super().__init__(model_url)
    self._device = device
    self._validate_device()

  def _validate_model(self):
    pass

  def _validate_device(self):
    if self._device.upper() not in self.VALID_DEVICE_TYPES:
      raise ValueError('Device type must be one of ' + self.VALID_DEVICE_TYPES)

  def load_model(self):
    pass
