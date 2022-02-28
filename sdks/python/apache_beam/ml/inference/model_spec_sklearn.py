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


class SklearnModelSpec(ModelSpec):
  '''
  This class wraps the scikit-learn model, and other
  scikit-learn-specific parameters
  '''
  VALID_SERIALIZATION_TYPES = ['PICKLE', 'JOBLIB']

  def __init__(self, model_url, serialization_method='PICKLE'):
    super().__init__(model_url)
    self._serialization_method = serialization_method

    # Do scikit-learn validations
    self._validate_serialization()

  def _validate_model(self):
    pass

  def _validate_serialization(self):
    if self._serialization_method not in self.VALID_SERIALIZATION_TYPES:
      raise ValueError(
          'Serialization type must be one of ' + self.VALID_SERIALIZATION_TYPES)

  def load_model(self):
    pass
