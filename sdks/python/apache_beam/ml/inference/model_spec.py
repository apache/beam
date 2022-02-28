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
from typing import Generic, TypeVar

# TODO: consolidate this definition of TypeVar if defined elsewhere
ExampleBatchType = TypeVar('ExampleBatchType')
InferenceBatchType = TypeVar('InferenceBatchType')


class ModelSpec(Generic[ExampleBatchType, InferenceBatchType]):
  '''
  Base class that defines the parameters for a model.
  '''
  def __init__(self, model_url):
    self._model_url = model_url
    self._validate_model()

  @abc.abstractmethod
  def load_model(self) -> RunnableModel[ExampleBatchType, InferenceBatchType]:
    """Loads an initializes a model for processing."""
    raise NotImplementedError(type(self))

  @abc.abstractmethod
  def _validate_model(self):
    raise NotImplementedError("Please implement _validate_model")
