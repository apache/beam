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

from abc import ABC, abstractmethod


class ScopedStateInterface(ABC):
  @abstractmethod
  def sampled_seconds(self) -> float:
    pass

  @abstractmethod
  def sampled_msecs_int(self) -> int:
    pass

  @abstractmethod
  def __enter__(self):
    pass

  @abstractmethod
  def __exit__(self, exc_type, exc_value, traceback):
    pass


class StateSamplerInterface(ABC):
  @abstractmethod
  def start(self) -> None:
    pass

  @abstractmethod
  def stop(self) -> None:
    pass

  @abstractmethod
  def reset(self) -> None:
    pass

  @abstractmethod
  def current_state(self) -> ScopedStateInterface:
    pass

  @abstractmethod
  def update_metric(self, typed_metric_name, value) -> None:
    pass
