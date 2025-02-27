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

from apache_beam.runners.worker.statesampler_interface import StateSamplerInterface, ScopedStateInterface


class StubStateSampler(StateSamplerInterface):
  def __init__(self):
    self._update_metric_calls = {}

  def update_metric(self, typed_metric_name, value):
    if (typed_metric_name not in self._update_metric_calls):
      self._update_metric_calls[typed_metric_name] = value
      return
    self._update_metric_calls[typed_metric_name] += value

  def get_recorded_calls(self):
    return self._update_metric_calls

  def start(self) -> None:
    raise NotImplementedError()

  def stop(self) -> None:
    raise NotImplementedError()

  def reset(self) -> None:
    raise NotImplementedError()

  def current_state(self) -> ScopedStateInterface:
    raise NotImplementedError()
