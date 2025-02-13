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
from collections import deque
from enum import Enum


class BaseTracker(abc.ABC):
  @abc.abstractmethod
  def push(self, x):
    raise NotImplementedError()

  @abc.abstractmethod
  def get(self):
    raise NotImplementedError()


class WindowMode(Enum):
  LANDMARK = 1
  SLIDING = 2


class WindowedTracker(BaseTracker):
  def __init__(self, window_mode, **kwargs):
    if window_mode == WindowMode.SLIDING:
      self._window_size = kwargs.get("window_size", 100)
      self._queue = deque(maxlen=self._window_size)
    self._n = 0
    self._window_mode = window_mode

  def push(self, x):
    self._queue.append(x)

  def pop(self):
    return self._queue.popleft()
