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

EPSILON = 1e-9


class BaseTracker(abc.ABC):
  """Abstract base class for all univariate trackers."""
  @abc.abstractmethod
  def push(self, x):
    """Push a new value to the tracker.

    Args:
      x: The value to be pushed.
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def get(self):
    """Get the current tracking value.

    Returns:
      The current tracked value, the type of which depends on the specific
      tracker implementation.
    """
    raise NotImplementedError()


class WindowMode(Enum):
  """Enum representing the window mode for windowed trackers."""
  #: operating on all data points from the beginning.
  LANDMARK = 1
  #: operating on a fixed-size sliding window of recent data points.
  SLIDING = 2


class WindowedTracker(BaseTracker):
  """Abstract base class for trackers that operate on a data window.

  This class provides a foundation for trackers that maintain a window of data,
  either as a landmark window or a sliding window. It provides basic push and
  pop operations.

  Args:
    window_mode: A `WindowMode` enum specifying whether the window is `LANDMARK`
      or `SLIDING`.
    **kwargs: Keyword arguments.
      For `SLIDING` window mode, `window_size` can be specified to set the
      maximum size of the sliding window. Defaults to 100.
  """
  def __init__(self, window_mode, **kwargs):
    if window_mode == WindowMode.SLIDING:
      self._window_size = kwargs.get("window_size", 100)
      self._queue = deque(maxlen=self._window_size)
    self._n = 0
    self._window_mode = window_mode

  def push(self, x):
    """Adds a new value to the data window.

    Args:
      x: The value to be added to the window.
    """
    self._queue.append(x)

  def pop(self):
    """Removes and returns the oldest value from the data window (FIFO).

    Returns:
      The oldest value from the window.
    """
    return self._queue.popleft()
