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

import math
import warnings

import numpy as np

from apache_beam.ml.anomaly.univariate.base import WindowMode
from apache_beam.ml.anomaly.univariate.base import WindowedTracker

__all__ = [
    "SimpleSlidingMeanTracker",
    "IncLandmarkMeanTracker",
    "IncSlidingMeanTracker"
]


class MeanTracker(WindowedTracker):
  pass


class SimpleSlidingMeanTracker(MeanTracker):
  def __init__(self, window_size):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)

  def get(self):
    if len(self._queue) == 0:
      return float('nan')

    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      return np.nanmean(self._queue)


class IncMeanTracker(MeanTracker):
  def __init__(self, window_mode, **kwargs):
    super().__init__(window_mode=window_mode, **kwargs)
    self._mean = 0

  def push(self, x):
    if not math.isnan(x):
      self._n += 1
      delta = x - self._mean
    else:
      delta = 0

    if self._window_mode == WindowMode.SLIDING:
      if len(self._queue) >= self._window_size and \
          not math.isnan(old_x := self.pop()):
        self._n -= 1
        delta += (self._mean - old_x)

      super().push(x)

    if self._n > 0:
      self._mean += delta / self._n
    else:
      self._mean = 0

  def get(self):
    if self._n < 1:
      # keep it consistent with numpy
      return float("nan")
    return self._mean


class IncLandmarkMeanTracker(IncMeanTracker):
  def __init__(self):
    super().__init__(window_mode=WindowMode.LANDMARK)


class IncSlidingMeanTracker(IncMeanTracker):
  def __init__(self, window_size):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)
