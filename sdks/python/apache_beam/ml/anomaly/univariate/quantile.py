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
import typing
import warnings

import numpy as np
from sortedcontainers import SortedList

from apache_beam.ml.anomaly.univariate.base import WindowMode
from apache_beam.ml.anomaly.univariate.base import WindowedTracker

__all__ = [
    "IncLandmarkQuantileTracker",
    "SimpleSlidingQuantileTracker",
    "IncSlidingQuantileTracker"
]


class QuantileTracker(WindowedTracker):
  pass


class SimpleSlidingQuantileTracker(QuantileTracker):
  def __init__(self, window_size, q):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)
    assert 0 <= q <= 1, "quantile argument should be between 0 and 1"
    self._q = q

  def get(self):
    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      return np.nanquantile(self._queue, self._q)


class IncQuantileTracker(WindowedTracker):
  def __init__(self, window_mode, q, **kwargs):
    super().__init__(window_mode, **kwargs)
    assert 0 <= q <= 1, "quantile argument should be between 0 and 1"
    self._q = q
    self._sorted_items = SortedList()

  def push(self, x):
    if not math.isnan(x):
      self._sorted_items.add(x)

    if self._window_mode == WindowMode.SLIDING:
      if (len(self._queue) >= self._window_size and
          not math.isnan(old_x := self.pop())):
        self._sorted_items.discard(old_x)

      super().push(x)

  def get(self):
    n = len(self._sorted_items)
    if n < 1:
      return float("nan")

    pos = self._q * (n - 1)
    lo = math.floor(pos)
    lo_value = typing.cast(float, self._sorted_items[lo])

    # Use linear interpolation to yield the requested quantile
    hi = min(lo + 1, n - 1)
    hi_value: float = typing.cast(float, self._sorted_items[hi])
    return lo_value + (hi_value - lo_value) * (pos - lo)


class IncLandmarkQuantileTracker(IncQuantileTracker):
  def __init__(self, q):
    warnings.warn(
        "Quantile trackers should not be used in production due to "
        "the unbounded memory consumption.")
    super().__init__(window_mode=WindowMode.LANDMARK, q=q)


class IncSlidingQuantileTracker(IncQuantileTracker):
  def __init__(self, window_size, q):
    super().__init__(
        window_mode=WindowMode.SLIDING, q=q, window_size=window_size)
