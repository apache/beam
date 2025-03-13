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

"""Trackers for calculating quantiles in windowed fashion.

This module defines different types of quantile trackers that operate on
windows of data. It includes:

  * `SimpleSlidingQuantileTracker`: Calculates quantile using numpy in a sliding
    window.
  * `BufferedLandmarkQuantileTracker`: Sortedlist based quantile tracker in
    landmark window mode.
  * `BufferedSlidingQuantileTracker`: Sortedlist based quantile tracker in
    sliding window mode.
"""

import math
import typing
import warnings

import numpy as np
from sortedcontainers import SortedList

from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.univariate.base import BaseTracker
from apache_beam.ml.anomaly.univariate.base import WindowedTracker
from apache_beam.ml.anomaly.univariate.base import WindowMode


class QuantileTracker(BaseTracker):
  """Abstract base class for quantile trackers.

  Currently, it does not add any specific functionality but provides a type
  hierarchy for quantile trackers.
  """
  def __init__(self, q):
    assert 0 <= q <= 1, "quantile argument should be between 0 and 1"
    self._q = q


@specifiable
class SimpleSlidingQuantileTracker(WindowedTracker, QuantileTracker):
  """Sliding window quantile tracker using NumPy.

  This tracker uses NumPy's `nanquantile` function to calculate the specified
  quantile of the values currently in the sliding window. It's a simple,
  non-incremental approach.

  Args:
    window_size: The size of the sliding window.
    q: The quantile to calculate, a float between 0 and 1 (inclusive).
  """
  def __init__(self, window_size, q):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)
    QuantileTracker.__init__(self, q)

  def get(self):
    """Calculates and returns the specified quantile of the current sliding
    window.

    Returns:
      float: The specified quantile of the values in the current sliding window.
             Returns NaN if the window is empty.
    """
    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      return np.nanquantile(self._queue, self._q)


class BufferedQuantileTracker(WindowedTracker, QuantileTracker):
  """Abstract base class for buffered quantile trackers.

  Warning:
    Buffered quantile trackers are NOT truly incremental in the sense that they
    don't update the quantile in constant time per new data point. They maintain
    a sorted list of all values in the window.

  Args:
    window_mode: A `WindowMode` enum specifying whether the window is `LANDMARK`
      or `SLIDING`.
    q: The quantile to calculate, a float between 0 and 1 (inclusive).
    **kwargs: Keyword arguments passed to the parent class constructor.
  """
  def __init__(self, window_mode, q, **kwargs):
    super().__init__(window_mode, **kwargs)
    QuantileTracker.__init__(self, q)
    self._sorted_items = SortedList()

  def push(self, x):
    """Pushes a new value, maintains the sorted list, and manages the window.

    Args:
      x: The new value to be pushed.
    """
    if not math.isnan(x):
      self._sorted_items.add(x)

    if self._window_mode == WindowMode.SLIDING:
      if (len(self._queue) >= self._window_size and
          not math.isnan(old_x := self.pop())):
        self._sorted_items.discard(old_x)

      super().push(x)

  @staticmethod
  def _get_helper(sorted_items, q):
    n = len(sorted_items)
    if n < 1:
      return float("nan")

    pos = q * (n - 1)
    lo = math.floor(pos)
    lo_value = typing.cast(float, sorted_items[lo])

    # Use linear interpolation to yield the requested quantile
    hi = min(lo + 1, n - 1)
    hi_value: float = typing.cast(float, sorted_items[hi])
    return lo_value + (hi_value - lo_value) * (pos - lo)

  def get(self):
    """Returns the current quantile value using the sorted list.

    Calculates the quantile using linear interpolation on the sorted values.

    Returns:
      float: The calculated quantile value. Returns NaN if the window is empty.
    """
    return self._get_helper(self._sorted_items, self._q)


@specifiable
class SecondaryBufferedQuantileTracker(WindowedTracker, QuantileTracker):
  """A secondary quantile tracker that shares its data with a master tracker.

  This tracker acts as a read-only view of the master tracker's data, providing
  quantile calculations without maintaining its own independent buffer. It
  relies on the master's sorted items for quantile estimations.

  Args:
    master: The BufferedQuantileTracker instance to share data with.
    q: A list of quantiles to track.
  """
  def __init__(self, master: QuantileTracker, q):
    assert isinstance(master, BufferedQuantileTracker), \
        "Cannot create secondary tracker from non-BufferedQuantileTracker"
    self._master = master
    super().__init__(self._master._window_mode)
    QuantileTracker.__init__(self, q)
    self._sorted_items = self._master._sorted_items

  def push(self, x):
    """Does nothing, as this is a secondary tracker.
    """
    pass

  def get(self):
    """Returns the calculated quantiles based on the master tracker's buffer.

    Returns:
        A list of calculated quantiles.
    """
    return self._master._get_helper(self._master._sorted_items, self._q)


@specifiable
class BufferedLandmarkQuantileTracker(BufferedQuantileTracker):
  """Landmark quantile tracker using a sorted list for quantile calculation.

  Warning:
    Landmark quantile trackers have unbounded memory consumption as they store
    all pushed values in a sorted list. Avoid using in production for
    long-running streams.

  Args:
    q: The quantile to calculate, a float between 0 and 1 (inclusive).
  """
  def __init__(self, q):
    warnings.warn(
        "Quantile trackers should not be used in production due to "
        "the unbounded memory consumption.")
    super().__init__(window_mode=WindowMode.LANDMARK, q=q)


@specifiable
class BufferedSlidingQuantileTracker(BufferedQuantileTracker):
  """Sliding window quantile tracker using a sorted list for quantile
  calculation.

  Warning:
    Maintains a sorted list of values within the sliding window to calculate
    the specified quantile. Memory consumption is bounded by the window size
    but can still be significant for large windows.

  Args:
    window_size: The size of the sliding window.
    q: The quantile to calculate, a float between 0 and 1 (inclusive).
  """
  def __init__(self, window_size, q):
    super().__init__(
        window_mode=WindowMode.SLIDING, q=q, window_size=window_size)
