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

"""Trackers for calculating mean in windowed fashion.

This module defines different types of mean trackers that operate on windows
of data. It includes:

  * `SimpleSlidingMeanTracker`: Calculates mean using numpy in a sliding window.
  * `IncLandmarkMeanTracker`: Incremental mean tracker in landmark window mode.
  * `IncSlidingMeanTracker`: Incremental mean tracker in sliding window mode.
"""

import math
import warnings

import numpy as np

from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.univariate.base import BaseTracker
from apache_beam.ml.anomaly.univariate.base import WindowedTracker
from apache_beam.ml.anomaly.univariate.base import WindowMode


class MeanTracker(BaseTracker):
  """Abstract base class for mean trackers.

  Currently, it does not add any specific functionality but provides a type
  hierarchy for mean trackers.
  """
  pass


@specifiable
class SimpleSlidingMeanTracker(WindowedTracker, MeanTracker):
  """Sliding window mean tracker that calculates mean using NumPy.

  This tracker uses NumPy's `nanmean` function to calculate the mean of the
  values currently in the sliding window. It's a simple, non-incremental
  approach.

  Args:
    window_size: The size of the sliding window.
  """
  def __init__(self, window_size):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)

  def get(self):
    """Calculates and returns the mean of the current sliding window.

    Returns:
      float: The mean of the values in the current sliding window.
             Returns NaN if the window is empty.
    """
    if len(self._queue) == 0:
      return float('nan')

    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      return np.nanmean(self._queue)


class IncMeanTracker(WindowedTracker, MeanTracker):
  """Base class for incremental mean trackers.

  This class implements incremental calculation of the mean, which is more
  efficient for streaming data as it updates the mean with each new data point
  instead of recalculating from scratch.

  Args:
    window_mode: A `WindowMode` enum specifying whether the window is `LANDMARK`
      or `SLIDING`.
    **kwargs: Keyword arguments passed to the parent class constructor.
  """
  def __init__(self, window_mode, **kwargs):
    super().__init__(window_mode=window_mode, **kwargs)
    self._mean = 0

  def push(self, x):
    """Pushes a new value and updates the incremental mean.

    Args:
      x: The new value to be pushed.
    """
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
    """Returns the current incremental mean.

    Returns:
      float: The current incremental mean value.
             Returns NaN if no valid (non-NaN) values have been pushed.
    """
    if self._n < 1:
      # keep it consistent with numpy
      return float("nan")
    return self._mean


@specifiable
class IncLandmarkMeanTracker(IncMeanTracker):
  """Landmark window mean tracker using incremental calculation."""
  def __init__(self):
    super().__init__(window_mode=WindowMode.LANDMARK)


@specifiable
class IncSlidingMeanTracker(IncMeanTracker):
  """Sliding window mean tracker using incremental calculation.

  Args:
      window_size: The size of the sliding window.
  """
  def __init__(self, window_size):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)
