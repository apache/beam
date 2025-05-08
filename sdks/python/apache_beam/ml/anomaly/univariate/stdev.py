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

"""Trackers for calculating standard deviation in windowed fashion.

This module defines different types of standard deviation trackers that operate
on windows of data. It includes:

  * `SimpleSlidingStdevTracker`: Calculates stdev using numpy in a sliding
    window.
  * `IncLandmarkStdevTracker`: Incremental stdev tracker in landmark window
    mode.
  * `IncSlidingStdevTracker`: Incremental stdev tracker in sliding window mode.
"""

import math
import warnings

import numpy as np

from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.univariate.base import BaseTracker
from apache_beam.ml.anomaly.univariate.base import WindowedTracker
from apache_beam.ml.anomaly.univariate.base import WindowMode


class StdevTracker(BaseTracker):
  """Abstract base class for standard deviation trackers.

  Currently, it does not add any specific functionality but provides a type
  hierarchy for stdev trackers.
  """
  pass


@specifiable
class SimpleSlidingStdevTracker(WindowedTracker, StdevTracker):
  """Sliding window standard deviation tracker using NumPy.

  This tracker uses NumPy's `nanvar` function to calculate the variance of the
  values currently in the sliding window and then takes the square root to get
  the standard deviation. It's a simple, non-incremental approach.
  """
  def __init__(self, window_size):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)

  def get(self):
    """Calculates and returns the stdev of the current sliding window.

    Returns:
      float: The standard deviation of the values in the current sliding window.
        Returns NaN if the window contains fewer than 2 elements.
    """
    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      # We do not use nanstd, since nanstd([]) gives 0, which is incorrect.
      # Use nanvar instead.
      return math.sqrt(np.nanvar(self._queue, ddof=1))


class IncStdevTracker(WindowedTracker, StdevTracker):
  """Abstract base class for incremental standard deviation trackers.

  This class implements an online algorithm for calculating standard deviation,
  updating the standard deviation incrementally as new data points arrive.

  Args:
    window_mode: A `WindowMode` enum specifying whether the window is `LANDMARK`
      or `SLIDING`.
    **kwargs: Keyword arguments passed to the parent class constructor.
  """
  def __init__(self, window_mode, **kwargs):
    super().__init__(window_mode, **kwargs)
    self._mean = 0
    self._m2 = 0

  def push(self, x):
    """Pushes a new value and updates the incremental standard deviation
    calculation.

    Implements Welford's online algorithm for variance, then derives stdev.

    Args:
      x: The new value to be pushed.
    """
    if not math.isnan(x):
      self._n += 1
      delta1 = x - self._mean
    else:
      delta1 = 0

    if self._window_mode == WindowMode.SLIDING:
      if (len(self._queue) >= self._window_size and
          not math.isnan(old_x := self.pop())):
        self._n -= 1
        delta2 = self._mean - old_x
      else:
        delta2 = 0

      super().push(x)
    else:
      delta2 = 0

    if self._n > 0:
      self._mean += (delta1 + delta2) / self._n

      if delta1 != 0:
        self._m2 += delta1 * (x - self._mean)
      if delta2 != 0:
        self._m2 += delta2 * (old_x - self._mean)
    else:
      self._mean = 0
      self._m2 = 0

  def get(self):
    """Returns the current incremental standard deviation.

    Returns:
      float: The current incremental standard deviation value.
        Returns NaN if fewer than 2 valid (non-NaN) values have been pushed.
    """
    if self._n < 2:
      # keep it consistent with numpy
      return float("nan")
    dof = self._n - 1
    return math.sqrt(self._m2 / dof)


@specifiable
class IncLandmarkStdevTracker(IncStdevTracker):
  """Landmark window standard deviation tracker using incremental calculation."""  # pylint: disable=line-too-long

  def __init__(self):
    super().__init__(window_mode=WindowMode.LANDMARK)


@specifiable
class IncSlidingStdevTracker(IncStdevTracker):
  """Sliding window standard deviation tracker using incremental calculation.

  Args:
    window_size: The size of the sliding window.
  """
  def __init__(self, window_size):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)
