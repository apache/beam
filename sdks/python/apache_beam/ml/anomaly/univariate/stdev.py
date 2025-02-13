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
    "SimpleSlidingStdevTracker",
    "IncLandmarkStdevTracker",
    "IncSlidingStdevTracker"
]


class StdevTracker(WindowedTracker):
  pass


class SimpleSlidingStdevTracker(StdevTracker):
  def __init__(self, window_size):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)

  def get(self):
    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      # We do not use nanstd, since nanstd([]) gives 0, which is incorrect.
      # Use nanvar instead.
      return math.sqrt(np.nanvar(self._queue, ddof=1))


class IncStdevTracker(StdevTracker):
  def __init__(self, window_mode, **kwargs):
    super().__init__(window_mode, **kwargs)
    self._mean = 0
    self._m2 = 0

  def push(self, x):
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
    if self._n < 2:
      # keep it consistent with numpy
      return float("nan")
    dof = self._n - 1
    return math.sqrt(self._m2 / dof)


class IncLandmarkStdevTracker(IncStdevTracker):
  def __init__(self):
    super().__init__(window_mode=WindowMode.LANDMARK)


class IncSlidingStdevTracker(IncStdevTracker):
  def __init__(self, window_size):
    super().__init__(window_mode=WindowMode.SLIDING, window_size=window_size)
