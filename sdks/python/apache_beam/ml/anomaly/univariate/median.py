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

"""Trackers for calculating median in windowed fashion."""

from typing import Optional

from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.univariate.base import BaseTracker
from apache_beam.ml.anomaly.univariate.quantile import BufferedSlidingQuantileTracker  # pylint: disable=line-too-long
from apache_beam.ml.anomaly.univariate.quantile import QuantileTracker

DEFAULT_WINDOW_SIZE = 1000


@specifiable
class MedianTracker(BaseTracker):
  """Tracks the median of a stream of values using a quantile tracker.

    This wrapper class encapsulates a `QuantileTracker` configured specifically
    for the 0.5 quantile (median).

    Args:
      quantile_tracker: An optional `QuantileTracker` instance. If not provided,
        a `BufferedSlidingQuantileTracker` with a default window size 1000 and
        q=0.5 is created.

    Raises:
      AssertionError: If the provided quantile_tracker is not initialized with
        q=0.5.
  """
  def __init__(self, quantile_tracker: Optional[QuantileTracker] = None):
    self._quantile_tracker = quantile_tracker or BufferedSlidingQuantileTracker(
        DEFAULT_WINDOW_SIZE, 0.5)
    assert self._quantile_tracker._q == 0.5, \
        "quantile_tracker must be initialized with q = 0.5"

  def push(self, x):
    """Pushes a new value and updates the internal quantile tracker.

    Args:
      x: The new value to be pushed.
    """
    self._quantile_tracker.push(x)

  def get(self):
    """Calculates and returns the median (q = 0.5).

    Returns:
      float: The median of the values in the window setting specified in the
             internal quantile tracker. Returns NaN if the window is empty.
    """
    return self._quantile_tracker.get()
