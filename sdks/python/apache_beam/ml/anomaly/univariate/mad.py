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

"""Trackers for calculating median absolute deviation in windowed fashion."""

from typing import Optional

from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.univariate.base import BaseTracker
from apache_beam.ml.anomaly.univariate.median import MedianTracker


@specifiable
class MadTracker(BaseTracker):
  """Tracks the Median Absolute Deviation (MAD) of a stream of values.

  This class calculates the MAD, a robust measure of statistical dispersion, in
  an online setting.

  Similar functionality is available in the River library:
  https://github.com/online-ml/river/blob/main/river/stats/mad.py

  Important:
    This online version of MAD that does not exactly match its batch
    counterpart. In a streaming data context, where the true median is initially
    unknown, we employ an iterative estimation process. For each incoming data
    point, we first update the estimated median, and then calculate the absolute
    difference between the data point and this updated median. To maintain
    computational efficiency, previously calculated absolute differences are not
    recalculated with each subsequent median update.

  Args:
    median_tracker: An optional `MedianTracker` instance for tracking the
      median of the input values. If None, a default `MedianTracker` is
      created.
    diff_median_tracker: An optional `MedianTracker` instance for tracking
      the median of the absolute deviations from the median. If None, a
      default `MedianTracker` is created.
  """
  def __init__(
      self,
      median_tracker: Optional[MedianTracker] = None,
      diff_median_tracker: Optional[MedianTracker] = None):
    self._median_tracker = median_tracker or MedianTracker()
    self._diff_median_tracker = diff_median_tracker or MedianTracker()

  def push(self, x):
    """Adds a new value to the tracker and updates the MAD.

    Args:
      x: The value to be added to the tracked stream.
    """
    self._median_tracker.push(x)
    median = self._median_tracker.get()
    self._diff_median_tracker.push(abs(x - median))

  def get(self):
    """Retrieves the current MAD value.

    Returns:
      float: The MAD of the values within the defined window. Returns `NaN` if
        the window is empty.
    """
    return self._diff_median_tracker.get()

  def get_median(self):
    """Retrieves the current median value.

    Returns:
      float: The median of the values within the defined window. Returns `NaN`
        if the window is empty.
    """
    return self._median_tracker.get()
