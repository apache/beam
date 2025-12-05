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
from typing import Optional

import apache_beam as beam
from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.thresholds import FixedThreshold
from apache_beam.ml.anomaly.univariate.base import EPSILON
from apache_beam.ml.anomaly.univariate.quantile import BufferedSlidingQuantileTracker  # pylint: disable=line-too-long
from apache_beam.ml.anomaly.univariate.quantile import QuantileTracker
from apache_beam.ml.anomaly.univariate.quantile import SecondaryBufferedQuantileTracker  # pylint: disable=line-too-long

DEFAULT_WINDOW_SIZE = 1000


@specifiable
class IQR(AnomalyDetector):
  """Interquartile Range (IQR) anomaly detector.

  This class implements an anomaly detection algorithm based on the
  Interquartile Range (IQR) [#]_ . It calculates the IQR using quantile trackers
  for Q1 (25th percentile) and Q3 (75th percentile) and scores data points based
  on their deviation from these quartiles.

  The score is calculated as follows:

    * If a data point is above Q3, the score is (value - Q3) / IQR.
    * If a data point is below Q1, the score is (Q1 - value) / IQR.
    * If a data point is within the IQR (Q1 <= value <= Q3), the score is 0.
      Initializes the IQR anomaly detector.

  Args:
    q1_tracker: Optional QuantileTracker for Q1 (25th percentile). If None, a
      BufferedSlidingQuantileTracker with a default window size is used.
    q3_tracker: Optional QuantileTracker for Q3 (75th percentile). If None, a
      SecondaryBufferedQuantileTracker based on q1_tracker is used.
    threshold_criterion: Optional ThresholdFn to apply on the score. Defaults
      to `FixedThreshold(1.5)` since outliers are commonly defined as data
      points that fall below Q1 - 1.5 IQR or above Q3 + 1.5 IQR.
    **kwargs: Additional keyword arguments.

  .. [#] https://en.wikipedia.org/wiki/Interquartile_range
  """
  def __init__(
      self,
      q1_tracker: Optional[QuantileTracker] = None,
      q3_tracker: Optional[QuantileTracker] = None,
      **kwargs):
    if "threshold_criterion" not in kwargs:
      kwargs["threshold_criterion"] = FixedThreshold(1.5)
    super().__init__(**kwargs)

    self._q1_tracker = q1_tracker or \
        BufferedSlidingQuantileTracker(DEFAULT_WINDOW_SIZE, 0.25)
    assert self._q1_tracker._q == 0.25, \
        "q1_tracker must be initialized with q = 0.25"

    self._q3_tracker = q3_tracker or \
        SecondaryBufferedQuantileTracker(self._q1_tracker, 0.75)
    assert self._q3_tracker._q == 0.75, \
        "q3_tracker must be initialized with q = 0.75"

  def learn_one(self, x: beam.Row) -> None:
    """Updates the quantile trackers with a new data point.

    Args:
      x: A `beam.Row` containing a single numerical value.
    """
    if len(x.__dict__) != 1:
      raise ValueError(
          "IQR.learn_one expected univariate input, but got %s", str(x))

    v = next(iter(x))
    self._q1_tracker.push(v)
    self._q3_tracker.push(v)

  def score_one(self, x: beam.Row) -> Optional[float]:
    """Scores a data point based on its deviation from the IQR.

    Args:
      x: A `beam.Row` containing a single numerical value.

    Returns:
      float | None: The anomaly score.
    """
    if len(x.__dict__) != 1:
      raise ValueError(
          "IQR.score_one expected univariate input, but got %s", str(x))

    v = next(iter(x))
    if v is None or math.isnan(v):
      return None

    q1 = self._q1_tracker.get()
    q3 = self._q3_tracker.get()

    # not enough data points to compute median or median absolute deviation
    if math.isnan(q1) or math.isnan(q3):
      return float('NaN')

    iqr = q3 - q1
    if abs(iqr) < EPSILON:
      return 0.0

    if v > q3:
      return (v - q3) / iqr

    if v < q1:
      return (q1 - v) / iqr

    # q1 <= v <= q3, normal points
    return 0
