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
from apache_beam.ml.anomaly.univariate.mad import MadTracker


# pylint: disable=line-too-long
@specifiable
class RobustZScore(AnomalyDetector):
  """Robust Z-Score anomaly detector.

  This class implements an detection algorithm based on Robust Z-Score (also
  known as Modified Z-Score), which is a robust alternative to the traditional
  Z-score [#]_. It uses the median and Median Absolute Deviation (MAD) to
  compute a score that is less sensitive to outliers.

  The score is calculated as: `|0.6745 * (value - median) / MAD|`

  Important:
    In the streaming setting, we use the online version of median and MAD in the
    calculation. Therefore, the score computed here does not exactly match its
    batch counterpart.

  This implementation is adapted from the implementation within PySAD [#]_:
  https://github.com/selimfirat/pysad/blob/master/pysad/models/median_absolute_deviation.py

  The batch version can be seen at PyOD [#]_:
  https://github.com/yzhao062/pyod/blob/master/pyod/models/mad.py


  Args:
    mad_tracker: Optional `MadTracker` instance. If None, a default `MadTracker`
      is created.
    threshold_criterion: threshold_criterion: Optional `ThresholdFn` to apply on
      the score. Defaults to `FixedThreshold(3)` due to the commonly used
      3-sigma rule.
    **kwargs: Additional keyword arguments.

  .. [#] Hoaglin, David C.. (2013). Volume 16: How to Detect and Handle Outliers.
  .. [#] Yilmaz, Selim & Kozat, Suleyman. (2020). PySAD: A Streaming Anomaly Detection Framework in Python. 10.48550/arXiv.2009.02572.
  .. [#] Zhao, Y., Nasrullah, Z. and Li, Z.. (2019). PyOD: A Python Toolbox for Scalable Outlier Detection. Journal of machine learning research (JMLR), 20(96), pp.1-7.
  """
  # pylint: enable=line-too-long
  SCALE_FACTOR = 0.6745

  def __init__(self, mad_tracker: Optional[MadTracker] = None, **kwargs):
    if "threshold_criterion" not in kwargs:
      kwargs["threshold_criterion"] = FixedThreshold(3)
    super().__init__(**kwargs)
    self._mad_tracker = mad_tracker or MadTracker()

  def learn_one(self, x: beam.Row) -> None:
    """Updates the `MadTracker` with a new data point.

    Args:
      x: A `beam.Row` containing a single numerical value.
    """
    if len(x.__dict__) != 1:
      raise ValueError(
          "RobustZScore.learn_one expected univariate input, but got %s",
          str(x))

    v = next(iter(x))
    self._mad_tracker.push(v)

  def score_one(self, x: beam.Row) -> Optional[float]:
    """Scores a data point using the Robust Z-Score.

    Args:
      x: A `beam.Row` containing a single numerical value.

    Returns:
      float | None: The Robust Z-Score.
    """
    if len(x.__dict__) != 1:
      raise ValueError(
          "RobustZScore.score_one expected univariate input, but got %s",
          str(x))

    v = next(iter(x))
    if v is None or math.isnan(v):
      return None

    median = self._mad_tracker.get_median()
    mad = self._mad_tracker.get()

    # not enough data points to compute median or median absolute deviation
    if math.isnan(mad) or math.isnan(median):
      return float('NaN')

    if abs(mad) < EPSILON:
      return 0.0

    return abs(RobustZScore.SCALE_FACTOR * (v - median) / mad)
