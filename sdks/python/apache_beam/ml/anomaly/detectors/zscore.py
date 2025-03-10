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
from apache_beam.ml.anomaly.univariate.mean import IncSlidingMeanTracker
from apache_beam.ml.anomaly.univariate.mean import MeanTracker
from apache_beam.ml.anomaly.univariate.stdev import IncSlidingStdevTracker
from apache_beam.ml.anomaly.univariate.stdev import StdevTracker

DEFAULT_WINDOW_SIZE = 1000


# pylint: disable=line-too-long
@specifiable
class ZScore(AnomalyDetector):
  """Z-Score anomaly detector.

  This class implements an anomaly detection algorithm based on Z-Score (also
  known as Standard Score [#]_ ), which measures how many standard deviations a
  data point is from the mean.

  The score is calculated as: `| (value - mean) / stdev |`

  Important:
    In the streaming setting, we use the online version of mean and standard
    deviation in the calculation.

  This implementation is adapted from the implementations within PySAD [#]_ and
  River [#]_:

    * https://github.com/selimfirat/pysad/blob/master/pysad/models/standard_absolute_deviation.py
    * https://github.com/online-ml/river/blob/main/river/anomaly/sad.py

  Args:
    sub_stat_tracker: Optional `MeanTracker` instance. If None, an
      `IncSlidingMeanTracker` with a default window size 1000 is created.
    stdev_tracker: Optional `StdevTracker` instance. If None, an
      `IncSlidingStdevTracker` with a default window size 1000 is created.
    threshold_criterion: threshold_criterion: Optional `ThresholdFn` to apply on
      the score. Defaults to `FixedThreshold(3)` due to the commonly used
      3-sigma rule.
    **kwargs: Additional keyword arguments.

  .. [#] https://en.wikipedia.org/wiki/Standard_score
  .. [#] Yilmaz, Selim & Kozat, Suleyman. (2020). PySAD: A Streaming Anomaly Detection Framework in Python. 10.48550/arXiv.2009.02572.
  .. [#] Jacob Montiel, Max Halford, Saulo Martiello Mastelini, Geoffrey Bolmier, Raphaël Sourty, et al.. (2021). River: machine learning for streaming data in Python. Journal of Machine Learning Research, 2021, 22, pp.1-8.
  """

  # pylint: enable=line-too-long
  def __init__(
      self,
      sub_stat_tracker: Optional[MeanTracker] = None,
      stdev_tracker: Optional[StdevTracker] = None,
      **kwargs):
    if "threshold_criterion" not in kwargs:
      kwargs["threshold_criterion"] = FixedThreshold(3)
    super().__init__(**kwargs)

    self._sub_stat_tracker = sub_stat_tracker or IncSlidingMeanTracker(
        DEFAULT_WINDOW_SIZE)
    self._stdev_tracker = stdev_tracker or IncSlidingStdevTracker(
        DEFAULT_WINDOW_SIZE)

  def learn_one(self, x: beam.Row) -> None:
    """Updates the mean and standard deviation trackers with a new data point.

    Args:
      x: A `beam.Row` containing a single numerical value.
    """
    if len(x.__dict__) != 1:
      raise ValueError(
          "ZScore.learn_one expected univariate input, but got %s", str(x))

    v = next(iter(x))
    self._stdev_tracker.push(v)
    self._sub_stat_tracker.push(v)

  def score_one(self, x: beam.Row) -> Optional[float]:
    """Scores a data point using the Z-Score.

    Args:
      x: A `beam.Row` containing a single numerical value.

    Returns:
      float | None: The Z-Score.
    """
    if len(x.__dict__) != 1:
      raise ValueError(
          "ZScore.score_one expected univariate input, but got %s", str(x))

    v = next(iter(x))
    if v is None or math.isnan(v):
      return None

    sub_stat = self._sub_stat_tracker.get()
    stdev = self._stdev_tracker.get()

    # not enough data points to compute sub_stat or standard deviation
    if math.isnan(stdev) or math.isnan(sub_stat):
      return float('NaN')

    if abs(stdev) < EPSILON:
      return 0.0

    return abs((v - sub_stat) / stdev)
