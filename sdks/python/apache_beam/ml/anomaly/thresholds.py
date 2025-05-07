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

from __future__ import annotations

import math
from typing import Optional

from apache_beam.ml.anomaly.base import ThresholdFn
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.univariate.quantile import BufferedSlidingQuantileTracker  # pylint: disable=line-too-long
from apache_beam.ml.anomaly.univariate.quantile import QuantileTracker


@specifiable
class FixedThreshold(ThresholdFn):
  """Applies a fixed cutoff value to anomaly scores.

  This `ThresholdFn` is stateless and uses a pre-defined cutoff value to
  classify anomaly scores. Scores below the cutoff are considered normal, while
  scores at or above the cutoff are classified as outliers.

  Args:
    cutoff (float): The fixed threshold value. Anomaly scores at or above this
      value will be labeled as outliers.
    **kwargs: Additional keyword arguments to be passed to the base
      `ThresholdFn` constructor.
  """
  def __init__(self, cutoff: float, **kwargs):
    super().__init__(**kwargs)
    self._cutoff = cutoff

  @property
  def is_stateful(self) -> bool:
    """Indicates whether this ThresholdFn is stateful.

    Returns:
      bool: Always False for `FixedThreshold` as it is stateless.
    """
    return False

  @property
  def threshold(self) -> float:
    """Returns the fixed cutoff threshold value.

    Returns:
      float: The fixed threshold value.
    """
    return self._cutoff

  def apply(self, score: Optional[float]) -> Optional[int]:
    """Applies the fixed threshold to an anomaly score.

    Classifies the given anomaly score as normal or outlier based on the
    predefined cutoff.

    Args:
      score (Optional[float]): The input anomaly score.

    Returns:
      Optional[int]: The anomaly label:
        - `normal_label` if the score is less than the threshold.
        - `outlier_label` if the score is at or above the threshold.
        - `missing_label` if the score is `NaN` (detector not ready).
        - `None` if the score is `None` (detector ready, but unable to produce
          score).
    """
    # score error: detector is ready but is unable to produce the score due to
    # errors such as ill-formatted input.
    if score is None:
      return None

    # score missing: detector is not yet ready
    if math.isnan(score):
      return self._missing_label

    if score < self.threshold:
      return self._normal_label

    return self._outlier_label


@specifiable
class QuantileThreshold(ThresholdFn):
  """Applies a quantile-based dynamic threshold to anomaly scores.

  This `ThresholdFn` is stateful and uses a quantile tracker to dynamically
  determine the threshold for anomaly detection. It estimates the specified
  quantile of the incoming anomaly scores and uses this quantile value as the
  threshold.

  The threshold adapts over time as more data is processed, making it suitable
  for scenarios where the distribution of anomaly scores may change.

  Args:
    quantile (Optional[float]): The quantile to be tracked (e.g., 0.95 for the
      95th percentile). This value determines the dynamic threshold. Defaults to
      0.95.
    quantile_tracker (Optional[BufferedQuantileTracker]): An optional
      pre-initialized quantile tracker. If provided, this tracker will be used;
      otherwise, a `BufferedSlidingQuantileTracker` will be created with a
      default window size of 100.
    **kwargs: Additional keyword arguments to be passed to the base
      `ThresholdFn` constructor.
  """
  def __init__(
      self,
      quantile: Optional[float] = 0.95,
      quantile_tracker: Optional[QuantileTracker] = None,
      **kwargs):
    super().__init__(**kwargs)
    if quantile_tracker is not None:
      self._tracker = quantile_tracker
    else:
      self._tracker = BufferedSlidingQuantileTracker(
          window_size=100, q=quantile)

  @property
  def is_stateful(self) -> bool:
    """Indicates whether this ThresholdFn is stateful.

    Returns:
      bool: Always True for `QuantileThreshold` as it is stateful.
    """
    return True

  @property
  def threshold(self) -> float:
    """Returns the current quantile-based threshold value.

    Returns:
      float: The dynamically calculated threshold value based on the quantile
        tracker.
    """
    return self._tracker.get()

  def apply(self, score: Optional[float]) -> Optional[int]:
    """Applies the quantile-based threshold to an anomaly score.

    Updates the quantile tracker with the given score and classifies the score
    as normal or outlier based on the current quantile threshold.

    Args:
      score (Optional[float]): The input anomaly score.

    Returns:
      Optional[int]: The anomaly label:
        - `normal_label` if the score is less than the threshold.
        - `outlier_label` if the score is at or above the threshold.
        - `missing_label` if the score is `NaN` (detector not ready).
        - `None` if the score is `None` (detector ready, but unable to produce
          score).
    """
    # score error: detector is ready but is unable to produce the score due to
    # errors such as ill-formatted input.
    if score is None:
      # store NaN instead of None in quantile tracker to simplify tracker logic.
      # After all, it is only a place holder and will not be used in calculation
      self._tracker.push(float("NaN"))
      return None

    self._tracker.push(score)
    # score missing: detector is not yet ready
    if math.isnan(score):
      return self._missing_label

    if score < self.threshold:
      return self._normal_label

    return self._outlier_label
