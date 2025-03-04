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

import dataclasses
import math
from typing import Any
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import Union

import apache_beam as beam
from apache_beam.coders import DillCoder
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.ml.anomaly.base import ThresholdFn
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.univariate.quantile import BufferedSlidingQuantileTracker  # pylint: disable=line-too-long
from apache_beam.ml.anomaly.univariate.quantile import QuantileTracker
from apache_beam.transforms.userstate import ReadModifyWriteRuntimeState
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec


class BaseThresholdDoFn(beam.DoFn):
  """Applies a ThresholdFn to anomaly detection results.

  This abstract base class defines the structure for DoFns that use a
  `ThresholdFn` to convert anomaly scores into anomaly labels (e.g., normal
  or outlier). It handles the core logic of applying the threshold function
  and updating the prediction labels within `AnomalyResult` objects.

  Args:
    threshold_fn_spec (Spec): Specification defining the `ThresholdFn` to be
      used.
  """
  def __init__(self, threshold_fn_spec: Spec):
    self._threshold_fn_spec = threshold_fn_spec
    self._threshold_fn = None

  def _apply_threshold_to_predictions(
      self, result: AnomalyResult) -> AnomalyResult:
    """Updates the prediction labels in an AnomalyResult using the ThresholdFn.

    Args:
      result (AnomalyResult): The input `AnomalyResult` containing anomaly
        scores.

    Returns:
      AnomalyResult: A new `AnomalyResult` with updated prediction labels
        and threshold values.
    """
    predictions = [
        dataclasses.replace(
            p,
            label=self._threshold_fn.apply(p.score),
            threshold=self._threshold_fn.threshold) for p in result.predictions
    ]
    return dataclasses.replace(result, predictions=predictions)


class StatelessThresholdDoFn(BaseThresholdDoFn):
  """Applies a stateless ThresholdFn to anomaly detection results.

  This DoFn is designed for stateless `ThresholdFn` implementations. It
  initializes the `ThresholdFn` once during setup and applies it to each
  incoming element without maintaining any state across elements.

  Args:
    threshold_fn_spec (Spec): Specification defining the `ThresholdFn` to be
      used.

  Raises:
    AssertionError: If the provided `threshold_fn_spec` leads to the
      creation of a stateful `ThresholdFn`.
  """
  def __init__(self, threshold_fn_spec: Spec):
    threshold_fn_spec.config["_run_init"] = True
    self._threshold_fn: Any = Specifiable.from_spec(threshold_fn_spec)
    assert not self._threshold_fn.is_stateful, \
      "This DoFn can only take stateless function as threshold_fn"

  def process(self, element: Tuple[Any, Tuple[Any, AnomalyResult]],
              **kwargs) -> Iterable[Tuple[Any, Tuple[Any, AnomalyResult]]]:
    """Processes a batch of anomaly results using a stateless ThresholdFn.

    Args:
      element (Tuple[Any, Tuple[Any, AnomalyResult]]): A tuple representing
        an element in the Beam pipeline. It is expected to be in the format
        `(key1, (key2, AnomalyResult))`, where key1 is the original input key,
        and key2 is a disambiguating key for distinct data points.
      **kwargs:  Additional keyword arguments passed to the `process` method
        in Beam DoFns.

    Yields:
      Iterable[Tuple[Any, Tuple[Any, AnomalyResult]]]: An iterable containing
        a single output element with the same structure as the input, but with
        the `AnomalyResult` having updated prediction labels based on the
        stateless `ThresholdFn`.
    """
    k1, (k2, result) = element
    yield k1, (k2, self._apply_threshold_to_predictions(result))


class StatefulThresholdDoFn(BaseThresholdDoFn):
  """Applies a stateful ThresholdFn to anomaly detection results.

  This DoFn is designed for stateful `ThresholdFn` implementations. It leverages
  Beam's state management to persist and update the state of the `ThresholdFn`
  across multiple elements. This is necessary for `ThresholdFn`s that need to
  accumulate information or adapt over time, such as quantile-based thresholds.

  Args:
    threshold_fn_spec (Spec): Specification defining the `ThresholdFn` to be
      used.

  Raises:
    AssertionError: If the provided `threshold_fn_spec` leads to the
      creation of a stateless `ThresholdFn`.
  """
  THRESHOLD_STATE_INDEX = ReadModifyWriteStateSpec('saved_tracker', DillCoder())

  def __init__(self, threshold_fn_spec: Spec):
    threshold_fn_spec.config["_run_init"] = True
    threshold_fn: Any = Specifiable.from_spec(threshold_fn_spec)
    assert threshold_fn.is_stateful, \
      "This DoFn can only take stateful function as threshold_fn"
    self._threshold_fn_spec = threshold_fn_spec

  def process(
      self,
      element: Tuple[Any, Tuple[Any, AnomalyResult]],
      threshold_state: Union[ReadModifyWriteRuntimeState,
                             Any] = beam.DoFn.StateParam(THRESHOLD_STATE_INDEX),
      **kwargs) -> Iterable[Tuple[Any, Tuple[Any, AnomalyResult]]]:
    """Processes a batch of anomaly results using a stateful ThresholdFn.

    For each input element, this DoFn retrieves the stateful `ThresholdFn` from
    Beam state, initializes it if it's the first time, applies it to update
    the prediction labels in the `AnomalyResult`, and then updates the state in
    Beam for future elements.

    Args:
      element (Tuple[Any, Tuple[Any, AnomalyResult]]): A tuple representing
        an element in the Beam pipeline. It is expected to be in the format
        `(key1, (key2, AnomalyResult))`, where key1 is the original input key,
        and key2 is a disambiguating key for distinct data points.
      threshold_state (Union[ReadModifyWriteRuntimeState, Any]): A Beam state
        parameter that provides access to the persisted state of the
        `ThresholdFn`. It is automatically managed by Beam.
      **kwargs: Additional keyword arguments passed to the `process` method
        in Beam DoFns.

    Yields:
      Iterable[Tuple[Any, Tuple[Any, AnomalyResult]]]: An iterable containing
        a single output element with the same structure as the input, but
        with the `AnomalyResult` having updated prediction labels based on
        the stateful `ThresholdFn`.
    """
    k1, (k2, result) = element

    self._threshold_fn = threshold_state.read()
    if self._threshold_fn is None:
      self._threshold_fn: Specifiable = Specifiable.from_spec(
          self._threshold_fn_spec)

    yield k1, (k2, self._apply_threshold_to_predictions(result))

    threshold_state.write(self._threshold_fn)


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
