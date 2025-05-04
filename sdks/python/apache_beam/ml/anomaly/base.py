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

"""
Base classes for anomaly detection
"""
from __future__ import annotations

import abc
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional

import apache_beam as beam

__all__ = [
    "AnomalyPrediction",
    "AnomalyResult",
    "ThresholdFn",
    "AggregationFn",
    "AnomalyDetector",
    "EnsembleAnomalyDetector"
]

DEFAULT_NORMAL_LABEL = 0
DEFAULT_OUTLIER_LABEL = 1
DEFAULT_MISSING_LABEL = -2


@dataclass(frozen=True)
class AnomalyPrediction():
  """A dataclass for anomaly detection predictions."""
  #: The ID of detector (model) that generates the prediction.
  model_id: Optional[str] = None
  #: The outlier score resulting from applying the detector to the input data.
  score: Optional[float] = None
  #: The outlier label (normal or outlier) derived from the outlier score.
  label: Optional[int] = None
  #: The threshold used to determine the label.
  threshold: Optional[float] = None
  #: Additional information about the prediction.
  info: str = ""
  #: If enabled, a list of `AnomalyPrediction` objects used to derive the
  #: aggregated prediction.
  source_predictions: Optional[Iterable[AnomalyPrediction]] = None


@dataclass(frozen=True)
class AnomalyResult():
  """A dataclass for the anomaly detection results"""
  #: The original input data.
  example: beam.Row
  #: The iterable of `AnomalyPrediction` objects containing the predictions.
  #: Expect length 1 if it is a result for a non-ensemble detector or an
  #: ensemble detector with an aggregation strategy applied.
  predictions: Iterable[AnomalyPrediction]


class ThresholdFn(abc.ABC):
  """An abstract base class for threshold functions.

  Args:
    normal_label: The integer label used to identify normal data. Defaults to 0.
    outlier_label: The integer label used to identify outlier data. Defaults to
      1.
    missing_label: The integer label used when a score is missing because the
      model is not ready to score.
  """
  def __init__(
      self,
      normal_label: int = DEFAULT_NORMAL_LABEL,
      outlier_label: int = DEFAULT_OUTLIER_LABEL,
      missing_label: int = DEFAULT_MISSING_LABEL):
    self._normal_label = normal_label
    self._outlier_label = outlier_label
    self._missing_label = missing_label

  @property
  @abc.abstractmethod
  def is_stateful(self) -> bool:
    """Indicates whether the threshold function is stateful or not."""
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def threshold(self) -> Optional[float]:
    """Retrieves the current threshold value, or None if not set."""
    raise NotImplementedError

  @abc.abstractmethod
  def apply(self, score: Optional[float]) -> Optional[int]:
    """Applies the threshold function to a given score to classify it as
    normal or outlier.

    Args:
      score: The outlier score generated from the detector (model).

    Returns:
      The label assigned to the score, either `self._normal_label`
      or `self._outlier_label`
    """
    raise NotImplementedError


class AggregationFn(abc.ABC):
  """An abstract base class for aggregation functions."""
  @abc.abstractmethod
  def apply(
      self, predictions: Iterable[AnomalyPrediction]) -> AnomalyPrediction:
    """Applies the aggregation function to an iterable of predictions, either on
    their outlier scores or labels.

    Args:
      predictions: An Iterable of `AnomalyPrediction` objects to aggregate.

    Returns:
      An `AnomalyPrediction` object containing the aggregated result.
    """
    raise NotImplementedError


class AnomalyDetector(abc.ABC):
  """An abstract base class for anomaly detectors.

  Args:
    model_id: The ID of detector (model). Defaults to the value of the
      `spec_type` attribute, or 'unknown' if not set.
    features: An Iterable of strings representing the names of the input
      features in the `beam.Row`
    target: The name of the target field in the `beam.Row`.
    threshold_criterion: An optional `ThresholdFn` to apply to the outlier score
      and yield a label.
  """
  def __init__(
      self,
      model_id: Optional[str] = None,
      features: Optional[Iterable[str]] = None,
      target: Optional[str] = None,
      threshold_criterion: Optional[ThresholdFn] = None,
      **kwargs):
    self._model_id = model_id if model_id is not None else getattr(
        self, 'spec_type', lambda: "unknown")()
    self._features = features
    self._target = target
    self._threshold_criterion = threshold_criterion

  @abc.abstractmethod
  def learn_one(self, x: beam.Row) -> None:
    """Trains the detector on a single data instance.

    Args:
      x: A `beam.Row` representing the data instance.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def score_one(self, x: beam.Row) -> Optional[float]:
    """Scores a single data instance for anomalies.

    Args:
      x: A `beam.Row` representing the data instance.

    Returns:
      The outlier score as a float. None if an exception occurs during scoring,
      and NaN if the model is not ready.
    """
    raise NotImplementedError


class EnsembleAnomalyDetector(AnomalyDetector):
  """An abstract base class for an ensemble of anomaly (sub-)detectors.

  Args:
    sub_detectors: A list of `AnomalyDetector` used in this ensemble model.
    aggregation_strategy: An optional `AggregationFn` to apply to the
      predictions from all sub-detectors and yield an aggregated result.
    model_id: Inherited from `AnomalyDetector`.
    features: Inherited from `AnomalyDetector`.
    target: Inherited from `AnomalyDetector`.
    threshold_criterion: Inherited from `AnomalyDetector`.
  """
  def __init__(
      self,
      sub_detectors: Optional[list[AnomalyDetector]] = None,
      aggregation_strategy: Optional[AggregationFn] = None,
      **kwargs):
    if "model_id" not in kwargs or kwargs["model_id"] is None:
      kwargs["model_id"] = getattr(self, 'spec_type', lambda: 'custom')()

    super().__init__(**kwargs)

    self._aggregation_strategy = aggregation_strategy
    self._sub_detectors = sub_detectors

  def learn_one(self, x: beam.Row) -> None:
    """Inherited from `AnomalyDetector.learn_one`.

    This method is never called during ensemble detector training. The training
    process is done on each sub-detector independently and in parallel.
    """
    raise NotImplementedError

  def score_one(self, x: beam.Row) -> float:
    """Inherited from `AnomalyDetector.score_one`.

    This method is never called during ensemble detector scoring. The scoring
    process is done on sub-detector independently and in parallel, and then
    the results are aggregated in the pipeline.
    """
    raise NotImplementedError
