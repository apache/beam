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

"""Base classes for anomaly detection"""
from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Iterable
from typing import List
from typing import Optional

import apache_beam as beam


@dataclass(frozen=True)
class AnomalyPrediction():
  model_id: Optional[str] = None
  score: Optional[float] = None
  label: Optional[int] = None
  threshold: Optional[float] = None
  info: str = ""
  agg_history: Optional[Iterable[AnomalyPrediction]] = None


@dataclass(frozen=True)
class AnomalyResult():
  example: beam.Row
  prediction: AnomalyPrediction


class ThresholdFn(abc.ABC):
  def __init__(self, normal_label: int = 0, outlier_label: int = 1):
    self._normal_label = normal_label
    self._outlier_label = outlier_label

  @property
  @abc.abstractmethod
  def is_stateful(self) -> bool:
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def threshold(self) -> Optional[float]:
    raise NotImplementedError

  @abc.abstractmethod
  def apply(self, score: Optional[float]) -> int:
    raise NotImplementedError


class AggregationFn(abc.ABC):
  @abc.abstractmethod
  def apply(
      self, predictions: Iterable[AnomalyPrediction]) -> AnomalyPrediction:
    raise NotImplementedError


class AnomalyDetector(abc.ABC):
  def __init__(
      self,
      model_id: Optional[str] = None,
      features: Optional[Iterable[str]] = None,
      target: Optional[str] = None,
      threshold_criterion: Optional[ThresholdFn] = None,
      **kwargs):
    self._model_id = model_id if model_id is not None else getattr(
        self, '_key', 'unknown')
    self._features = features
    self._target = target
    self._threshold_criterion = threshold_criterion

  @abc.abstractmethod
  def learn_one(self, x: beam.Row) -> None:
    raise NotImplementedError

  @abc.abstractmethod
  def score_one(self, x: beam.Row) -> float:
    raise NotImplementedError


class EnsembleAnomalyDetector(AnomalyDetector):
  def __init__(
      self,
      sub_detectors: Optional[List[AnomalyDetector]] = None,
      aggregation_strategy: Optional[AggregationFn] = None,
      **kwargs):
    if "model_id" not in kwargs or kwargs["model_id"] is None:
      kwargs["model_id"] = getattr(self, '_key', 'custom')

    super().__init__(**kwargs)

    self._aggregation_strategy = aggregation_strategy
    self._sub_detectors = sub_detectors

  def learn_one(self, x: beam.Row) -> None:
    raise NotImplementedError

  def score_one(self, x: beam.Row) -> float:
    raise NotImplementedError
