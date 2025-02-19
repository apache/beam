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

import collections
import math
import statistics
from typing import Callable
from typing import Iterable

from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AggregationFn
from apache_beam.ml.anomaly.specifiable import specifiable


class LabelAggregation(AggregationFn):
  """Aggregates anomaly predictions based on their labels.

  This is an abstract base class for `AggregationFn`s that combine multiple
  `AnomalyPrediction` objects into a single `AnomalyPrediction` based on
  the labels of the input predictions.

  Args:
    agg_func (Callable[[Iterable[int]], int]): A function that aggregates
      a collection of anomaly labels (integers) into a single label.
    include_history (bool): If True, include the input predictions in the
      `agg_history` of the output. Defaults to False.
  """
  def __init__(
      self,
      agg_func: Callable[[Iterable[int]], int],
      include_history: bool = False):
    self._agg = agg_func
    self._include_history = include_history
    self._agg_model_id = None

  def apply(
      self, predictions: Iterable[AnomalyPrediction]) -> AnomalyPrediction:
    """Applies the label aggregation function to a list of predictions.

    Args:
      predictions (Iterable[AnomalyPrediction]): A collection of
        `AnomalyPrediction` objects to be aggregated.

    Returns:
      AnomalyPrediction: A single `AnomalyPrediction` object with the
        aggregated label.
    """
    labels = [
        prediction.label for prediction in predictions
        if prediction.label is not None
    ]

    if len(labels) == 0:
      return AnomalyPrediction(model_id=self._agg_model_id)

    label = self._agg(labels)

    history = list(predictions) if self._include_history else None

    return AnomalyPrediction(
        model_id=self._agg_model_id, label=label, agg_history=history)


class ScoreAggregation(AggregationFn):
  """Aggregates anomaly predictions based on their scores.

  This is an abstract base class for `AggregationFn`s that combine multiple
  `AnomalyPrediction` objects into a single `AnomalyPrediction` based on
  the scores of the input predictions.

  Args:
    agg_func (Callable[[Iterable[float]], float]): A function that aggregates
      a collection of anomaly scores (floats) into a single score.
    include_history (bool): If True, include the input predictions in the
      `agg_history` of the output. Defaults to False.
  """
  def __init__(
      self,
      agg_func: Callable[[Iterable[float]], float],
      include_history: bool = False):
    self._agg = agg_func
    self._include_history = include_history
    self._agg_model_id = None

  def apply(
      self, predictions: Iterable[AnomalyPrediction]) -> AnomalyPrediction:
    """Applies the score aggregation function to a list of predictions.

    Args:
      predictions (Iterable[AnomalyPrediction]): A collection of
        `AnomalyPrediction` objects to be aggregated.

    Returns:
      AnomalyPrediction: A single `AnomalyPrediction` object with the
        aggregated score.
    """
    scores = [
        prediction.score for prediction in predictions
        if prediction.score is not None and not math.isnan(prediction.score)
    ]
    if len(scores) == 0:
      return AnomalyPrediction(model_id=self._agg_model_id)

    score = self._agg(scores)

    history = list(predictions) if self._include_history else None

    return AnomalyPrediction(
        model_id=self._agg_model_id, score=score, agg_history=history)


@specifiable
class MajorityVote(LabelAggregation):
  """Aggregates anomaly labels using majority voting.

  This `AggregationFn` implements a majority voting strategy to combine
  anomaly labels from multiple `AnomalyPrediction` objects. It counts the
  occurrences of normal and outlier labels and selects the label with the
  higher count as the aggregated label. In case of a tie, a tie-breaker
  label is used.

  Example:
    If input labels are [normal, outlier, outlier, normal, outlier], and
    normal_label=0, outlier_label=1, then the aggregated label will be
    outlier (1) because outliers have a majority (3 vs 2).

  Args:
    normal_label (int): The integer label for normal predictions. Defaults to 0.
    outlier_label (int): The integer label for outlier predictions. Defaults to
      1.
    tie_breaker (int): The label to return if there is a tie in votes.
      Defaults to 0 (normal_label).
    **kwargs: Additional keyword arguments to pass to the base
      `LabelAggregation` class.
  """
  def __init__(self, normal_label=0, outlier_label=1, tie_breaker=0, **kwargs):
    self._tie_breaker = tie_breaker
    self._normal_label = normal_label
    self._outlier_label = outlier_label

    def inner(predictions: Iterable[int]) -> int:
      counters = collections.Counter(predictions)
      if counters[self._normal_label] < counters[self._outlier_label]:
        vote = self._outlier_label
      elif counters[self._normal_label] > counters[self._outlier_label]:
        vote = self._normal_label
      else:
        vote = self._tie_breaker
      return vote

    super().__init__(agg_func=inner, **kwargs)


# And scheme
@specifiable
class AllVote(LabelAggregation):
  """Aggregates anomaly labels using an "all vote" (AND) scheme.

  This `AggregationFn` implements an "all vote" strategy. It aggregates
  anomaly labels such that the result is considered an outlier only if all
  input `AnomalyPrediction` objects are labeled as outliers.

  Example:
    If input labels are [outlier, outlier, outlier], and outlier_label=1,
    then the aggregated label will be outlier (1).
    If input labels are [outlier, normal, outlier], and outlier_label=1,
    then the aggregated label will be normal (0).

  Args:
    normal_label (int): The integer label for normal predictions. Defaults to 0.
    outlier_label (int): The integer label for outlier predictions. Defaults to
      1.
    **kwargs: Additional keyword arguments to pass to the base
      `LabelAggregation` class.
  """
  def __init__(self, normal_label=0, outlier_label=1, **kwargs):
    self._normal_label = normal_label
    self._outlier_label = outlier_label

    def inner(predictions: Iterable[int]) -> int:
      return self._outlier_label if all(
          map(lambda p: p == self._outlier_label,
              predictions)) else self._normal_label

    super().__init__(agg_func=inner, **kwargs)


# Or scheme
@specifiable
class AnyVote(LabelAggregation):
  """Aggregates anomaly labels using an "any vote" (OR) scheme.

  This `AggregationFn` implements an "any vote" strategy. It aggregates
  anomaly labels such that the result is considered an outlier if at least
  one of the input `AnomalyPrediction` objects is labeled as an outlier.

  Example:
    If input labels are [normal, normal, outlier], and outlier_label=1,
    then the aggregated label will be outlier (1).
    If input labels are [normal, normal, normal], and outlier_label=1,
    then the aggregated label will be normal (0).

  Args:
    normal_label (int): The integer label for normal predictions. Defaults to 0.
    outlier_label (int): The integer label for outlier predictions. Defaults to
      1.
    **kwargs: Additional keyword arguments to pass to the base
      `LabelAggregation` class.
  """
  def __init__(self, normal_label=0, outlier_label=1, **kwargs):
    self._normal_label = normal_label
    self._outlier_label = outlier_label

    def inner(predictions: Iterable[int]) -> int:
      return self._outlier_label if any(
          map(lambda p: p == self._outlier_label,
              predictions)) else self._normal_label

    super().__init__(agg_func=inner, **kwargs)


@specifiable
class AverageScore(ScoreAggregation):
  """Aggregates anomaly scores by calculating their average.

  This `AggregationFn` computes the average of the anomaly scores from a
  collection of `AnomalyPrediction` objects.

  Args:
    **kwargs: Additional keyword arguments to pass to the base
      `ScoreAggregation` class.
  """
  def __init__(self, **kwargs):
    super().__init__(agg_func=statistics.mean, **kwargs)


@specifiable
class MaxScore(ScoreAggregation):
  """Aggregates anomaly scores by selecting the maximum score.

  This `AggregationFn` selects the highest anomaly score from a collection
  of `AnomalyPrediction` objects as the aggregated score.

  Args:
    **kwargs: Additional keyword arguments to pass to the base
      `ScoreAggregation` class.
  """
  def __init__(self, **kwargs):
    super().__init__(agg_func=max, **kwargs)
