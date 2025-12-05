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
from collections.abc import Callable
from collections.abc import Iterable
from typing import Any
from typing import Optional

from apache_beam.ml.anomaly.base import DEFAULT_MISSING_LABEL
from apache_beam.ml.anomaly.base import DEFAULT_NORMAL_LABEL
from apache_beam.ml.anomaly.base import DEFAULT_OUTLIER_LABEL
from apache_beam.ml.anomaly.base import AggregationFn
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.specifiable import specifiable


class _AggModelIdMixin:
  def __init__(self, agg_model_id: Optional[str] = None):
    self._agg_model_id = agg_model_id

  def _set_agg_model_id_if_unset(self, agg_model_id: str) -> None:
    if self._agg_model_id is None:
      self._agg_model_id = agg_model_id

  def add_model_id(self, result_dict):
    result_dict["model_id"] = self._agg_model_id


class _SourcePredictionMixin:
  def __init__(self, include_source_predictions):
    self._include_source_predictions = include_source_predictions

  def add_source_predictions(self, result_dict, source_predictions):
    if self._include_source_predictions:
      result_dict["source_predictions"] = list(source_predictions)


class LabelAggregation(AggregationFn, _AggModelIdMixin, _SourcePredictionMixin):
  """Aggregates anomaly predictions based on their labels.

  This is an abstract base class for `AggregationFn`s that combine multiple
  `AnomalyPrediction` objects into a single `AnomalyPrediction` based on
  the labels of the input predictions.

  Args:
    agg_func (Callable[[Iterable[int]], int]): A function that aggregates
      a collection of anomaly labels (integers) into a single label.
    agg_model_id (Optional[str]): The model id used in aggregated predictions.
      Defaults to None.
    include_source_predictions (bool): If True, include the input predictions in
      the `source_predictions` of the output. Defaults to False.
  """
  def __init__(
      self,
      agg_func: Callable[[Iterable[int]], int],
      agg_model_id: Optional[str] = None,
      include_source_predictions: bool = False,
      normal_label: int = DEFAULT_NORMAL_LABEL,
      outlier_label: int = DEFAULT_OUTLIER_LABEL,
      missing_label: int = DEFAULT_MISSING_LABEL,
  ):
    self._agg = agg_func
    self._normal_label = normal_label
    self._outlier_label = outlier_label
    self._missing_label = missing_label
    _AggModelIdMixin.__init__(self, agg_model_id)
    _SourcePredictionMixin.__init__(self, include_source_predictions)

  def apply(
      self, predictions: Iterable[AnomalyPrediction]) -> AnomalyPrediction:
    """Applies the label aggregation function to a list of predictions.

    Args:
      predictions (Iterable[AnomalyPrediction]): A collection of
        `AnomalyPrediction` objects to be aggregated.

    Returns:
      AnomalyPrediction: A single `AnomalyPrediction` object with the
        aggregated label. The aggregated label is determined as follows:

        - If there are any non-missing and non-error labels, the `agg_func` is
          applied to aggregate them.
        - If all labels are error labels (`None`), the aggregated label is also
          `None`.
        - If there are a mix of missing and error labels, the aggregated label
          is the `missing_label`.
    """
    result_dict: dict[str, Any] = {}
    _AggModelIdMixin.add_model_id(self, result_dict)
    _SourcePredictionMixin.add_source_predictions(
        self, result_dict, predictions)

    labels = [
        prediction.label for prediction in predictions if
        prediction.label is not None and prediction.label != self._missing_label
    ]

    if len(labels) > 0:
      # apply aggregation_fn if there is any non-None and non-missing label
      result_dict["label"] = self._agg(labels)
    elif all(map(lambda x: x.label is None, predictions)):
      # all are error labels (None) -- all scores are error
      result_dict["label"] = None
    else:
      # some missing labels with some error labels (None)
      result_dict["label"] = self._missing_label

    return AnomalyPrediction(**result_dict)


class ScoreAggregation(AggregationFn, _AggModelIdMixin, _SourcePredictionMixin):
  """Aggregates anomaly predictions based on their scores.

  This is an abstract base class for `AggregationFn`s that combine multiple
  `AnomalyPrediction` objects into a single `AnomalyPrediction` based on
  the scores of the input predictions.

  Args:
    agg_func (Callable[[Iterable[float]], float]): A function that aggregates
      a collection of anomaly scores (floats) into a single score.
    agg_model_id (Optional[str]): The model id used in aggregated predictions.
      Defaults to None.
    include_source_predictions (bool): If True, include the input predictions in
      the `source_predictions` of the output. Defaults to False.
  """
  def __init__(
      self,
      agg_func: Callable[[Iterable[float]], float],
      agg_model_id: Optional[str] = None,
      include_source_predictions: bool = False):
    self._agg = agg_func
    _AggModelIdMixin.__init__(self, agg_model_id)
    _SourcePredictionMixin.__init__(self, include_source_predictions)

  def apply(
      self, predictions: Iterable[AnomalyPrediction]) -> AnomalyPrediction:
    """Applies the score aggregation function to a list of predictions.

    Args:
      predictions (Iterable[AnomalyPrediction]): A collection of
        `AnomalyPrediction` objects to be aggregated.

    Returns:
      AnomalyPrediction: A single `AnomalyPrediction` object with the
        aggregated score. The aggregated score is determined as follows:

        - If there are any non-missing and non-error scores, the `agg_func` is
          applied to aggregate them.
        - If all scores are error scores (`None`), the aggregated score is also
          `None`.
        - If there are a mix of missing (`NaN`) and error scores (`None`), the
          aggregated score is `NaN`.
    """
    result_dict: dict[str, Any] = {}
    _AggModelIdMixin.add_model_id(self, result_dict)
    _SourcePredictionMixin.add_source_predictions(
        self, result_dict, predictions)

    scores = [
        prediction.score for prediction in predictions
        if prediction.score is not None and not math.isnan(prediction.score)
    ]

    if len(scores) > 0:
      # apply aggregation_fn if there is any non-None and non-NaN score
      result_dict["score"] = self._agg(scores)
    elif all(map(lambda x: x.score is None, predictions)):
      # all are error scores (None)
      result_dict["score"] = None
    else:
      # some missing scores (NaN) with some error scores (None)
      result_dict["score"] = float("NaN")

    return AnomalyPrediction(**result_dict)


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
  def __init__(self, tie_breaker=DEFAULT_NORMAL_LABEL, **kwargs):
    self._tie_breaker = tie_breaker

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
  def __init__(self, **kwargs):
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
  def __init__(self, **kwargs):
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
