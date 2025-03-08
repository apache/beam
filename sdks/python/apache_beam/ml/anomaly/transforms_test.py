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

import logging
import math
from typing import Iterable
import unittest

import apache_beam as beam
from apache_beam.ml.anomaly.aggregations import AnyVote
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import EnsembleAnomalyDetector
from apache_beam.ml.anomaly.detectors.zscore import ZScore
from apache_beam.ml.anomaly.transforms import AnomalyDetection
from apache_beam.ml.anomaly.transforms import _StatefulThresholdDoFn
from apache_beam.ml.anomaly.transforms import _StatelessThresholdDoFn
from apache_beam.ml.anomaly.thresholds import FixedThreshold
from apache_beam.ml.anomaly.thresholds import QuantileThreshold
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def _prediction_iterable_is_equal_to(
    a: Iterable[AnomalyPrediction], b: Iterable[AnomalyPrediction]):
  a_list = list(a)
  b_list = list(b)

  if len(a_list) != len(b_list):
    return False

  return any(
      map(lambda x: _prediction_is_equal_to(x[0], x[1]), zip(a_list, b_list)))


def _prediction_is_equal_to(a: AnomalyPrediction, b: AnomalyPrediction):
  if a.model_id != b.model_id:
    return False

  if a.threshold != b.threshold:
    return False

  if a.score != b.score:
    if not (a.score is not None and b.score is not None and
            math.isnan(a.score) and math.isnan(b.score)):
      return False

  if a.label != b.label:
    return False

  if a.info != b.info:
    return False

  if a.source_predictions is None and b.source_predictions is None:
    return True

  if a.source_predictions is not None and b.source_predictions is not None:
    return _prediction_iterable_is_equal_to(
        a.source_predictions, b.source_predictions)

  return False


def _keyed_result_is_equal_to(
    a: tuple[int, AnomalyResult], b: tuple[int, AnomalyResult]):
  if a[0] != b[0]:
    return False

  if a[1].example != b[1].example:
    return False

  return _prediction_iterable_is_equal_to(a[1].predictions, b[1].predictions)


class TestAnomalyDetection(unittest.TestCase):
  def setUp(self):
    self._input = [
        (1, beam.Row(x1=1, x2=4)),
        (2, beam.Row(x1=100, x2=5)),  # an row with a different key (key=2)
        (1, beam.Row(x1=2, x2=4)),
        (1, beam.Row(x1=3, x2=5)),
        (1, beam.Row(x1=10, x2=4)),  # outlier in key=1, with respect to x1
        (1, beam.Row(x1=2, x2=10)),  # outlier in key=1, with respect to x2
        (1, beam.Row(x1=3, x2=4)),
    ]

  def test_one_detector(self):
    zscore_x1_expected = [
        AnomalyPrediction(
            model_id='zscore_x1', score=float('NaN'), label=-2, threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1', score=float('NaN'), label=-2, threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1', score=float('NaN'), label=-2, threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1',
            score=2.1213203435596424,
            label=0,
            threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1', score=8.0, label=1, threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1',
            score=0.4898979485566356,
            label=0,
            threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1',
            score=0.16452254913212455,
            label=0,
            threshold=3),
    ]
    detector = ZScore(features=["x1"], model_id="zscore_x1")

    with TestPipeline() as p:
      result = (
          p | beam.Create(self._input)
          # TODO: get rid of this conversion between BeamSchema to beam.Row.
          | beam.Map(lambda t: (t[0], beam.Row(**t[1]._asdict())))
          | AnomalyDetection(detector))
      assert_that(
          result,
          equal_to([(
              input[0], AnomalyResult(example=input[1], predictions=[decision]))
                    for input,
                    decision in zip(self._input, zscore_x1_expected)],
                   _keyed_result_is_equal_to))

  def test_multiple_detectors_without_aggregation(self):
    zscore_x1_expected = [
        AnomalyPrediction(
            model_id='zscore_x1', score=float('NaN'), label=-2, threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1', score=float('NaN'), label=-2, threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1', score=float('NaN'), label=-2, threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1',
            score=2.1213203435596424,
            label=0,
            threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1', score=8.0, label=1, threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1',
            score=0.4898979485566356,
            label=0,
            threshold=3),
        AnomalyPrediction(
            model_id='zscore_x1',
            score=0.16452254913212455,
            label=0,
            threshold=3),
    ]
    zscore_x2_expected = [
        AnomalyPrediction(
            model_id='zscore_x2', score=float('NaN'), label=-2, threshold=2),
        AnomalyPrediction(
            model_id='zscore_x2', score=float('NaN'), label=-2, threshold=2),
        AnomalyPrediction(
            model_id='zscore_x2', score=float('NaN'), label=-2, threshold=2),
        AnomalyPrediction(model_id='zscore_x2', score=0, label=0, threshold=2),
        AnomalyPrediction(
            model_id='zscore_x2',
            score=0.5773502691896252,
            label=0,
            threshold=2),
        AnomalyPrediction(
            model_id='zscore_x2', score=11.5, label=1, threshold=2),
        AnomalyPrediction(
            model_id='zscore_x2',
            score=0.5368754921931594,
            label=0,
            threshold=2),
    ]

    sub_detectors = []
    sub_detectors.append(ZScore(features=["x1"], model_id="zscore_x1"))
    sub_detectors.append(
        ZScore(
            features=["x2"],
            threshold_criterion=FixedThreshold(2),
            model_id="zscore_x2"))

    with beam.Pipeline() as p:
      result = (
          p | beam.Create(self._input)
          # TODO: get rid of this conversion between BeamSchema to beam.Row.
          | beam.Map(lambda t: (t[0], beam.Row(**t[1]._asdict())))
          | AnomalyDetection(EnsembleAnomalyDetector(sub_detectors)))

      assert_that(
          result,
          equal_to([(
              input[0],
              AnomalyResult(
                  example=input[1], predictions=[decision1, decision2]))
                    for input,
                    decision1,
                    decision2 in zip(
                        self._input, zscore_x1_expected, zscore_x2_expected)],
                   _keyed_result_is_equal_to))

  def test_multiple_sub_detectors_with_aggregation(self):
    aggregated = [
        AnomalyPrediction(model_id="custom", label=-2),
        AnomalyPrediction(model_id="custom", label=-2),
        AnomalyPrediction(model_id="custom", label=-2),
        AnomalyPrediction(model_id="custom", label=0),
        AnomalyPrediction(model_id="custom", label=1),
        AnomalyPrediction(model_id="custom", label=1),
        AnomalyPrediction(model_id="custom", label=0),
    ]

    sub_detectors = []
    sub_detectors.append(ZScore(features=["x1"], model_id="zscore_x1"))
    sub_detectors.append(
        ZScore(
            features=["x2"],
            threshold_criterion=FixedThreshold(2),
            model_id="zscore_x2"))

    with beam.Pipeline() as p:
      result = (
          p | beam.Create(self._input)
          # TODO: get rid of this conversion between BeamSchema to beam.Row.
          | beam.Map(lambda t: (t[0], beam.Row(**t[1]._asdict())))
          | AnomalyDetection(
              EnsembleAnomalyDetector(
                  sub_detectors, aggregation_strategy=AnyVote())))

      assert_that(
          result,
          equal_to([(
              input[0],
              AnomalyResult(example=input[1], predictions=[prediction]))
                    for input,
                    prediction in zip(self._input, aggregated)]))


R = beam.Row(x=10, y=20)


class TestStatelessThresholdDoFn(unittest.TestCase):
  def test_dofn_on_single_prediction(self):
    input = [
        (1, (2, AnomalyResult(R, [AnomalyPrediction(score=1)]))),
        (1, (3, AnomalyResult(R, [AnomalyPrediction(score=2)]))),
        (1, (4, AnomalyResult(R, [AnomalyPrediction(score=3)]))),
    ]
    expected = [
        (
            1,
            (
                2,
                AnomalyResult(
                    R, [AnomalyPrediction(score=1, label=0, threshold=2)]))),
        (
            1,
            (
                3,
                AnomalyResult(
                    R, [AnomalyPrediction(score=2, label=1, threshold=2)]))),
        (
            1,
            (
                4,
                AnomalyResult(
                    R, [AnomalyPrediction(score=3, label=1, threshold=2)]))),
    ]
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(input)
          | beam.ParDo(
              _StatelessThresholdDoFn(
                  FixedThreshold(2, normal_label=0,
                                 outlier_label=1).to_spec())))
      assert_that(result, equal_to(expected))

  def test_dofn_on_multiple_predictions(self):
    input = [
        (
            1,
            (
                2,
                AnomalyResult(
                    R,
                    [AnomalyPrediction(score=1), AnomalyPrediction(score=4)]))),
        (
            1,
            (
                3,
                AnomalyResult(
                    R,
                    [AnomalyPrediction(score=2), AnomalyPrediction(score=0.5)
                     ]))),
    ]
    expected = [
        (
            1,
            (
                2,
                AnomalyResult(
                    R,
                    [
                        AnomalyPrediction(score=1, label=0, threshold=2),
                        AnomalyPrediction(score=4, label=1, threshold=2)
                    ]))),
        (
            1,
            (
                3,
                AnomalyResult(
                    R,
                    [
                        AnomalyPrediction(score=2, label=1, threshold=2),
                        AnomalyPrediction(score=0.5, label=0, threshold=2)
                    ]))),
    ]
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(input)
          | beam.ParDo(
              _StatelessThresholdDoFn(
                  FixedThreshold(2, normal_label=0,
                                 outlier_label=1).to_spec())))

      assert_that(result, equal_to(expected))


class TestStatefulThresholdDoFn(unittest.TestCase):
  def test_dofn_on_single_prediction(self):
    # use the input data with two keys to test stateful threshold function
    input = [
        (1, (2, AnomalyResult(R, [AnomalyPrediction(score=1)]))),
        (1, (3, AnomalyResult(R, [AnomalyPrediction(score=2)]))),
        (1, (4, AnomalyResult(R, [AnomalyPrediction(score=3)]))),
        (2, (2, AnomalyResult(R, [AnomalyPrediction(score=10)]))),
        (2, (3, AnomalyResult(R, [AnomalyPrediction(score=20)]))),
        (2, (4, AnomalyResult(R, [AnomalyPrediction(score=30)]))),
    ]
    expected = [
        (
            1,
            (
                2,
                AnomalyResult(
                    R, [AnomalyPrediction(score=1, label=1, threshold=1)]))),
        (
            1,
            (
                3,
                AnomalyResult(
                    R, [AnomalyPrediction(score=2, label=1, threshold=1.5)]))),
        (
            2,
            (
                2,
                AnomalyResult(
                    R, [AnomalyPrediction(score=10, label=1, threshold=10)]))),
        (
            2,
            (
                3,
                AnomalyResult(
                    R, [AnomalyPrediction(score=20, label=1, threshold=15)]))),
        (
            1,
            (
                4,
                AnomalyResult(
                    R, [AnomalyPrediction(score=3, label=1, threshold=2)]))),
        (
            2,
            (
                4,
                AnomalyResult(
                    R, [AnomalyPrediction(score=30, label=1, threshold=20)]))),
    ]
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(input)
          # use median just for test convenience
          | beam.ParDo(
              _StatefulThresholdDoFn(
                  QuantileThreshold(
                      quantile=0.5, normal_label=0,
                      outlier_label=1).to_spec())))

      assert_that(result, equal_to(expected))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.WARNING)
  unittest.main()
