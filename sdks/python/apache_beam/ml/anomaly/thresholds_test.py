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
import unittest

import apache_beam as beam
from apache_beam.ml.anomaly import thresholds
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.univariate.quantile import BufferedSlidingQuantileTracker  # pylint: disable=line-too-long
from apache_beam.ml.anomaly.univariate.quantile import SimpleSlidingQuantileTracker  # pylint: disable=line-too-long
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

R = beam.Row(x=10, y=20)


class TestFixedThreshold(unittest.TestCase):
  def test_apply_only(self):
    threshold_fn = thresholds.FixedThreshold(2)
    self.assertEqual(threshold_fn.apply(1.0), 0)
    self.assertEqual(threshold_fn.apply(2.0), 1)
    self.assertEqual(threshold_fn.apply(None), None)
    self.assertEqual(threshold_fn.apply(float('NaN')), -2)

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
              thresholds.StatelessThresholdDoFn(
                  thresholds.FixedThreshold(2, normal_label=0,
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
              thresholds.StatelessThresholdDoFn(
                  thresholds.FixedThreshold(2, normal_label=0,
                                            outlier_label=1).to_spec())))

      assert_that(result, equal_to(expected))


class TestQuantileThreshold(unittest.TestCase):
  def test_apply_only(self):
    threshold_fn = thresholds.QuantileThreshold(0.9)
    self.assertEqual(threshold_fn.apply(1.0), 1)
    self.assertEqual(threshold_fn.apply(2.0), 1)
    self.assertEqual(threshold_fn.apply(1.2), 0)
    self.assertEqual(threshold_fn.apply(None), None)
    self.assertEqual(threshold_fn.apply(float('NaN')), -2)

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
              thresholds.StatefulThresholdDoFn(
                  thresholds.QuantileThreshold(
                      quantile=0.5, normal_label=0,
                      outlier_label=1).to_spec())))

      assert_that(result, equal_to(expected))

  def test_quantile_tracker(self):
    t1 = thresholds.QuantileThreshold()
    self.assertTrue(isinstance(t1._tracker, BufferedSlidingQuantileTracker))
    self.assertEqual(t1._tracker._q, 0.95)
    self.assertEqual(t1.to_spec(), Spec("QuantileThreshold", config={}))

    t2 = thresholds.QuantileThreshold(quantile=0.99)
    self.assertTrue(isinstance(t2._tracker, BufferedSlidingQuantileTracker))
    self.assertEqual(t2._tracker._q, 0.99)
    self.assertEqual(
        t2.to_spec(), Spec("QuantileThreshold", config={"quantile": 0.99}))

    # argument quantile=0.9 is not used because quantile_tracker is set
    t3 = thresholds.QuantileThreshold(
        quantile=0.9, quantile_tracker=SimpleSlidingQuantileTracker(50, 0.975))
    self.assertTrue(isinstance(t3._tracker, SimpleSlidingQuantileTracker))
    self.assertEqual(t3._tracker._q, 0.975)
    print(t3.to_spec())
    self.assertEqual(
        t3.to_spec(),
        Spec(
            "QuantileThreshold",
            config={
                'quantile': 0.9,
                'quantile_tracker': Spec(
                    type='SimpleSlidingQuantileTracker',
                    config={
                        'window_size': 50, 'q': 0.975
                    })
            }))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
