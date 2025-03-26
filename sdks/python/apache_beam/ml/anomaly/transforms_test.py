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
import os
import pickle
import shutil
import tempfile
import unittest
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import SupportsFloat
from typing import Tuple

import mock
import numpy
from sklearn.base import BaseEstimator

import apache_beam as beam
from apache_beam.ml.anomaly.aggregations import AnyVote
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.ml.anomaly.base import EnsembleAnomalyDetector
from apache_beam.ml.anomaly.detectors.offline import OfflineDetector
from apache_beam.ml.anomaly.detectors.zscore import ZScore
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.specifiable import Specifiable
from apache_beam.ml.anomaly.specifiable import _spec_type_to_subspace
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.anomaly.thresholds import FixedThreshold
from apache_beam.ml.anomaly.thresholds import QuantileThreshold
from apache_beam.ml.anomaly.transforms import AnomalyDetection
from apache_beam.ml.anomaly.transforms import _StatefulThresholdDoFn
from apache_beam.ml.anomaly.transforms import _StatelessThresholdDoFn
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.base import _PostProcessingModelHandler
from apache_beam.ml.inference.base import _PreProcessingModelHandler
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
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


class FakeNumpyModel():
  def __init__(self):
    self.total_predict_calls = 0

  def predict(self, input_vector: numpy.ndarray):
    self.total_predict_calls += 1
    return [input_vector[0][0] * 10 - input_vector[0][1]]


def alternate_numpy_inference_fn(
    model: BaseEstimator,
    batch: Sequence[numpy.ndarray],
    inference_args: Optional[Dict[str, Any]] = None) -> Any:
  return [0]


def _to_keyed_numpy_array(t: Tuple[Any, beam.Row]):
  """Converts an Apache Beam Row to a NumPy array."""
  return t[0], numpy.array(list(t[1]))


def _from_keyed_numpy_array(t: Tuple[Any, PredictionResult]):
  assert isinstance(t[1].inference, SupportsFloat)
  return t[0], float(t[1].inference)


class TestOfflineDetector(unittest.TestCase):
  def setUp(self):
    global SklearnModelHandlerNumpy, KeyedModelHandler
    global _PreProcessingModelHandler, _PostProcessingModelHandler
    # Make model handlers into Specifiable
    SklearnModelHandlerNumpy = specifiable(SklearnModelHandlerNumpy)
    KeyedModelHandler = specifiable(KeyedModelHandler)
    _PreProcessingModelHandler = specifiable(_PreProcessingModelHandler)
    _PostProcessingModelHandler = specifiable(_PostProcessingModelHandler)
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)
    # Make the model handlers back to normal
    SklearnModelHandlerNumpy.unspecifiable()
    KeyedModelHandler.unspecifiable()
    _PreProcessingModelHandler.unspecifiable()
    _PostProcessingModelHandler.unspecifiable()

  def test_default_inference_fn(self):
    temp_file_name = self.tmpdir + os.sep + 'pickled_file'
    with open(temp_file_name, 'wb') as file:
      pickle.dump(FakeNumpyModel(), file)

    keyed_model_handler = KeyedModelHandler(
        SklearnModelHandlerNumpy(model_uri=temp_file_name)).with_preprocess_fn(
            _to_keyed_numpy_array).with_postprocess_fn(_from_keyed_numpy_array)

    detector = OfflineDetector(keyed_model_handler=keyed_model_handler)
    detector_spec = detector.to_spec()
    expected_spec = Spec(
        type='OfflineDetector',
        config={
            'keyed_model_handler': Spec(
                type='_PostProcessingModelHandler',
                config={
                    'base': Spec(
                        type='_PreProcessingModelHandler',
                        config={
                            'base': Spec(
                                type='KeyedModelHandler',
                                config={
                                    'unkeyed': Spec(
                                        type='SklearnModelHandlerNumpy',
                                        config={'model_uri': temp_file_name})
                                }),
                            'preprocess_fn': Spec(
                                type='_to_keyed_numpy_array', config=None)
                        }),
                    'postprocess_fn': Spec(
                        type='_from_keyed_numpy_array', config=None)
                })
        })
    self.assertEqual(detector_spec, expected_spec)

    self.assertEqual(_spec_type_to_subspace('SklearnModelHandlerNumpy'), '*')
    self.assertEqual(_spec_type_to_subspace('_PreProcessingModelHandler'), '*')
    self.assertEqual(_spec_type_to_subspace('_PostProcessingModelHandler'), '*')
    self.assertEqual(_spec_type_to_subspace('_to_keyed_numpy_array'), '*')
    self.assertEqual(_spec_type_to_subspace('_from_keyed_numpy_array'), '*')

    # Make sure the spec from the detector can be used to reconstruct the same
    # detector
    detector_new = Specifiable.from_spec(detector_spec)

    input = [
        (1, beam.Row(x=1, y=2)),
        (1, beam.Row(x=2, y=4)),
        (1, beam.Row(x=3, y=6)),
    ]
    expected_predictions = [
        AnomalyPrediction(
            model_id='OfflineDetector',
            score=8.0,
            label=None,
            threshold=None,
            info='',
            source_predictions=None),
        AnomalyPrediction(
            model_id='OfflineDetector',
            score=16.0,
            label=None,
            threshold=None,
            info='',
            source_predictions=None),
        AnomalyPrediction(
            model_id='OfflineDetector',
            score=24.0,
            label=None,
            threshold=None,
            info='',
            source_predictions=None),
    ]
    with TestPipeline() as p:
      result = (
          p | beam.Create(input)
          # TODO: get rid of this conversion between BeamSchema to beam.Row.
          | beam.Map(lambda t: (t[0], beam.Row(**t[1]._asdict())))
          | AnomalyDetection(detector_new))

      assert_that(
          result,
          equal_to([(
              input[0],
              AnomalyResult(example=input[1], predictions=[prediction]))
                    for input,
                    prediction in zip(input, expected_predictions)]))

  def test_run_inference_args(self):
    model_handler = SklearnModelHandlerNumpy(model_uri="unused")
    detector = OfflineDetector(
        keyed_model_handler=model_handler,
        run_inference_args={"inference_args": {
            "multiplier": 10
        }})

    p = TestPipeline()

    input = [
        (1, beam.Row(x=1, y=2)),
        (1, beam.Row(x=2, y=4)),
        (1, beam.Row(x=3, y=6)),
    ]

    # patch the RunInference in "apache_beam.ml.anomaly.transforms" where
    # it is imported and call
    with mock.patch('apache_beam.ml.anomaly.transforms.RunInference') as mock_run_inference:  # pylint: disable=line-too-long
      # make the actual RunInference as the sideeffect, so we record the call
      # information but also create the true RunInference instance.
      mock_run_inference.side_effect = RunInference
      try:
        p = TestPipeline()
        _ = (p | beam.Create(input) | AnomalyDetection(detector))
      except:  # pylint: disable=bare-except
        pass
      call_args = mock_run_inference.call_args[1]
      self.assertEqual(
          call_args,
          {
              'inference_args': {
                  'multiplier': 10
              },
              'model_identifier': 'OfflineDetector'
          })


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
