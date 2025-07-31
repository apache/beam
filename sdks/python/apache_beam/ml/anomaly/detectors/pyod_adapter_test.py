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
import os.path
import pickle
import shutil
import tempfile
import unittest

import numpy as np
from parameterized import parameterized

import apache_beam as beam
from apache_beam.ml.anomaly.base import AnomalyPrediction
from apache_beam.ml.anomaly.base import AnomalyResult
from apache_beam.ml.anomaly.transforms import AnomalyDetection
from apache_beam.ml.anomaly.transforms_test import _keyed_result_is_equal_to
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where onnx and pytorch library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.ml.anomaly.detectors.pyod_adapter import PyODFactory
  from pyod.models.iforest import IForest
except ImportError:
  raise unittest.SkipTest('PyOD dependencies are not installed')


class PyODIForestTest(unittest.TestCase):
  def setUp(self) -> None:
    self.tmp_dir = tempfile.mkdtemp()

    seed = 1234
    model = IForest(random_state=seed)
    model.fit(self.get_train_data())
    self.pickled_model_uri = os.path.join(self.tmp_dir, 'iforest_pickled')

    with open(self.pickled_model_uri, 'wb') as fp:
      pickle.dump(model, fp)

  def tearDown(self) -> None:
    shutil.rmtree(self.tmp_dir)

  def get_train_data(self):
    return [
        np.array([1, 5], dtype="float32"),
        np.array([2, 6], dtype="float32"),
        np.array([3, 4], dtype="float32"),
        np.array([2, 6], dtype="float32"),
        np.array([10, 10], dtype="float32"),  # need an outlier in training data
        np.array([3, 4], dtype="float32"),
        np.array([2, 6], dtype="float32"),
        np.array([2, 6], dtype="float32"),
        np.array([2, 5], dtype="float32"),
    ]

  def get_test_data(self):
    return [
        np.array([2, 6], dtype="float32"),
        np.array([100, 100], dtype="float32"),
    ]

  def get_test_data_with_target(self):
    return [
        np.array([2, 6, 0], dtype="float32"),
        np.array([100, 100, 1], dtype="float32"),
    ]

  @parameterized.expand([True, False])
  def test_scoring_with_matched_features(self, with_target):
    if with_target:
      rows = [beam.Row(a=2, b=6, target=0), beam.Row(a=100, b=100, target=1)]
      field_names = ["a", "b", "target"]
      # The selected features should match the features used for training
      detector = PyODFactory.create_detector(
          self.pickled_model_uri, features=["a", "b"])
      input_data = self.get_test_data_with_target()
    else:
      rows = [beam.Row(a=2, b=6), beam.Row(a=100, b=100)]
      field_names = ["a", "b"]
      detector = PyODFactory.create_detector(self.pickled_model_uri)
      input_data = self.get_test_data()

    expected_out = [(
        0,
        AnomalyResult(
            example=rows[0],
            predictions=[
                AnomalyPrediction(
                    model_id='OfflineDetector',
                    score=-0.20316164744828075,
                    label=0,
                    threshold=8.326672684688674e-17,
                    info='',
                    source_predictions=None)
            ])),
                    (
                        0,
                        AnomalyResult(
                            example=rows[1],
                            predictions=[
                                AnomalyPrediction(
                                    model_id='OfflineDetector',
                                    score=0.179516865091218,
                                    label=1,
                                    threshold=8.326672684688674e-17,
                                    info='',
                                    source_predictions=None)
                            ]))]

    options = PipelineOptions([])
    with beam.Pipeline(options=options) as p:
      out = (
          p | beam.Create(input_data)
          | beam.Map(lambda x: beam.Row(**dict(zip(field_names, map(int, x)))))
          | beam.WithKeys(0)
          | AnomalyDetection(detector=detector))
      assert_that(out, equal_to(expected_out, _keyed_result_is_equal_to))

  def test_scoring_with_unmatched_features(self):
    # The model is trained with two features: a, b, but the input features of
    # scoring has one more feature (target).
    # In this case, we should either get rid of the extra feature(s) from
    # the scoring input or set `features` when creating the offline detector
    # (see the `test_scoring_with_matched_features`)
    detector = PyODFactory.create_detector(self.pickled_model_uri)
    options = PipelineOptions([])
    # This should raise a ValueError with message
    # "X has 3 features, but IsolationForest is expecting 2 features as input."
    with self.assertRaisesRegex(Exception, "is expecting 2 features"):
      with beam.Pipeline(options=options) as p:
        _ = (
            p | beam.Create(self.get_test_data_with_target())
            | beam.Map(
                lambda x: beam.Row(
                    **dict(zip(["a", "b", "target"], map(int, x)))))
            | beam.WithKeys(0)
            | AnomalyDetection(detector=detector))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.WARNING)
  unittest.main()
