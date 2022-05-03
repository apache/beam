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

# pytype: skip-file

import os
import pickle
import platform
import shutil
import sys
import tempfile
import unittest

import joblib
import numpy
from sklearn import svm

import apache_beam as beam
import apache_beam.ml.inference.api as api
import apache_beam.ml.inference.base as base
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnInferenceRunner
from apache_beam.ml.inference.sklearn_inference import SklearnModelLoader
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def _compare_prediction_result(a, b):
  example_equal = numpy.array_equal(a.example, b.example)
  return a.inference == b.inference and example_equal


class FakeModel:
  def __init__(self):
    self.total_predict_calls = 0

  def predict(self, input_vector: numpy.array):
    self.total_predict_calls += 1
    return numpy.sum(input_vector, axis=1)


def build_model():
  x = [[0, 0], [1, 1]]
  y = [0, 1]
  model = svm.SVC()
  model.fit(x, y)
  return model


class SkLearnRunInferenceTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_predict_output(self):
    fake_model = FakeModel()
    inference_runner = SklearnInferenceRunner()
    batched_examples = [
        numpy.array([1, 2, 3]), numpy.array([4, 5, 6]), numpy.array([7, 8, 9])
    ]
    expected_predictions = [
        api.PredictionResult(numpy.array([1, 2, 3]), 6),
        api.PredictionResult(numpy.array([4, 5, 6]), 15),
        api.PredictionResult(numpy.array([7, 8, 9]), 24)
    ]
    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_data_vectorized(self):
    fake_model = FakeModel()
    inference_runner = SklearnInferenceRunner()
    batched_examples = [
        numpy.array([1, 2, 3]), numpy.array([4, 5, 6]), numpy.array([7, 8, 9])
    ]
    # even though there are 3 examples, the data should
    # be vectorized and only 1 call should happen.
    inference_runner.run_inference(batched_examples, fake_model)
    self.assertEqual(1, fake_model.total_predict_calls)

  def test_num_bytes(self):
    inference_runner = SklearnInferenceRunner()
    batched_examples_int = [
        numpy.array([1, 2, 3]), numpy.array([4, 5, 6]), numpy.array([7, 8, 9])
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_int[0]) * 3,
        inference_runner.get_num_bytes(batched_examples_int))

    batched_examples_float = [
        numpy.array([1.0, 2.0, 3.0]),
        numpy.array([4.1, 5.2, 6.3]),
        numpy.array([7.7, 8.8, 9.9])
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_float[0]) * 3,
        inference_runner.get_num_bytes(batched_examples_float))

  @unittest.skipIf(platform.system() == 'Windows', 'BEAM-14359')
  def test_pipeline_pickled(self):
    temp_file_name = self.tmpdir + os.sep + 'pickled_file'
    with open(temp_file_name, 'wb') as file:
      pickle.dump(build_model(), file)
    with TestPipeline() as pipeline:
      examples = [numpy.array([0, 0]), numpy.array([1, 1])]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      #TODO(BEAM-14305) Test against the public API.
      actual = pcoll | base.RunInference(
          SklearnModelLoader(model_uri=temp_file_name))
      expected = [
          api.PredictionResult(numpy.array([0, 0]), 0),
          api.PredictionResult(numpy.array([1, 1]), 1)
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_prediction_result))

  @unittest.skipIf(platform.system() == 'Windows', 'BEAM-14359')
  def test_pipeline_joblib(self):
    temp_file_name = self.tmpdir + os.sep + 'joblib_file'
    with open(temp_file_name, 'wb') as file:
      joblib.dump(build_model(), file)
    with TestPipeline() as pipeline:
      examples = [numpy.array([0, 0]), numpy.array([1, 1])]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      #TODO(BEAM-14305) Test against the public API.

      actual = pcoll | base.RunInference(
          SklearnModelLoader(
              model_uri=temp_file_name, model_file_type=ModelFileType.JOBLIB))
      expected = [
          api.PredictionResult(numpy.array([0, 0]), 0),
          api.PredictionResult(numpy.array([1, 1]), 1)
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_prediction_result))

  def test_bad_file_raises(self):
    with self.assertRaises(RuntimeError):
      with TestPipeline() as pipeline:
        examples = [numpy.array([0, 0])]
        pcoll = pipeline | 'start' >> beam.Create(examples)
        # TODO(BEAM-14305) Test against the public API.
        _ = pcoll | base.RunInference(
            SklearnModelLoader(model_uri='/var/bad_file_name'))
        pipeline.run()

  @unittest.skipIf(platform.system() == 'Windows', 'BEAM-14359')
  def test_bad_input_type_raises(self):
    with self.assertRaisesRegex(AssertionError,
                                'Unsupported serialization type'):
      with tempfile.NamedTemporaryFile() as file:
        model_loader = SklearnModelLoader(
            model_uri=file.name, model_file_type=None)
        model_loader.load_model()


if __name__ == '__main__':
  unittest.main()
