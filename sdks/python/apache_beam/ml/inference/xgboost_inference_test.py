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

import os
import shutil
import sys
import tempfile
import unittest
import zipfile
from typing import Any
from typing import Tuple

try:
  import datatable
  import numpy
  import pandas
  import pytest
  import scipy
  import xgboost

  import apache_beam as beam
  from apache_beam.ml.inference import RunInference
  from apache_beam.ml.inference.base import KeyedModelHandler
  from apache_beam.ml.inference.base import PredictionResult
  from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerDatatable
  from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerNumpy
  from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerPandas
  from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerSciPy
  from apache_beam.testing.test_pipeline import TestPipeline
  from apache_beam.testing.util import assert_that
  from apache_beam.testing.util import equal_to
except ImportError:
  raise unittest.SkipTest('XGBoost dependencies are not installed')


def _compare_prediction_result(a: PredictionResult, b: PredictionResult):
  if isinstance(a.example, scipy.sparse.csr_matrix) and isinstance(
      b.example, scipy.sparse.csr_matrix):
    example_equal = numpy.array_equal(a.example.todense(), b.example.todense())

  else:
    example_equal = numpy.array_equal(a.example, b.example)  # type: ignore[arg-type]
  if isinstance(a.inference, dict):
    return all(
        x == y for x, y in zip(a.inference.values(),
                               b.inference.values())) and example_equal
  return a.inference == b.inference and example_equal


def _compare_keyed_prediction_result(
    a: Tuple[Any, PredictionResult], b: Tuple[Any, PredictionResult]):
  a_key, a_val = a
  b_key, b_val = b
  keys_equal = a_key == b_key
  return _compare_prediction_result(a_val, b_val) and keys_equal


def predict_fn(self, data):
  self.inference_calls += 1
  if isinstance(data, pandas.DataFrame):
    data = data.to_numpy()
  if isinstance(data, datatable.Frame):
    data = data.to_numpy()
  if isinstance(data, scipy.sparse.csr_matrix):
    data = data.toarray()
  return sum(sum(array) for array in data)


@pytest.fixture(autouse=True)
def predict_patched(monkeypatch):
  monkeypatch.setattr(xgboost.XGBClassifier, 'predict', predict_fn)


def build_monkeypatched_xgboost_classifier() -> xgboost.XGBClassifier:
  model = xgboost.XGBClassifier()
  model.inference_calls = 0
  model.fit([[0, 0], [0, 1], [1, 0], [1, 1]], [0, 1, 0, 1])
  return model


@pytest.mark.uses_xgboost
class XGBoostRunInferenceTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_predict_output(self):
    model = build_monkeypatched_xgboost_classifier()
    inference_runner = XGBoostModelHandlerNumpy(xgboost.XGBClassifier, 'unused')
    batched_examples = [
        numpy.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
        numpy.array([[1, 1, 1], [1, 1, 1], [1, 1, 1]])
    ]
    expected_predictions = [
        PredictionResult(numpy.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]), 45),
        PredictionResult(numpy.array([[1, 1, 1], [1, 1, 1], [1, 1, 1]]), 9),
    ]
    inferences = inference_runner.run_inference(batched_examples, model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_single_inference_call(self):
    model = build_monkeypatched_xgboost_classifier()
    inference_runner = XGBoostModelHandlerNumpy(xgboost.XGBClassifier, 'unused')
    self.assertEqual(model.inference_calls, 0)
    batched_examples = [numpy.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]
    _ = inference_runner.run_inference(batched_examples, model)
    self.assertEqual(model.inference_calls, 1)

  def test_multiple_inference_calls(self):
    model = build_monkeypatched_xgboost_classifier()
    inference_runner = XGBoostModelHandlerNumpy(xgboost.XGBClassifier, 'unused')
    self.assertEqual(model.inference_calls, 0)
    batched_examples = [
        numpy.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
        numpy.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
        numpy.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
        numpy.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
        numpy.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    ]
    _ = inference_runner.run_inference(batched_examples, model)
    self.assertEqual(model.inference_calls, 5)

  def test_num_bytes_numpy(self):
    inference_runner = XGBoostModelHandlerNumpy(
        model_class=xgboost.XGBClassifier, model_state='unused')
    batched_examples_int = [
        numpy.array([[1, 1], [2, 2]]),
        numpy.array([[2, 4], [6, 8]]),
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_int[0]) +
        sys.getsizeof(batched_examples_int[1]),
        inference_runner.get_num_bytes(batched_examples_int))

    batched_examples_float = [
        numpy.array([[1.0, 1.0], [2.0, 2.0]]),
        numpy.array([[2.0, 4.0], [6.0, 8.0]]),
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_float[0]) +
        sys.getsizeof(batched_examples_float[1]),
        inference_runner.get_num_bytes(batched_examples_float))

  def test_num_bytes_pandas(self):
    inference_runner = XGBoostModelHandlerPandas(
        model_class=xgboost.XGBClassifier, model_state='unused')
    batched_examples_int = [
        pandas.DataFrame([[1, 1], [2, 2]]),
        pandas.DataFrame([[2, 4], [6, 8]]),
    ]
    self.assertEqual(
        batched_examples_int[0].memory_usage(deep=True).sum() +
        batched_examples_int[1].memory_usage(deep=True).sum(),
        inference_runner.get_num_bytes(batched_examples_int))

    batched_examples_float = [
        pandas.DataFrame([[1.0, 1.0], [2.0, 2.0]]),
        pandas.DataFrame([[2.0, 4.0], [6.0, 8.0]]),
    ]
    self.assertEqual(
        batched_examples_float[0].memory_usage(deep=True).sum() +
        batched_examples_float[1].memory_usage(deep=True).sum(),
        inference_runner.get_num_bytes(batched_examples_float))

  def test_num_bytes_datatable(self):
    inference_runner = XGBoostModelHandlerDatatable(
        model_class=xgboost.XGBClassifier, model_state='unused')
    batched_examples_int = [
        datatable.Frame([[1, 1], [2, 2]]),
        datatable.Frame([[2, 4], [6, 8]]),
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_int[0]) +
        sys.getsizeof(batched_examples_int[1]),
        inference_runner.get_num_bytes(batched_examples_int))

    batched_examples_float = [
        datatable.Frame([[1.0, 1.0], [2.0, 2.0]]),
        datatable.Frame([[2.0, 4.0], [6.0, 8.0]]),
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_float[0]) +
        sys.getsizeof(batched_examples_float[1]),
        inference_runner.get_num_bytes(batched_examples_float))

  def test_num_bytes_scipy(self):
    inference_runner = XGBoostModelHandlerSciPy(
        model_class=xgboost.XGBClassifier, model_state='unused')
    batched_examples_int = [
        scipy.sparse.csr_matrix([[1, 1], [2, 2]]),
        scipy.sparse.csr_matrix([[2, 4], [6, 8]]),
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_int[0]) +
        sys.getsizeof(batched_examples_int[1]),
        inference_runner.get_num_bytes(batched_examples_int))

    batched_examples_float = [
        scipy.sparse.csr_matrix([[1.0, 1.0], [2.0, 2.0]]),
        scipy.sparse.csr_matrix([[2.0, 4.0], [6.0, 8.0]]),
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_float[0]) +
        sys.getsizeof(batched_examples_float[1]),
        inference_runner.get_num_bytes(batched_examples_float))

  def test_pipeline_numpy(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    with TestPipeline() as pipeline:
      examples = [
          numpy.array([[1, 1], [2, 2]]),
          numpy.array([[2, 4], [6, 8]]),
      ]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          XGBoostModelHandlerNumpy(
              model_class=xgboost.XGBClassifier, model_state=model_state))
      expected = [
          PredictionResult(numpy.array([[1, 1], [2, 2]]), 6),
          PredictionResult(numpy.array([[2, 4], [6, 8]]), 20)
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_prediction_result))

  def test_pipeline_numpy_sets_env_vars_correctly(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)
    os.environ.pop('FOO', None)
    self.assertFalse('FOO' in os.environ)

    with TestPipeline() as pipeline:
      examples = [
          numpy.array([[1, 1], [2, 2]]),
          numpy.array([[2, 4], [6, 8]]),
      ]
      handler_with_vars = XGBoostModelHandlerNumpy(
          env_vars={'FOO': 'bar'},
          model_class=xgboost.XGBClassifier,
          model_state=model_state)
      _ = (
          pipeline
          | 'start' >> beam.Create(examples)
          | RunInference(handler_with_vars))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue((os.environ['FOO']) == 'bar')

  def test_pipeline_pandas(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    with TestPipeline() as pipeline:
      examples = [
          pandas.DataFrame([[1, 1], [2, 2]]),
          pandas.DataFrame([[2, 4], [6, 8]]),
      ]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          XGBoostModelHandlerPandas(
              model_class=xgboost.XGBClassifier, model_state=model_state))
      expected = [
          PredictionResult(pandas.DataFrame([[1, 1], [2, 2]]), 6),
          PredictionResult(pandas.DataFrame([[2, 4], [6, 8]]), 20)
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_prediction_result))
      pipeline.run()

  def test_pipeline_pandas_sets_env_vars_correctly(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)
    os.environ.pop('FOO', None)
    self.assertFalse('FOO' in os.environ)

    with TestPipeline() as pipeline:
      examples = [
          pandas.DataFrame([[1, 1], [2, 2]]),
          pandas.DataFrame([[2, 4], [6, 8]]),
      ]
      handler_with_vars = XGBoostModelHandlerPandas(
          env_vars={'FOO': 'bar'},
          model_class=xgboost.XGBClassifier,
          model_state=model_state)
      _ = (
          pipeline
          | 'start' >> beam.Create(examples)
          | RunInference(handler_with_vars))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue((os.environ['FOO']) == 'bar')

  def test_pipeline_datatable(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    with TestPipeline() as pipeline:
      examples = [
          datatable.Frame([[1, 1], [2, 2]]),
          datatable.Frame([[2, 4], [6, 8]]),
      ]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          XGBoostModelHandlerDatatable(
              model_class=xgboost.XGBClassifier, model_state=model_state))
      expected = [
          PredictionResult(datatable.Frame([[1, 1], [2, 2]]), 6),
          PredictionResult(datatable.Frame([[2, 4], [6, 8]]), 20)
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_prediction_result))

  def test_pipeline_datatable_sets_env_vars_correctly(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)
    os.environ.pop('FOO', None)
    self.assertFalse('FOO' in os.environ)

    with TestPipeline() as pipeline:
      examples = [
          datatable.Frame([[1, 1], [2, 2]]),
          datatable.Frame([[2, 4], [6, 8]]),
      ]
      handler_with_vars = XGBoostModelHandlerDatatable(
          env_vars={'FOO': 'bar'},
          model_class=xgboost.XGBClassifier,
          model_state=model_state)
      _ = (
          pipeline
          | 'start' >> beam.Create(examples)
          | RunInference(handler_with_vars))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue((os.environ['FOO']) == 'bar')

  def test_pipeline_scipy(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    with TestPipeline() as pipeline:
      examples = [
          scipy.sparse.csr_matrix(numpy.array([[1, 1], [2, 2]])),
          scipy.sparse.csr_matrix(numpy.array([[2, 4], [6, 8]])),
      ]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          XGBoostModelHandlerSciPy(
              model_class=xgboost.XGBClassifier, model_state=model_state))
      expected = [
          PredictionResult(
              scipy.sparse.csr_matrix(numpy.array([[1, 1], [2, 2]])), 6),
          PredictionResult(
              scipy.sparse.csr_matrix(numpy.array([[2, 4], [6, 8]])), 20)
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_prediction_result))

  def test_pipeline_scipy_sets_env_vars_correctly(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)
    os.environ.pop('FOO', None)
    self.assertFalse('FOO' in os.environ)

    with TestPipeline() as pipeline:
      examples = [
          scipy.sparse.csr_matrix(numpy.array([[1, 1], [2, 2]])),
          scipy.sparse.csr_matrix(numpy.array([[2, 4], [6, 8]])),
      ]
      handler_with_vars = XGBoostModelHandlerSciPy(
          env_vars={'FOO': 'bar'},
          model_class=xgboost.XGBClassifier,
          model_state=model_state)
      _ = (
          pipeline
          | 'start' >> beam.Create(examples)
          | RunInference(handler_with_vars))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue((os.environ['FOO']) == 'bar')

  def test_bad_model_file_raises(self):
    model_state = self.tmpdir + os.sep + 'bad_file_name.json'

    with self.assertRaises(RuntimeError):
      with TestPipeline() as pipeline:
        examples = [
            datatable.Frame([[1, 1], [2, 2]]),
            datatable.Frame([[2, 4], [6, 8]]),
        ]

        pcoll = pipeline | 'start' >> beam.Create(examples)
        _ = pcoll | RunInference(
            XGBoostModelHandlerNumpy(xgboost.XGBClassifier, model_state))
        pipeline.run()

  def test_bad_input_type_raises(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    archived_model_state = self.tmpdir + os.sep + 'model.zip'

    zip_file = zipfile.ZipFile(archived_model_state, "w", zipfile.ZIP_DEFLATED)
    zip_file.write(model_state)
    zip_file.close()

    with self.assertRaises(xgboost.core.XGBoostError):
      model_handler = XGBoostModelHandlerNumpy(
          xgboost.XGBClassifier, model_state=archived_model_state)
      model_handler.load_model()

  def test_pipeline_scipy_with_keys(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    with TestPipeline() as pipeline:
      examples = [
          ('0', scipy.sparse.csr_matrix([[1, 1], [2, 2]])),
          ('1', scipy.sparse.csr_matrix([[2, 4], [6, 8]])),
      ]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          KeyedModelHandler(
              XGBoostModelHandlerSciPy(
                  model_class=xgboost.XGBClassifier, model_state=model_state)))
      expected = [
          ('0', PredictionResult(scipy.sparse.csr_matrix([[1, 1], [2, 2]]), 6)),
          (
              '1',
              PredictionResult(scipy.sparse.csr_matrix([[2, 4], [6, 8]]), 20))
      ]
      assert_that(
          actual,
          equal_to(expected, equals_fn=_compare_keyed_prediction_result))

  def test_pipeline_numpy_with_keys(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    with TestPipeline() as pipeline:
      examples = [
          ('0', numpy.array([[1, 1], [2, 2]])),
          ('1', numpy.array([[2, 4], [6, 8]])),
      ]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          KeyedModelHandler(
              XGBoostModelHandlerNumpy(
                  model_class=xgboost.XGBClassifier, model_state=model_state)))
      expected = [('0', PredictionResult(numpy.array([[1, 1], [2, 2]]), 6)),
                  ('1', PredictionResult(numpy.array([[2, 4], [6, 8]]), 20))]
      assert_that(
          actual,
          equal_to(expected, equals_fn=_compare_keyed_prediction_result))

  def test_pipeline_pandas_with_keys(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    with TestPipeline() as pipeline:
      examples = [
          ('0', pandas.DataFrame([[1, 1], [2, 2]])),
          ('1', pandas.DataFrame([[2, 4], [6, 8]])),
      ]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          KeyedModelHandler(
              XGBoostModelHandlerPandas(
                  model_class=xgboost.XGBClassifier, model_state=model_state)))
      expected = [
          ('0', PredictionResult(pandas.DataFrame([[1, 1], [2, 2]]), 6)),
          ('1', PredictionResult(pandas.DataFrame([[2, 4], [6, 8]]), 20))
      ]
      assert_that(
          actual,
          equal_to(expected, equals_fn=_compare_keyed_prediction_result))

  def test_pipeline_datatable_with_keys(self):
    model = build_monkeypatched_xgboost_classifier()
    model_state = self.tmpdir + os.sep + 'model.json'
    model.save_model(model_state)

    with TestPipeline() as pipeline:
      examples = [
          ('0', datatable.Frame([[1, 1], [2, 2]])),
          ('1', datatable.Frame([[2, 4], [6, 8]])),
      ]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          KeyedModelHandler(
              XGBoostModelHandlerDatatable(
                  model_class=xgboost.XGBClassifier, model_state=model_state)))
      expected = [
          ('0', PredictionResult(datatable.Frame([[1, 1], [2, 2]]), 6)),
          ('1', PredictionResult(datatable.Frame([[2, 4], [6, 8]]), 20))
      ]
      assert_that(
          actual,
          equal_to(expected, equals_fn=_compare_keyed_prediction_result))


if __name__ == '__main__':
  unittest.main()
