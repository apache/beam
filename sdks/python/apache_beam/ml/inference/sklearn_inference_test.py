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
import io
import math
import os
import pickle
import platform
import shutil
import sys
import tempfile
import unittest

import joblib
import numpy
import pandas
from sklearn import linear_model
from sklearn import svm
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.sklearn_inference import ModelFileType
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerPandas
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def _compare_prediction_result(a, b):
  example_equal = numpy.array_equal(a.example, b.example)
  return a.inference == b.inference and example_equal


def _compare_dataframe_predictions(a_in, b_in):
  keys_equal = True
  if isinstance(a_in, tuple) and not isinstance(a_in, PredictionResult):
    a_key, a = a_in
    b_key, b = b_in
    keys_equal = a_key == b_key
  else:
    a = a_in
    b = b_in
  example_equal = pandas.DataFrame.equals(a.example, b.example)
  inference_equal = math.floor(a.inference) == math.floor(b.inference)
  return inference_equal and example_equal and keys_equal


class FakeModel:
  def __init__(self):
    self.total_predict_calls = 0

  def predict(self, input_vector: numpy.ndarray):
    self.total_predict_calls += 1
    return numpy.sum(input_vector, axis=1)


def build_model():
  x = [[0, 0], [1, 1]]
  y = [0, 1]
  model = svm.SVC()
  model.fit(x, y)
  return model


def pandas_dataframe():
  csv_string = (
      'category_1,number_1,category_2,number_2,label,number_3\n'
      'red,4,frog,5,6,7\n'
      'blue,3,horse,8,9,10\n'
      'red,0,cow,1,2,3\n'
      'blue,4,frog,1,1,1\n'
      'red,1,horse,4,2,3')
  csv_string_io = io.StringIO(csv_string)
  return pandas.read_csv(csv_string_io)


def build_pandas_pipeline():
  """Builds a common type of pandas pipeline with preprocessing."""
  categorical_columns = ['category_1', 'category_2']
  numerical_columns = ['number_1', 'number_2', 'number_3']

  categorical_transformer = OneHotEncoder(handle_unknown='ignore')
  numerical_transformer = StandardScaler()

  preprocessor = ColumnTransformer(
      transformers=[
          ("numerical", numerical_transformer, numerical_columns),
          ("categorical", categorical_transformer, categorical_columns),
      ])
  pipeline = Pipeline(
      steps=[("preprocessor",
              preprocessor), ("classifier", linear_model.SGDRegressor())])
  data = pandas_dataframe()
  labels = data['label']
  pipeline.fit(data, labels)
  return pipeline


def convert_inference_to_floor(prediction_result):
  return math.floor(prediction_result.inference)


class SkLearnRunInferenceTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_predict_output(self):
    fake_model = FakeModel()
    inference_runner = SklearnModelHandlerNumpy(model_uri='unused')
    batched_examples = [
        numpy.array([1, 2, 3]), numpy.array([4, 5, 6]), numpy.array([7, 8, 9])
    ]
    expected_predictions = [
        PredictionResult(numpy.array([1, 2, 3]), 6),
        PredictionResult(numpy.array([4, 5, 6]), 15),
        PredictionResult(numpy.array([7, 8, 9]), 24)
    ]
    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_data_vectorized(self):
    fake_model = FakeModel()
    inference_runner = SklearnModelHandlerNumpy(model_uri='unused')
    batched_examples = [
        numpy.array([1, 2, 3]), numpy.array([4, 5, 6]), numpy.array([7, 8, 9])
    ]
    # even though there are 3 examples, the data should
    # be vectorized and only 1 call should happen.
    inference_runner.run_inference(batched_examples, fake_model)
    self.assertEqual(1, fake_model.total_predict_calls)

  def test_num_bytes_numpy(self):
    inference_runner = SklearnModelHandlerNumpy(model_uri='unused')
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

  @unittest.skipIf(
      platform.system() == 'Windows',
      'https://github.com/apache/beam/issues/21449')
  def test_pipeline_pickled(self):
    temp_file_name = self.tmpdir + os.sep + 'pickled_file'
    with open(temp_file_name, 'wb') as file:
      pickle.dump(build_model(), file)
    with TestPipeline() as pipeline:
      examples = [numpy.array([0, 0]), numpy.array([1, 1])]

      pcoll = pipeline | 'start' >> beam.Create(examples)
      actual = pcoll | RunInference(
          SklearnModelHandlerNumpy(model_uri=temp_file_name))
      expected = [
          PredictionResult(numpy.array([0, 0]), 0),
          PredictionResult(numpy.array([1, 1]), 1)
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_prediction_result))

  @unittest.skipIf(
      platform.system() == 'Windows',
      'https://github.com/apache/beam/issues/21449')
  def test_pipeline_joblib(self):
    temp_file_name = self.tmpdir + os.sep + 'joblib_file'
    with open(temp_file_name, 'wb') as file:
      joblib.dump(build_model(), file)
    with TestPipeline() as pipeline:
      examples = [numpy.array([0, 0]), numpy.array([1, 1])]

      pcoll = pipeline | 'start' >> beam.Create(examples)

      actual = pcoll | RunInference(
          SklearnModelHandlerNumpy(
              model_uri=temp_file_name, model_file_type=ModelFileType.JOBLIB))
      expected = [
          PredictionResult(numpy.array([0, 0]), 0),
          PredictionResult(numpy.array([1, 1]), 1)
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_prediction_result))

  def test_bad_file_raises(self):
    with self.assertRaises(RuntimeError):
      with TestPipeline() as pipeline:
        examples = [numpy.array([0, 0])]
        pcoll = pipeline | 'start' >> beam.Create(examples)
        _ = pcoll | RunInference(
            SklearnModelHandlerNumpy(model_uri='/var/bad_file_name'))
        pipeline.run()

  @unittest.skipIf(
      platform.system() == 'Windows',
      'https://github.com/apache/beam/issues/21449')
  def test_bad_input_type_raises(self):
    with self.assertRaisesRegex(AssertionError,
                                'Unsupported serialization type'):
      with tempfile.NamedTemporaryFile() as file:
        model_handler = SklearnModelHandlerNumpy(
            model_uri=file.name, model_file_type=None)
        model_handler.load_model()

  @unittest.skipIf(
      platform.system() == 'Windows',
      'https://github.com/apache/beam/issues/21449')
  def test_pipeline_pandas(self):
    temp_file_name = self.tmpdir + os.sep + 'pickled_file'
    with open(temp_file_name, 'wb') as file:
      pickle.dump(build_pandas_pipeline(), file)
    with TestPipeline() as pipeline:
      dataframe = pandas_dataframe()
      splits = [dataframe.loc[[i]] for i in dataframe.index]
      pcoll = pipeline | 'start' >> beam.Create(splits)
      actual = pcoll | RunInference(
          SklearnModelHandlerPandas(model_uri=temp_file_name))

      expected = [
          PredictionResult(splits[0], 5),
          PredictionResult(splits[1], 8),
          PredictionResult(splits[2], 1),
          PredictionResult(splits[3], 1),
          PredictionResult(splits[4], 2),
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_dataframe_predictions))

  @unittest.skipIf(platform.system() == 'Windows', 'BEAM-14359')
  def test_pipeline_pandas_joblib(self):
    temp_file_name = self.tmpdir + os.sep + 'pickled_file'
    with open(temp_file_name, 'wb') as file:
      joblib.dump(build_pandas_pipeline(), file)
    with TestPipeline() as pipeline:
      dataframe = pandas_dataframe()
      splits = [dataframe.loc[[i]] for i in dataframe.index]
      pcoll = pipeline | 'start' >> beam.Create(splits)
      actual = pcoll | RunInference(
          SklearnModelHandlerPandas(
              model_uri=temp_file_name, model_file_type=ModelFileType.JOBLIB))

      expected = [
          PredictionResult(splits[0], 5),
          PredictionResult(splits[1], 8),
          PredictionResult(splits[2], 1),
          PredictionResult(splits[3], 1),
          PredictionResult(splits[4], 2),
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_dataframe_predictions))

  @unittest.skipIf(
      platform.system() == 'Windows',
      'https://github.com/apache/beam/issues/21449')
  def test_pipeline_pandas_with_keys(self):
    temp_file_name = self.tmpdir + os.sep + 'pickled_file'
    with open(temp_file_name, 'wb') as file:
      pickle.dump(build_pandas_pipeline(), file)
    with TestPipeline() as pipeline:
      data_frame = pandas_dataframe()
      keys = [str(i) for i in range(5)]
      splits = [data_frame.loc[[i]] for i in data_frame.index]
      keyed_rows = [(key, value) for key, value in zip(keys, splits)]

      pcoll = pipeline | 'start' >> beam.Create(keyed_rows)
      actual = pcoll | RunInference(
          KeyedModelHandler(
              SklearnModelHandlerPandas(model_uri=temp_file_name)))
      expected = [
          ('0', PredictionResult(splits[0], 5)),
          ('1', PredictionResult(splits[1], 8)),
          ('2', PredictionResult(splits[2], 1)),
          ('3', PredictionResult(splits[3], 1)),
          ('4', PredictionResult(splits[4], 2)),
      ]
      assert_that(
          actual, equal_to(expected, equals_fn=_compare_dataframe_predictions))

  def test_infer_too_many_rows_in_dataframe(self):
    with self.assertRaisesRegex(
        ValueError, r'Only dataframes with single rows are supported'):
      data_frame_too_many_rows = pandas_dataframe()
      fake_model = FakeModel()
      inference_runner = SklearnModelHandlerPandas(model_uri='unused')
      inference_runner.run_inference([data_frame_too_many_rows], fake_model)

  def test_inference_args_passed(self):
    with self.assertRaisesRegex(ValueError, r'inference_args were provided'):
      data_frame = pandas_dataframe()
      fake_model = FakeModel()
      inference_runner = SklearnModelHandlerPandas(model_uri='unused')
      inference_runner.run_inference([data_frame],
                                     fake_model,
                                     inference_args={'key1': 'value1'})


if __name__ == '__main__':
  unittest.main()
