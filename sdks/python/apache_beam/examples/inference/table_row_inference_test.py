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

"""Unit tests for table row inference pipeline."""

import json
import os
import pickle
import tempfile
import unittest

import apache_beam as beam
import numpy as np
from apache_beam.examples.inference.table_row_inference import FormatTableOutput
from apache_beam.examples.inference.table_row_inference import TableRowModelHandler
from apache_beam.examples.inference.table_row_inference import build_output_schema
from apache_beam.examples.inference.table_row_inference import (
    parse_json_to_table_row)
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

# Module-level matcher for assert_that (must be picklable; no closure over
# self).
REQUIRED_OUTPUT_KEYS = (
    'row_key',
    'prediction',
    'input_feature1',
    'input_feature2',
    'input_feature3')


def _assert_table_inference_outputs(outputs):
  """Asserts pipeline output has expected structure. Used in assert_that."""
  if len(outputs) != 2:
    raise AssertionError(f'Expected 2 outputs, got {len(outputs)}')
  for output in outputs:
    for key in REQUIRED_OUTPUT_KEYS:
      if key not in output:
        raise AssertionError(f'Missing key {key!r} in output {output}')


try:
  from sklearn.linear_model import LinearRegression
  SKLEARN_AVAILABLE = True
except ImportError:
  SKLEARN_AVAILABLE = False


class SimpleLinearModel:
  """Simple model for testing without sklearn dependency."""
  def predict(self, X):
    return np.sum(X, axis=1)


@unittest.skipIf(not SKLEARN_AVAILABLE, 'sklearn is not available')
class TableRowInferenceTest(unittest.TestCase):
  def setUp(self):
    self.tmp_dir = tempfile.mkdtemp()
    self.model_path = os.path.join(self.tmp_dir, 'test_model.pkl')

    model = LinearRegression()
    X_train = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    y_train = np.array([6, 15, 24])
    model.fit(X_train, y_train)

    with open(self.model_path, 'wb') as f:
      pickle.dump(model, f)

  def test_parse_json_to_table_row(self):
    json_data = json.dumps({
        'id': 'test_1', 'feature1': 1.0, 'feature2': 2.0, 'feature3': 3.0
    }).encode('utf-8')

    key, row = parse_json_to_table_row(
        json_data, schema_fields=['feature1', 'feature2', 'feature3'])

    self.assertEqual(key, 'test_1')
    self.assertEqual(row.feature1, 1.0)
    self.assertEqual(row.feature2, 2.0)
    self.assertEqual(row.feature3, 3.0)

  def test_build_output_schema(self):
    feature_cols = ['feature1', 'feature2', 'feature3']
    schema = build_output_schema(feature_cols)

    expected_fields = [
        'row_key',
        'prediction',
        'model_id',
        'input_feature1',
        'input_feature2',
        'input_feature3'
    ]

    for field in expected_fields:
      self.assertIn(field, schema)

  def test_table_row_model_handler(self):
    model_handler = TableRowModelHandler(
        model_uri=self.model_path, feature_columns=['f1', 'f2', 'f3'])

    model = model_handler.load_model()

    test_rows = [
        beam.Row(f1=1.0, f2=2.0, f3=3.0),
        beam.Row(f1=4.0, f2=5.0, f3=6.0),
    ]

    results = list(model_handler.run_inference(test_rows, model))

    self.assertEqual(len(results), 2)
    self.assertIsInstance(results[0], PredictionResult)
    self.assertEqual(results[0].example, test_rows[0])
    self.assertIsNotNone(results[0].inference)

  def test_format_table_output(self):
    row = beam.Row(feature1=1.0, feature2=2.0, feature3=3.0)
    prediction_result = PredictionResult(
        example=row, inference=6.0, model_id='test_model')

    keyed_result = ('test_key', prediction_result)

    feature_columns = ['feature1', 'feature2', 'feature3']
    formatter = FormatTableOutput(feature_columns=feature_columns)
    outputs = list(formatter.process(keyed_result))

    self.assertEqual(len(outputs), 1)
    output = outputs[0]

    self.assertEqual(output['row_key'], 'test_key')
    self.assertEqual(output['prediction'], 6.0)
    self.assertEqual(output['model_id'], 'test_model')
    self.assertEqual(output['input_feature1'], 1.0)
    self.assertEqual(output['input_feature2'], 2.0)
    self.assertEqual(output['input_feature3'], 3.0)

  def test_pipeline_integration(self):
    test_data = [
        json.dumps({
            'id': 'row_1', 'feature1': 1.0, 'feature2': 2.0, 'feature3': 3.0
        }),
        json.dumps({
            'id': 'row_2', 'feature1': 4.0, 'feature2': 5.0, 'feature3': 6.0
        }),
    ]

    feature_columns = ['feature1', 'feature2', 'feature3']
    model_handler = TableRowModelHandler(
        model_uri=self.model_path, feature_columns=feature_columns)

    with TestPipeline() as p:
      input_data = (
          p
          | beam.Create(test_data)
          | beam.Map(
              lambda line: parse_json_to_table_row(
                  line.encode('utf-8'), feature_columns)))

      predictions = (
          input_data
          | RunInference(KeyedModelHandler(model_handler))
          | beam.ParDo(FormatTableOutput(feature_columns=feature_columns)))

      assert_that(predictions, _assert_table_inference_outputs)


class TableRowInferenceNoSklearnTest(unittest.TestCase):
  """Tests that don't require sklearn."""
  def test_parse_json_without_schema(self):
    json_data = json.dumps({'id': 'test', 'value': 123}).encode('utf-8')

    key, row = parse_json_to_table_row(json_data)

    self.assertEqual(key, 'test')
    self.assertTrue(hasattr(row, 'value'))


if __name__ == '__main__':
  unittest.main()
