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

import pytest
import unittest
import uuid

from apache_beam.examples.inference import xgboost_iris_classification
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline


def process_outputs(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return lines


@pytest.mark.uses_xgboost
@pytest.mark.it_postcommit
class XGBoostInference(unittest.TestCase):
  def test_iris_classification_numpy_single_batch(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_type = 'numpy'
    output_file_dir = '/tmp'
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'numpy_single_batch.txt'])
    model_state_path = '/tmp/model.json'
    extra_opts = {
        'input-type': input_type,
        'output': output_file,
        'model-state': model_state_path,
        'split': False
    }

    xgboost_iris_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = '/tmp/test_xgboost_iris_classification_numpy_single_batch_actual.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for predicted_output in predicted_outputs:
      true_label, prediction = predicted_output.split(',')
      predictions_dict[true_label] = prediction

    for expected_output in expected_outputs:
      true_label, expected_prediction = expected_output.split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)

  def test_iris_classification_pandas_single_batch(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_type = 'pandas'
    output_file_dir = '/tmp'
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'pandas_single_batch.txt'])
    model_state_path = '/tmp/model.json'
    extra_opts = {
        'input-type': input_type,
        'output': output_file,
        'model-state': model_state_path,
        'split': False
    }

    xgboost_iris_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = '/tmp/test_xgboost_iris_classification_pandas_single_batch_actual.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for predicted_output in predicted_outputs:
      true_label, prediction = predicted_output.split(',')
      predictions_dict[true_label] = prediction

    for expected_output in expected_outputs:
      true_label, expected_prediction = expected_output.split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)

  def test_iris_classification_scipy_single_batch(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_type = 'scipy'
    output_file_dir = '/tmp'
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'scipy_single_batch.txt'])
    model_state_path = '/tmp/model.json'
    extra_opts = {
        'input-type': input_type,
        'output': output_file,
        'model-state': model_state_path,
        'split': False
    }

    xgboost_iris_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = '/tmp/test_xgboost_iris_classification_scipy_single_batch_actual.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for predicted_output in predicted_outputs:
      true_label, prediction = predicted_output.split(',')
      predictions_dict[true_label] = prediction

    for expected_output in expected_outputs:
      true_label, expected_prediction = expected_output.split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)

  def test_iris_classification_datatable_single_batch(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_type = 'datatable'
    output_file_dir = '/tmp'
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'datatable_single_batch.txt'])
    model_state_path = '/tmp/model.json'
    extra_opts = {
        'input-type': input_type,
        'output': output_file,
        'model-state': model_state_path,
        'split': False
    }

    xgboost_iris_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = '/tmp/test_xgboost_iris_classification_datatable_single_batch_actual.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for predicted_output in predicted_outputs:
      true_label, prediction = predicted_output.split(',')
      predictions_dict[true_label] = prediction

    for expected_output in expected_outputs:
      true_label, expected_prediction = expected_output.split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)

  def test_iris_classification_numpy_multi_batch(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_type = 'numpy'
    output_file_dir = '/tmp'
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'numpy_multi_batch.txt'])
    model_state_path = '/tmp/model.json'
    extra_opts = {
        'input-type': input_type,
        'output': output_file,
        'model-state': model_state_path,
        'split': True
    }

    xgboost_iris_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = '/tmp/test_xgboost_iris_classification_numpy_multi_batch_actual.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for predicted_output in predicted_outputs:
      true_label, prediction = predicted_output.split(',')
      predictions_dict[true_label] = prediction

    for expected_output in expected_outputs:
      true_label, expected_prediction = expected_output.split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)

  def test_iris_classification_pandas_multi_batch(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_type = 'pandas'
    output_file_dir = '/tmp'
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'pandas_multi_batch.txt'])
    model_state_path = '/tmp/model.json'
    extra_opts = {
        'input-type': input_type,
        'output': output_file,
        'model-state': model_state_path,
        'split': True
    }

    xgboost_iris_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = '/tmp/test_xgboost_iris_classification_pandas_multi_batch_actual.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for predicted_output in predicted_outputs:
      true_label, prediction = predicted_output.split(',')
      predictions_dict[true_label] = prediction

    for expected_output in expected_outputs:
      true_label, expected_prediction = expected_output.split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)

  def test_iris_classification_scipy_multi_batch(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_type = 'scipy'
    output_file_dir = '/tmp'
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'scipy_multi_batch.txt'])
    model_state_path = '/tmp/model.json'
    extra_opts = {
        'input-type': input_type,
        'output': output_file,
        'model-state': model_state_path,
        'split': True
    }

    xgboost_iris_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = '/tmp/test_xgboost_iris_classification_scipy_multi_batch_actual.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for predicted_output in predicted_outputs:
      true_label, prediction = predicted_output.split(',')
      predictions_dict[true_label] = prediction

    for expected_output in expected_outputs:
      true_label, expected_prediction = expected_output.split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)

  def test_iris_classification_datatable_multi_batch(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_type = 'datatable'
    output_file_dir = '/tmp'
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'datatable_multi_batch.txt'])
    model_state_path = '/tmp/model.json'
    extra_opts = {
        'input-type': input_type,
        'output': output_file,
        'model-state': model_state_path,
        'split': True
    }

    xgboost_iris_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = '/tmp/test_xgboost_iris_classification_datatable_multi_batch_actual.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for predicted_output in predicted_outputs:
      true_label, prediction = predicted_output.split(',')
      predictions_dict[true_label] = prediction

    for expected_output in expected_outputs:
      true_label, expected_prediction = expected_output.split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
