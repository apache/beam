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

"""End-to-End test for Sklearn Inference"""

import logging
import re
import unittest
import uuid

import pytest

from apache_beam.examples.inference import sklearn_japanese_housing_regression
from apache_beam.examples.inference import sklearn_mnist_classification
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports, unused-import
try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  raise unittest.SkipTest('GCP dependencies are not installed')


def process_outputs(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return lines


def file_lines_sorted(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return sorted(lines)


@pytest.mark.uses_sklearn
@pytest.mark.it_postcommit
class SklearnInference(unittest.TestCase):
  def test_sklearn_mnist_classification(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = 'gs://apache-beam-ml/testing/inputs/it_mnist_data.csv'
    output_file_dir = 'gs://temp-storage-for-end-to-end-tests'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = 'gs://apache-beam-ml/models/mnist_model_svm.pickle'
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
    }
    sklearn_mnist_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = 'gs://apache-beam-ml/testing/expected_outputs/test_sklearn_mnist_classification_actuals.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    predictions_dict = {}
    for i in range(len(predicted_outputs)):
      true_label, prediction = predicted_outputs[i].split(',')
      predictions_dict[true_label] = prediction

    for i in range(len(expected_outputs)):
      true_label, expected_prediction = expected_outputs[i].split(',')
      self.assertEqual(predictions_dict[true_label], expected_prediction)

  def test_sklearn_regression(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = 'gs://apache-beam-ml/testing/inputs/japanese_housing_test_data.csv'  # pylint: disable=line-too-long
    output_file_dir = 'gs://temp-storage-for-end-to-end-tests'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = 'gs://apache-beam-ml/models/japanese_housing/'
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
    }
    sklearn_japanese_housing_regression.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = 'gs://apache-beam-ml/testing/expected_outputs/japanese_housing_subset.txt'  # pylint: disable=line-too-long
    expected_outputs = file_lines_sorted(expected_output_filepath)
    actual_outputs = file_lines_sorted(output_file)
    self.assertEqual(len(expected_outputs), len(actual_outputs))

    for expected, actual in zip(expected_outputs, actual_outputs):
      expected_true, expected_predict = re.findall(r'\d+', expected)
      actual_true, actual_predict = re.findall(r'\d+', actual)
      # actual_true is the y value from the input csv file.
      # Therefore it should be an exact match to expected_true.
      self.assertEqual(actual_true, expected_true)
      # predictions might not be exactly equal due to differences between
      # environments. This code validates they are within 10 percent.
      percent_diff = abs(float(expected_predict) - float(actual_predict)
                         ) / float(expected_predict) * 100.0
      self.assertLess(percent_diff, 10)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
