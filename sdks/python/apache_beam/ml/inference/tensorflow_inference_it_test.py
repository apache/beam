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

"""End-to-End test for Tensorflow Inference"""

import logging
import unittest
import uuid

import pytest

from apache_beam.examples.inference import tensorflow_imagenet_segmentation
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  import tensorflow as tf
  from apache_beam.examples.inference import tensorflow_mnist_classification
except ImportError as e:
  tf = None


def process_outputs(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return lines


@unittest.skipIf(
    tf is None, 'Missing dependencies. '
    'Test depends on tensorflow')
@pytest.mark.uses_tf
@pytest.mark.it_postcommit
class TensorflowInference(unittest.TestCase):
  def test_tf_mnist_classification(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = 'gs://clouddfe-riteshghorse/tf/mnist/dataset/testing_inputs_it_mnist_data.csv'  # pylint: disable=line-too-long
    output_file_dir = 'gs://clouddfe-riteshghorse/tf/mnist/output/'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = 'gs://clouddfe-riteshghorse/tf/mnist/model/'
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
    }
    tensorflow_mnist_classification.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = 'gs://clouddfe-riteshghorse/tf/mnist/output/testing_expected_outputs_test_sklearn_mnist_classification_actuals.txt'  # pylint: disable=line-too-long
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

  def test_tf_imagenet_image_classification(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = 'gs://clouddfe-riteshghorse/tf/imagenet/input/input_labels.txt'  # pylint: disable=line-too-long
    image_dir = 'https://storage.googleapis.com/download.tensorflow.org/example_images/'  # pylint: disable=line-too-long
    output_file_dir = 'gs://clouddfe-riteshghorse/tf/imagenet/output'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = 'https://tfhub.dev/google/tf2-preview/mobilenet_v2/classification/4'  # pylint: disable=line-too-long
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
        'image_dir': image_dir
    }
    tensorflow_imagenet_segmentation.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)

    expected_output_filepath = 'gs://clouddfe-riteshghorse/tf/imagenet/output/actuals.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)

    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    for true_label, predicted_label in zip(expected_outputs, predicted_outputs):
      self.assertEqual(true_label, predicted_label)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
