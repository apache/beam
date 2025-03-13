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
from pathlib import Path

import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  import tensorflow as tf
  import tensorflow_hub as hub
  from apache_beam.examples.inference import tensorflow_imagenet_segmentation
  from apache_beam.examples.inference import tensorflow_mnist_classification
  from apache_beam.examples.inference import tensorflow_mnist_with_weights
except ImportError as e:
  tf = None


def process_outputs(filepath):
  with FileSystems().open(filepath) as f:
    lines = f.readlines()
  lines = [l.decode('utf-8').strip('\n') for l in lines]
  return lines


def rmdir(directory):
  directory = Path(directory)
  for item in directory.iterdir():
    if item.is_dir():
      rmdir(item)
    else:
      item.unlink()
  directory.rmdir()


def clear_tf_hub_temp_dir(model_path):
  # When loading from tensorflow hub using tfhub.resolve, the model is saved in
  # a temporary directory. That file can be persisted between test runs, in
  # which case tfhub.resolve will no-op. If the model is deleted and the file
  # isn't, tfhub.resolve will still no-op and tf.keras.models.load_model will
  # throw. To avoid this (and test more robustly) we delete the temporary
  # directory entirely between runs.
  local_path = hub.resolve(model_path)
  rmdir(local_path)


@unittest.skipIf(
    tf is None, 'Missing dependencies. '
    'Test depends on tensorflow')
@pytest.mark.uses_tf
@pytest.mark.it_postcommit
class TensorflowInference(unittest.TestCase):
  def test_tf_mnist_classification(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = 'gs://apache-beam-ml/testing/inputs/it_mnist_data.csv'
    output_file_dir = 'gs://apache-beam-ml/testing/outputs'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = 'gs://apache-beam-ml/models/tensorflow/mnist/'
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
    }
    tensorflow_mnist_classification.run(
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

  def test_tf_mnist_classification_large_model(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = 'gs://apache-beam-ml/testing/inputs/it_mnist_data.csv'
    output_file_dir = 'gs://apache-beam-ml/testing/outputs'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = 'gs://apache-beam-ml/models/tensorflow/mnist/'
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
        'large_model': True,
    }
    tensorflow_mnist_classification.run(
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

  def test_tf_imagenet_image_segmentation(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = (
        'gs://apache-beam-ml/testing/inputs/it_imagenet_input_labels.txt')
    image_dir = (
        'https://storage.googleapis.com/download.tensorflow.org/example_images/'
    )
    output_file_dir = 'gs://apache-beam-ml/testing/outputs'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = (
        'https://tfhub.dev/google/tf2-preview/mobilenet_v2/classification/4')
    clear_tf_hub_temp_dir(model_path)
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

    expected_output_filepath = 'gs://apache-beam-ml/testing/expected_outputs/test_tf_imagenet_image_segmentation.txt'  # pylint: disable=line-too-long
    expected_outputs = process_outputs(expected_output_filepath)
    predicted_outputs = process_outputs(output_file)
    self.assertEqual(len(expected_outputs), len(predicted_outputs))

    for true_label, predicted_label in zip(expected_outputs, predicted_outputs):
      self.assertEqual(true_label, predicted_label)

  def test_tf_mnist_with_weights_classification(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = 'gs://apache-beam-ml/testing/inputs/it_mnist_data.csv'
    output_file_dir = 'gs://apache-beam-ml/testing/outputs'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = 'gs://apache-beam-ml/models/tensorflow/mnist'
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
    }
    tensorflow_mnist_with_weights.run(
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


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
