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

import unittest

import numpy
import pytest

try:
  import tensorflow as tf
  from apache_beam.ml.inference.sklearn_inference_test import _compare_prediction_result
  from apache_beam.ml.inference.base import KeyedModelHandler, PredictionResult
  from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerNumpy, TFModelHandlerTensor
except ImportError:
  raise unittest.SkipTest('Tensorflow dependencies are not installed')


class FakeTFNumpyModel:
  def predict(self, input: numpy.ndarray):
    return numpy.multiply(input, 10)


class FakeTFTensorModel:
  def predict(self, input: tf.Tensor, add=False):
    if add:
      return tf.math.add(tf.math.multiply(input, 10), 10)
    return tf.math.multiply(input, 10)


def _compare_tensor_prediction_result(x, y):
  return tf.math.equal(x.inference, y.inference)


@pytest.mark.uses_tf
class TFRunInferenceTest(unittest.TestCase):
  def test_predict_numpy(self):
    fake_model = FakeTFNumpyModel()
    inference_runner = TFModelHandlerNumpy(model_uri='unused')
    batched_examples = [numpy.array([1]), numpy.array([10]), numpy.array([100])]
    expected_predictions = [
        PredictionResult(numpy.array([1]), 10),
        PredictionResult(numpy.array([10]), 100),
        PredictionResult(numpy.array([100]), 1000)
    ]
    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_predict_tensor(self):
    fake_model = FakeTFTensorModel()
    inference_runner = TFModelHandlerTensor(model_uri='unused')
    batched_examples = [
        tf.convert_to_tensor(numpy.array([1])),
        tf.convert_to_tensor(numpy.array([10])),
        tf.convert_to_tensor(numpy.array([100])),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex,
        pred in zip(
            batched_examples,
            [tf.math.multiply(n, 10) for n in batched_examples])
    ]

    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_tensor_prediction_result(actual, expected))

  def test_predict_tensor_with_args(self):
    fake_model = FakeTFTensorModel()
    inference_runner = TFModelHandlerTensor(model_uri='unused')
    batched_examples = [
        tf.convert_to_tensor(numpy.array([1])),
        tf.convert_to_tensor(numpy.array([10])),
        tf.convert_to_tensor(numpy.array([100])),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex,
        pred in zip(
            batched_examples, [
                tf.math.add(tf.math.multiply(n, 10), 10)
                for n in batched_examples
            ])
    ]

    inferences = inference_runner.run_inference(
        batched_examples, fake_model, inference_args={"add": True})
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_tensor_prediction_result(actual, expected))

  def test_predict_keyed_numpy(self):
    fake_model = FakeTFNumpyModel()
    inference_runner = KeyedModelHandler(
        TFModelHandlerNumpy(model_uri='unused'))
    batched_examples = [
        ('k1', numpy.array([1], dtype=numpy.int64)),
        ('k2', numpy.array([10], dtype=numpy.int64)),
        ('k3', numpy.array([100], dtype=numpy.int64)),
    ]
    expected_predictions = [
        (ex[0], PredictionResult(ex[1], pred)) for ex,
        pred in zip(
            batched_examples,
            [numpy.multiply(n[1], 10) for n in batched_examples])
    ]
    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_prediction_result(actual[1], expected[1]))

  def test_predict_keyed_tensor(self):
    fake_model = FakeTFTensorModel()
    inference_runner = KeyedModelHandler(
        TFModelHandlerTensor(model_uri='unused'))
    batched_examples = [
        ('k1', tf.convert_to_tensor(numpy.array([1]))),
        ('k2', tf.convert_to_tensor(numpy.array([10]))),
        ('k3', tf.convert_to_tensor(numpy.array([100]))),
    ]
    expected_predictions = [
        (ex[0], PredictionResult(ex[1], pred)) for ex,
        pred in zip(
            batched_examples,
            [tf.math.multiply(n[1], 10) for n in batched_examples])
    ]
    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_tensor_prediction_result(actual[1], expected[1]))


if __name__ == '__main__':
  unittest.main()
