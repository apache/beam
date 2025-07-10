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

import shutil
import tempfile
import unittest
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import Union

import pytest

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.tensorflow_inference_test import FakeTFTensorModel
from apache_beam.ml.inference.tensorflow_inference_test import _compare_tensor_prediction_result

# pylint: disable=ungrouped-imports
try:
  import tensorflow as tf
  import torch
  from transformers import AutoModel
  from transformers import TFAutoModel
  from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerTensor
except ImportError:
  raise unittest.SkipTest('Transformers dependencies are not installed.')


def fake_inference_fn_tensor(
    batch: Sequence[Union[tf.Tensor, torch.Tensor]],
    model: Union[AutoModel, TFAutoModel],
    device,
    inference_args: dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  predictions = model.predict(batch, **inference_args)
  return utils._convert_to_result(batch, predictions, model_id)


class FakeTorchModel:
  def predict(self, input: torch.Tensor):
    return input


@pytest.mark.uses_transformers
class HuggingFaceInferenceTest(unittest.TestCase):
  def setUp(self) -> None:
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self) -> None:
    shutil.rmtree(self.tmpdir)

  def test_predict_tensor(self):
    fake_model = FakeTFTensorModel()
    inference_runner = HuggingFaceModelHandlerTensor(
        model_uri='unused',
        model_class=TFAutoModel,
        inference_fn=fake_inference_fn_tensor)
    batched_examples = [tf.constant([1]), tf.constant([10]), tf.constant([100])]
    expected_predictions = [
        PredictionResult(ex, pred) for ex, pred in zip(
            batched_examples,
            [tf.math.multiply(n, 10) for n in batched_examples])
    ]

    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_tensor_prediction_result(actual, expected))

  def test_predict_tensor_with_inference_args(self):
    fake_model = FakeTFTensorModel()
    inference_runner = HuggingFaceModelHandlerTensor(
        model_uri='unused',
        model_class=TFAutoModel,
        inference_fn=fake_inference_fn_tensor,
        inference_args={"add": True})
    batched_examples = [tf.constant([1]), tf.constant([10]), tf.constant([100])]
    expected_predictions = [
        PredictionResult(ex, pred) for ex, pred in zip(
            batched_examples, [
                tf.math.add(tf.math.multiply(n, 10), 10)
                for n in batched_examples
            ])
    ]

    inferences = inference_runner.run_inference(
        batched_examples, fake_model, inference_args={"add": True})

    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(_compare_tensor_prediction_result(actual, expected))

  def test_framework_detection_torch(self):
    fake_model = FakeTorchModel()
    inference_runner = HuggingFaceModelHandlerTensor(
        model_uri='unused',
        model_class=TFAutoModel,
        inference_fn=fake_inference_fn_tensor)
    batched_examples = [torch.tensor(1), torch.tensor(10), torch.tensor(100)]
    inference_runner.run_inference(batched_examples, fake_model)
    self.assertEqual(inference_runner._framework, "pt")

  def test_framework_detection_tensorflow(self):
    fake_model = FakeTFTensorModel()
    inference_runner = HuggingFaceModelHandlerTensor(
        model_uri='unused',
        model_class=TFAutoModel,
        inference_fn=fake_inference_fn_tensor,
        inference_args={"add": True})
    batched_examples = [tf.constant([1]), tf.constant([10]), tf.constant([100])]
    inference_runner.run_inference(
        batched_examples, fake_model, inference_args={"add": True})
    self.assertEqual(inference_runner._framework, "tf")


if __name__ == '__main__':
  unittest.main()
