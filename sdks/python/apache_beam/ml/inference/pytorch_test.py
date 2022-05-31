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
import shutil
import tempfile
import unittest
from collections import OrderedDict

import numpy as np
import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where pytorch library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import torch
  from apache_beam.ml.inference.api import PredictionResult
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference.pytorch import PytorchInferenceRunner
  from apache_beam.ml.inference.pytorch import PytorchModelLoader
except ImportError:
  raise unittest.SkipTest('PyTorch dependencies are not installed')


def _compare_prediction_result(a, b):
  return (
      torch.equal(a.inference, b.inference) and
      torch.equal(a.example, b.example))


class PytorchLinearRegression(torch.nn.Module):
  def __init__(self, input_dim, output_dim):
    super().__init__()
    self.linear = torch.nn.Linear(input_dim, output_dim)

  def forward(self, x):
    out = self.linear(x)
    return out


@pytest.mark.uses_pytorch
class PytorchRunInferenceTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_inference_runner_single_tensor_feature(self):
    examples = [
        torch.from_numpy(np.array([1], dtype="float32")),
        torch.from_numpy(np.array([5], dtype="float32")),
        torch.from_numpy(np.array([-3], dtype="float32")),
        torch.from_numpy(np.array([10.0], dtype="float32")),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex,
        pred in zip(
            examples,
            torch.Tensor([example * 2.0 + 0.5
                          for example in examples]).reshape(-1, 1))
    ]

    model = PytorchLinearRegression(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = PytorchInferenceRunner(torch.device('cpu'))
    predictions = inference_runner.run_inference(examples, model)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_inference_runner_multiple_tensor_features(self):
    examples = torch.from_numpy(
        np.array([1, 5, 3, 10, -14, 0, 0.5, 0.5],
                 dtype="float32")).reshape(-1, 2)
    examples = [
        torch.from_numpy(np.array([1, 5], dtype="float32")),
        torch.from_numpy(np.array([3, 10], dtype="float32")),
        torch.from_numpy(np.array([-14, 0], dtype="float32")),
        torch.from_numpy(np.array([0.5, 0.5], dtype="float32")),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex,
        pred in zip(
            examples,
            torch.Tensor([f1 * 2.0 + f2 * 3 + 0.5
                          for f1, f2 in examples]).reshape(-1, 1))
    ]

    model = PytorchLinearRegression(input_dim=2, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = PytorchInferenceRunner(torch.device('cpu'))
    predictions = inference_runner.run_inference(examples, model)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_num_bytes(self):
    inference_runner = PytorchInferenceRunner(torch.device('cpu'))
    examples = torch.from_numpy(
        np.array([1, 5, 3, 10, -14, 0, 0.5, 0.5],
                 dtype="float32")).reshape(-1, 2)
    self.assertEqual((examples[0].element_size()) * 8,
                     inference_runner.get_num_bytes(examples))

  def test_namespace(self):
    inference_runner = PytorchInferenceRunner(torch.device('cpu'))
    self.assertEqual(
        'RunInferencePytorch', inference_runner.get_metrics_namespace())

  def test_pipeline_local_model(self):
    with TestPipeline() as pipeline:
      examples = torch.from_numpy(
          np.array([1, 5, 3, 10, -14, 0, 0.5, 0.5],
                   dtype="float32")).reshape(-1, 2)
      expected_predictions = [
          PredictionResult(ex, pred) for ex,
          pred in zip(
              examples,
              torch.Tensor([f1 * 2.0 + f2 * 3 + 0.5
                            for f1, f2 in examples]).reshape(-1, 1))
      ]

      state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      path = os.path.join(self.tmpdir, 'my_state_dict_path')
      torch.save(state_dict, path)

      model_loader = PytorchModelLoader(
          state_dict_path=path,
          model_class=PytorchLinearRegression,
          model_params={
              'input_dim': 2, 'output_dim': 1
          })

      pcoll = pipeline | 'start' >> beam.Create(examples)
      predictions = pcoll | RunInference(model_loader)
      assert_that(
          predictions,
          equal_to(expected_predictions, equals_fn=_compare_prediction_result))

  def test_pipeline_gcs_model(self):
    with TestPipeline() as pipeline:
      examples = torch.from_numpy(
          np.array([1, 5, 3, 10], dtype="float32").reshape(-1, 1))
      expected_predictions = [
          PredictionResult(ex, pred) for ex,
          pred in zip(
              examples,
              torch.Tensor([example * 2.0 + 0.5
                            for example in examples]).reshape(-1, 1))
      ]

      gs_pth = 'gs://apache-beam-ml/pytorch_lin_reg_model_2x+0.5_state_dict.pth'
      model_loader = PytorchModelLoader(
          state_dict_path=gs_pth,
          model_class=PytorchLinearRegression,
          model_params={
              'input_dim': 1, 'output_dim': 1
          })

      pcoll = pipeline | 'start' >> beam.Create(examples)
      predictions = pcoll | RunInference(model_loader)
      assert_that(
          predictions,
          equal_to(expected_predictions, equals_fn=_compare_prediction_result))

  def test_invalid_input_type(self):
    with self.assertRaisesRegex(TypeError, "expected Tensor as element"):
      with TestPipeline() as pipeline:
        examples = np.array([1, 5, 3, 10], dtype="float32").reshape(-1, 1)

        state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                  ('linear.bias', torch.Tensor([0.5]))])
        path = os.path.join(self.tmpdir, 'my_state_dict_path')
        torch.save(state_dict, path)

        model_loader = PytorchModelLoader(
            state_dict_path=path,
            model_class=PytorchLinearRegression,
            model_params={
                'input_dim': 1, 'output_dim': 1
            })

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_loader)


if __name__ == '__main__':
  unittest.main()
