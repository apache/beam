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
  from apache_beam.ml.inference.base import PredictionResult
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference import pytorch_inference
  from apache_beam.ml.inference.pytorch_inference import default_keyed_tensor_inference_fn
  from apache_beam.ml.inference.pytorch_inference import default_tensor_inference_fn
  from apache_beam.ml.inference.pytorch_inference import make_keyed_tensor_model_fn
  from apache_beam.ml.inference.pytorch_inference import make_tensor_model_fn
  from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
  from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
except ImportError:
  raise unittest.SkipTest('PyTorch dependencies are not installed')

try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  GCSFileSystem = None  # type: ignore

TWO_FEATURES_EXAMPLES = [
    torch.from_numpy(np.array([1, 5], dtype="float32")),
    torch.from_numpy(np.array([3, 10], dtype="float32")),
    torch.from_numpy(np.array([-14, 0], dtype="float32")),
    torch.from_numpy(np.array([0.5, 0.5], dtype="float32")),
]

TWO_FEATURES_PREDICTIONS = [
    PredictionResult(ex, pred) for ex, pred in zip(
        TWO_FEATURES_EXAMPLES, torch.Tensor(
            [f1 * 2.0 + f2 * 3 + 0.5
             for f1, f2 in TWO_FEATURES_EXAMPLES]).reshape(-1, 1))
]

TWO_FEATURES_DICT_OUT_PREDICTIONS = [
    PredictionResult(
        p.example, {
            "output1": p.inference, "output2": p.inference
        }) for p in TWO_FEATURES_PREDICTIONS
]

KEYED_TORCH_EXAMPLES = [
    {
        'k1': torch.from_numpy(np.array([1], dtype="float32")),
        'k2': torch.from_numpy(np.array([1.5], dtype="float32"))
    },
    {
        'k1': torch.from_numpy(np.array([5], dtype="float32")),
        'k2': torch.from_numpy(np.array([5.5], dtype="float32"))
    },
    {
        'k1': torch.from_numpy(np.array([-3], dtype="float32")),
        'k2': torch.from_numpy(np.array([-3.5], dtype="float32"))
    },
    {
        'k1': torch.from_numpy(np.array([10.0], dtype="float32")),
        'k2': torch.from_numpy(np.array([10.5], dtype="float32"))
    },
]

KEYED_TORCH_PREDICTIONS = [
    PredictionResult(ex, pred) for ex, pred in zip(
        KEYED_TORCH_EXAMPLES, torch.Tensor(
            [(example['k1'] * 2.0 + 0.5) + (example['k2'] * 2.0 + 0.5)
             for example in KEYED_TORCH_EXAMPLES]).reshape(-1, 1))
]

KEYED_TORCH_HELPER_PREDICTIONS = [
    PredictionResult(ex, pred) for ex, pred in zip(
        KEYED_TORCH_EXAMPLES, torch.Tensor(
            [(example['k1'] * 2.0 + 0.5) + (example['k2'] * 2.0 + 0.5) + 0.5
             for example in KEYED_TORCH_EXAMPLES]).reshape(-1, 1))
]

KEYED_TORCH_DICT_OUT_PREDICTIONS = [
    PredictionResult(
        p.example, {
            "output1": p.inference, "output2": p.inference
        }) for p in KEYED_TORCH_PREDICTIONS
]


class TestPytorchModelHandlerForInferenceOnly(PytorchModelHandlerTensor):
  def __init__(self, device, *, inference_fn=default_tensor_inference_fn):
    self._device = device
    self._inference_fn = inference_fn
    self._state_dict_path = None
    self._torch_script_model_path = None


class TestPytorchModelHandlerKeyedTensorForInferenceOnly(
    PytorchModelHandlerKeyedTensor):
  def __init__(self, device, *, inference_fn=default_keyed_tensor_inference_fn):
    self._device = device
    self._inference_fn = inference_fn
    self._state_dict_path = None
    self._torch_script_model_path = None


def _compare_prediction_result(x, y):
  if isinstance(x.example, dict):
    example_equals = all(
        torch.equal(x, y)
        for x, y in zip(x.example.values(), y.example.values()))
  else:
    example_equals = torch.equal(x.example, y.example)
  if not example_equals:
    return False

  if isinstance(x.inference, dict):
    return all(
        torch.equal(x, y)
        for x, y in zip(x.inference.values(), y.inference.values()))

  return torch.equal(x.inference, y.inference)


def custom_tensor_inference_fn(
    batch, model, device, inference_args, model_id=None):
  predictions = [
      PredictionResult(ex, pred) for ex, pred in zip(
          batch, torch.Tensor([item * 2.0 + 1.5
                               for item in batch]).reshape(-1, 1))
  ]
  return predictions


class PytorchLinearRegression(torch.nn.Module):
  def __init__(self, input_dim, output_dim):
    super().__init__()
    self.linear = torch.nn.Linear(input_dim, output_dim)

  def forward(self, x):
    out = self.linear(x)
    return out

  def generate(self, x):
    out = self.linear(x) + 0.5
    return out


class PytorchLinearRegressionDict(torch.nn.Module):
  def __init__(self, input_dim, output_dim):
    super().__init__()
    self.linear = torch.nn.Linear(input_dim, output_dim)

  def forward(self, x):
    out = self.linear(x)
    return {'output1': out, 'output2': out}


class PytorchLinearRegressionKeyedBatchAndExtraInferenceArgs(torch.nn.Module):
  """
  A linear model with batched keyed inputs and non-batchable extra args.

  Note: k1 and k2 are batchable examples passed in as a dict from str to tensor.
  prediction_param_array, prediction_param_bool are non-batchable extra args
  (typically model-related info) used to configure the model before its predict
  call is invoked
  """
  def __init__(self, input_dim, output_dim):
    super().__init__()
    self.linear = torch.nn.Linear(input_dim, output_dim)

  def forward(self, k1, k2, prediction_param_array, prediction_param_bool):
    if not prediction_param_bool:
      raise ValueError("Expected prediction_param_bool to be True")
    if not torch.all(prediction_param_array):
      raise ValueError("Expected prediction_param_array to be all True")
    out = self.linear(k1) + self.linear(k2)
    return out


@pytest.mark.uses_pytorch
class PytorchRunInferenceTest(unittest.TestCase):
  def test_run_inference_single_tensor_feature(self):
    examples = [
        torch.from_numpy(np.array([1], dtype="float32")),
        torch.from_numpy(np.array([5], dtype="float32")),
        torch.from_numpy(np.array([-3], dtype="float32")),
        torch.from_numpy(np.array([10.0], dtype="float32")),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex, pred in zip(
            examples, torch.Tensor(
                [example * 2.0 + 0.5 for example in examples]).reshape(-1, 1))
    ]

    model = PytorchLinearRegression(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = TestPytorchModelHandlerForInferenceOnly(
        torch.device('cpu'))
    predictions = inference_runner.run_inference(examples, model)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)

  def test_run_inference_multiple_tensor_features(self):
    model = PytorchLinearRegression(input_dim=2, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = TestPytorchModelHandlerForInferenceOnly(
        torch.device('cpu'))
    predictions = inference_runner.run_inference(TWO_FEATURES_EXAMPLES, model)
    for actual, expected in zip(predictions, TWO_FEATURES_PREDICTIONS):
      self.assertEqual(actual, expected)

  def test_run_inference_multiple_tensor_features_dict_output(self):
    model = PytorchLinearRegressionDict(input_dim=2, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = TestPytorchModelHandlerForInferenceOnly(
        torch.device('cpu'))
    predictions = inference_runner.run_inference(TWO_FEATURES_EXAMPLES, model)
    for actual, expected in zip(predictions, TWO_FEATURES_DICT_OUT_PREDICTIONS):
      self.assertEqual(actual, expected)

  def test_run_inference_custom(self):
    examples = [
        torch.from_numpy(np.array([1], dtype="float32")),
        torch.from_numpy(np.array([5], dtype="float32")),
        torch.from_numpy(np.array([-3], dtype="float32")),
        torch.from_numpy(np.array([10.0], dtype="float32")),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex, pred in zip(
            examples, torch.Tensor(
                [example * 2.0 + 1.5 for example in examples]).reshape(-1, 1))
    ]

    model = PytorchLinearRegression(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = TestPytorchModelHandlerForInferenceOnly(
        torch.device('cpu'), inference_fn=custom_tensor_inference_fn)
    predictions = inference_runner.run_inference(examples, model)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)

  def test_run_inference_keyed(self):
    """
    This tests for inputs that are passed as a dictionary from key to tensor
    instead of a standard non-keyed tensor example.

    Example:
    Typical input format is
    input = torch.tensor([1, 2, 3])

    But Pytorch syntax allows inputs to have the form
    input = {
      'k1' : torch.tensor([1, 2, 3]),
      'k2' : torch.tensor([4, 5, 6])
    }
    """
    class PytorchLinearRegressionMultipleArgs(torch.nn.Module):
      def __init__(self, input_dim, output_dim):
        super().__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim)

      def forward(self, k1, k2):
        out = self.linear(k1) + self.linear(k2)
        return out

    model = PytorchLinearRegressionMultipleArgs(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = TestPytorchModelHandlerKeyedTensorForInferenceOnly(
        torch.device('cpu'))
    predictions = inference_runner.run_inference(KEYED_TORCH_EXAMPLES, model)
    for actual, expected in zip(predictions, KEYED_TORCH_PREDICTIONS):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_run_inference_keyed_dict_output(self):
    class PytorchLinearRegressionMultipleArgsDict(torch.nn.Module):
      def __init__(self, input_dim, output_dim):
        super().__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim)

      def forward(self, k1, k2):
        out = self.linear(k1) + self.linear(k2)
        return {'output1': out, 'output2': out}

    model = PytorchLinearRegressionMultipleArgsDict(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = TestPytorchModelHandlerKeyedTensorForInferenceOnly(
        torch.device('cpu'))
    predictions = inference_runner.run_inference(KEYED_TORCH_EXAMPLES, model)
    for actual, expected in zip(predictions, KEYED_TORCH_DICT_OUT_PREDICTIONS):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_inference_runner_inference_args(self):
    """
    This tests for non-batchable input arguments. Since we do the batching
    for the user, we have to distinguish between the inputs that should be
    batched and the ones that should not be batched.
    """
    inference_args = {
        'prediction_param_array': torch.from_numpy(
            np.array([1, 2], dtype="float32")),
        'prediction_param_bool': True
    }

    model = PytorchLinearRegressionKeyedBatchAndExtraInferenceArgs(
        input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = TestPytorchModelHandlerKeyedTensorForInferenceOnly(
        torch.device('cpu'))
    predictions = inference_runner.run_inference(
        batch=KEYED_TORCH_EXAMPLES, model=model, inference_args=inference_args)
    for actual, expected in zip(predictions, KEYED_TORCH_PREDICTIONS):
      self.assertEqual(actual, expected)

  def test_run_inference_helper(self):
    examples = [
        torch.from_numpy(np.array([1], dtype="float32")),
        torch.from_numpy(np.array([5], dtype="float32")),
        torch.from_numpy(np.array([-3], dtype="float32")),
        torch.from_numpy(np.array([10.0], dtype="float32")),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex, pred in zip(
            examples, torch.Tensor(
                [example * 2.0 + 1.0 for example in examples]).reshape(-1, 1))
    ]

    gen_fn = make_tensor_model_fn('generate')

    model = PytorchLinearRegression(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    inference_runner = TestPytorchModelHandlerForInferenceOnly(
        torch.device('cpu'), inference_fn=gen_fn)
    predictions = inference_runner.run_inference(examples, model)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)

  def test_run_inference_keyed_helper(self):
    """
    This tests for inputs that are passed as a dictionary from key to tensor
    instead of a standard non-keyed tensor example.

    Example:
    Typical input format is
    input = torch.tensor([1, 2, 3])

    But Pytorch syntax allows inputs to have the form
    input = {
      'k1' : torch.tensor([1, 2, 3]),
      'k2' : torch.tensor([4, 5, 6])
    }
    """
    class PytorchLinearRegressionMultipleArgs(torch.nn.Module):
      def __init__(self, input_dim, output_dim):
        super().__init__()
        self.linear = torch.nn.Linear(input_dim, output_dim)

      def forward(self, k1, k2):
        out = self.linear(k1) + self.linear(k2)
        return out

      def generate(self, k1, k2):
        out = self.linear(k1) + self.linear(k2) + 0.5
        return out

    model = PytorchLinearRegressionMultipleArgs(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    model.eval()

    gen_fn = make_keyed_tensor_model_fn('generate')

    inference_runner = TestPytorchModelHandlerKeyedTensorForInferenceOnly(
        torch.device('cpu'), inference_fn=gen_fn)
    predictions = inference_runner.run_inference(KEYED_TORCH_EXAMPLES, model)
    for actual, expected in zip(predictions, KEYED_TORCH_HELPER_PREDICTIONS):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_num_bytes(self):
    inference_runner = TestPytorchModelHandlerForInferenceOnly(
        torch.device('cpu'))
    examples = torch.from_numpy(
        np.array([1, 5, 3, 10, -14, 0, 0.5, 0.5],
                 dtype="float32")).reshape(-1, 2)
    self.assertEqual((examples[0].element_size()) * 8,
                     inference_runner.get_num_bytes(examples))

  def test_namespace(self):
    inference_runner = TestPytorchModelHandlerForInferenceOnly(
        torch.device('cpu'))
    self.assertEqual('BeamML_PyTorch', inference_runner.get_metrics_namespace())


@pytest.mark.uses_pytorch
class PytorchRunInferencePipelineTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_pipeline_local_model_simple(self):
    with TestPipeline() as pipeline:
      state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      path = os.path.join(self.tmpdir, 'my_state_dict_path')
      torch.save(state_dict, path)

      model_handler = PytorchModelHandlerTensor(
          state_dict_path=path,
          model_class=PytorchLinearRegression,
          model_params={
              'input_dim': 2, 'output_dim': 1
          })

      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              TWO_FEATURES_PREDICTIONS, equals_fn=_compare_prediction_result))

  def test_pipeline_local_model_large(self):
    with TestPipeline() as pipeline:

      def batch_validator_tensor_inference_fn(
          batch,
          model,
          device,
          inference_args,
          model_id,
      ):
        multi_process_shared_loaded = "multi_process_shared" in str(type(model))
        if not multi_process_shared_loaded:
          raise Exception(
              f'Loaded model of type {type(model)}, was ' +
              'expecting multi_process_shared_model')
        return default_tensor_inference_fn(
            batch, model, device, inference_args, model_id)

      state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      path = os.path.join(self.tmpdir, 'my_state_dict_path')
      torch.save(state_dict, path)

      model_handler = PytorchModelHandlerTensor(
          state_dict_path=path,
          model_class=PytorchLinearRegression,
          model_params={
              'input_dim': 2, 'output_dim': 1
          },
          inference_fn=batch_validator_tensor_inference_fn,
          large_model=True)

      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              TWO_FEATURES_PREDICTIONS, equals_fn=_compare_prediction_result))

  def test_pipeline_local_model_extra_inference_args(self):
    with TestPipeline() as pipeline:
      inference_args = {
          'prediction_param_array': torch.from_numpy(
              np.array([1, 2], dtype="float32")),
          'prediction_param_bool': True
      }

      state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      path = os.path.join(self.tmpdir, 'my_state_dict_path')
      torch.save(state_dict, path)

      model_handler = PytorchModelHandlerKeyedTensor(
          state_dict_path=path,
          model_class=PytorchLinearRegressionKeyedBatchAndExtraInferenceArgs,
          model_params={
              'input_dim': 1, 'output_dim': 1
          })

      pcoll = pipeline | 'start' >> beam.Create(KEYED_TORCH_EXAMPLES)
      inference_args_side_input = (
          pipeline | 'create side' >> beam.Create(inference_args))
      predictions = pcoll | RunInference(
          model_handler=model_handler,
          inference_args=beam.pvalue.AsDict(inference_args_side_input))
      assert_that(
          predictions,
          equal_to(
              KEYED_TORCH_PREDICTIONS, equals_fn=_compare_prediction_result))

  def test_pipeline_local_model_extra_inference_args_large(self):
    with TestPipeline() as pipeline:
      inference_args = {
          'prediction_param_array': torch.from_numpy(
              np.array([1, 2], dtype="float32")),
          'prediction_param_bool': True
      }

      state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      path = os.path.join(self.tmpdir, 'my_state_dict_path')
      torch.save(state_dict, path)

      def batch_validator_keyed_tensor_inference_fn(
          batch,
          model,
          device,
          inference_args,
          model_id,
      ):
        multi_process_shared_loaded = "multi_process_shared" in str(type(model))
        if not multi_process_shared_loaded:
          raise Exception(
              f'Loaded model of type {type(model)}, was ' +
              'expecting multi_process_shared_model')
        return default_keyed_tensor_inference_fn(
            batch, model, device, inference_args, model_id)

      model_handler = PytorchModelHandlerKeyedTensor(
          state_dict_path=path,
          model_class=PytorchLinearRegressionKeyedBatchAndExtraInferenceArgs,
          model_params={
              'input_dim': 1, 'output_dim': 1
          },
          inference_fn=batch_validator_keyed_tensor_inference_fn,
          large_model=True)

      pcoll = pipeline | 'start' >> beam.Create(KEYED_TORCH_EXAMPLES)
      inference_args_side_input = (
          pipeline | 'create side' >> beam.Create(inference_args))
      predictions = pcoll | RunInference(
          model_handler=model_handler,
          inference_args=beam.pvalue.AsDict(inference_args_side_input))
      assert_that(
          predictions,
          equal_to(
              KEYED_TORCH_PREDICTIONS, equals_fn=_compare_prediction_result))

  def test_pipeline_local_model_extra_inference_args_batching_args(self):
    with TestPipeline() as pipeline:
      inference_args = {
          'prediction_param_array': torch.from_numpy(
              np.array([1, 2], dtype="float32")),
          'prediction_param_bool': True
      }

      state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      path = os.path.join(self.tmpdir, 'my_state_dict_path')
      torch.save(state_dict, path)

      def batch_validator_keyed_tensor_inference_fn(
          batch,
          model,
          device,
          inference_args,
          model_id,
      ):
        if len(batch) != 2:
          raise Exception(
              f'Expected batch of size 2, received batch of size {len(batch)}')
        return default_keyed_tensor_inference_fn(
            batch, model, device, inference_args, model_id)

      model_handler = PytorchModelHandlerKeyedTensor(
          state_dict_path=path,
          model_class=PytorchLinearRegressionKeyedBatchAndExtraInferenceArgs,
          model_params={
              'input_dim': 1, 'output_dim': 1
          },
          inference_fn=batch_validator_keyed_tensor_inference_fn,
          min_batch_size=2,
          max_batch_size=2)

      pcoll = pipeline | 'start' >> beam.Create(KEYED_TORCH_EXAMPLES)
      inference_args_side_input = (
          pipeline | 'create side' >> beam.Create(inference_args))
      predictions = pcoll | RunInference(
          model_handler=model_handler,
          inference_args=beam.pvalue.AsDict(inference_args_side_input))
      assert_that(
          predictions,
          equal_to(
              KEYED_TORCH_PREDICTIONS, equals_fn=_compare_prediction_result))

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_gcs_model(self):
    with TestPipeline() as pipeline:
      examples = torch.from_numpy(
          np.array([1, 5, 3, 10], dtype="float32").reshape(-1, 1))
      expected_predictions = [
          PredictionResult(ex, pred) for ex, pred in zip(
              examples, torch.Tensor(
                  [example * 2.0 + 0.5 for example in examples]).reshape(-1, 1))
      ]

      gs_pth = 'gs://apache-beam-ml/models/' \
          'pytorch_lin_reg_model_2x+0.5_state_dict.pth'
      model_handler = PytorchModelHandlerTensor(
          state_dict_path=gs_pth,
          model_class=PytorchLinearRegression,
          model_params={
              'input_dim': 1, 'output_dim': 1
          })

      pcoll = pipeline | 'start' >> beam.Create(examples)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(expected_predictions, equals_fn=_compare_prediction_result))

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_gcs_model_control_batching(self):
    with TestPipeline() as pipeline:
      examples = torch.from_numpy(
          np.array([1, 5, 3, 10], dtype="float32").reshape(-1, 1))
      expected_predictions = [
          PredictionResult(ex, pred) for ex, pred in zip(
              examples, torch.Tensor(
                  [example * 2.0 + 0.5 for example in examples]).reshape(-1, 1))
      ]

      def batch_validator_tensor_inference_fn(
          batch,
          model,
          device,
          inference_args,
          model_id,
      ):
        if len(batch) != 2:
          raise Exception(
              f'Expected batch of size 2, received batch of size {len(batch)}')
        return default_tensor_inference_fn(
            batch, model, device, inference_args, model_id)


      gs_pth = 'gs://apache-beam-ml/models/' \
          'pytorch_lin_reg_model_2x+0.5_state_dict.pth'
      model_handler = PytorchModelHandlerTensor(
          state_dict_path=gs_pth,
          model_class=PytorchLinearRegression,
          model_params={
              'input_dim': 1, 'output_dim': 1
          },
          inference_fn=batch_validator_tensor_inference_fn,
          min_batch_size=2,
          max_batch_size=2)

      pcoll = pipeline | 'start' >> beam.Create(examples)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(expected_predictions, equals_fn=_compare_prediction_result))

  def test_invalid_input_type(self):
    with self.assertRaisesRegex(Exception, "expected Tensor as element"):
      with TestPipeline() as pipeline:
        examples = np.array([1, 5, 3, 10], dtype="float32").reshape(-1, 1)

        state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                  ('linear.bias', torch.Tensor([0.5]))])
        path = os.path.join(self.tmpdir, 'my_state_dict_path')
        torch.save(state_dict, path)

        model_handler = PytorchModelHandlerTensor(
            state_dict_path=path,
            model_class=PytorchLinearRegression,
            model_params={
                'input_dim': 1, 'output_dim': 1
            })

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)

  def test_gpu_auto_convert_to_cpu(self):
    """
    This tests the scenario in which the user defines `device='GPU'` for the
    PytorchModelHandlerX, but runs the pipeline on a machine without GPU, we
    automatically detect this discrepancy and do automatic conversion to CPU.
    A warning is also logged to inform the user.
    """
    with self.assertLogs() as log:
      with TestPipeline() as pipeline:
        examples = torch.from_numpy(
            np.array([1, 5, 3, 10], dtype="float32").reshape(-1, 1))

        state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                  ('linear.bias', torch.Tensor([0.5]))])
        path = os.path.join(self.tmpdir, 'my_state_dict_path')
        torch.save(state_dict, path)

        model_handler = PytorchModelHandlerTensor(
            state_dict_path=path,
            model_class=PytorchLinearRegression,
            model_params={
                'input_dim': 1, 'output_dim': 1
            },
            device='GPU')
        # Upon initialization, device is cuda
        self.assertEqual(model_handler._device, torch.device('cuda'))

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)

        # During model loading, device converted to cuda
        self.assertEqual(model_handler._device, torch.device('cuda'))

      self.assertIn("INFO:root:Device is set to CUDA", log.output)
      self.assertIn(
          "WARNING:root:Model handler specified a 'GPU' device, but GPUs " \
          "are not available. Switching to CPU.",
          log.output)

  def test_load_torch_script_model(self):
    torch_model = PytorchLinearRegression(2, 1)
    torch_script_model = torch.jit.script(torch_model)

    torch_script_path = os.path.join(self.tmpdir, 'torch_script_model.pt')

    torch.jit.save(torch_script_model, torch_script_path)

    model_handler = PytorchModelHandlerTensor(
        torch_script_model_path=torch_script_path)

    torch_script_model = model_handler.load_model()

    self.assertTrue(isinstance(torch_script_model, torch.jit.ScriptModule))

  def test_inference_torch_script_model(self):
    torch_model = PytorchLinearRegression(2, 1)
    torch_model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                     ('linear.bias', torch.Tensor([0.5]))]))

    torch_script_model = torch.jit.script(torch_model)

    torch_script_path = os.path.join(self.tmpdir, 'torch_script_model.pt')

    torch.jit.save(torch_script_model, torch_script_path)

    model_handler = PytorchModelHandlerTensor(
        torch_script_model_path=torch_script_path)

    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              TWO_FEATURES_PREDICTIONS, equals_fn=_compare_prediction_result))

  def test_torch_model_class_none(self):
    torch_model = PytorchLinearRegression(2, 1)
    torch_path = os.path.join(self.tmpdir, 'torch_model.pt')

    torch.save(torch_model, torch_path)

    with self.assertRaisesRegex(
        RuntimeError,
        "A state_dict_path has been supplied to the model "
        "handler, but the required model_class is missing. "
        "Please provide the model_class in order to"):
      _ = PytorchModelHandlerTensor(state_dict_path=torch_path)

    with self.assertRaisesRegex(
        RuntimeError,
        "A state_dict_path has been supplied to the model "
        "handler, but the required model_class is missing. "
        "Please provide the model_class in order to"):
      _ = (PytorchModelHandlerKeyedTensor(state_dict_path=torch_path))

  def test_torch_model_state_dict_none(self):
    with self.assertRaisesRegex(
        RuntimeError,
        "A model_class has been supplied to the model "
        "handler, but the required state_dict_path is missing. "
        "Please provide the state_dict_path in order to"):
      _ = PytorchModelHandlerTensor(model_class=PytorchLinearRegression)

    with self.assertRaisesRegex(
        RuntimeError,
        "A model_class has been supplied to the model "
        "handler, but the required state_dict_path is missing. "
        "Please provide the state_dict_path in order to"):
      _ = PytorchModelHandlerKeyedTensor(model_class=PytorchLinearRegression)

  def test_specify_torch_script_path_and_state_dict_path(self):
    torch_model = PytorchLinearRegression(2, 1)
    torch_path = os.path.join(self.tmpdir, 'torch_model.pt')

    torch.save(torch_model, torch_path)
    torch_script_model = torch.jit.script(torch_model)

    torch_script_path = os.path.join(self.tmpdir, 'torch_script_model.pt')

    torch.jit.save(torch_script_model, torch_script_path)
    with self.assertRaisesRegex(
        RuntimeError, "Please specify either torch_script_model_path or "):
      _ = PytorchModelHandlerTensor(
          state_dict_path=torch_path,
          model_class=PytorchLinearRegression,
          torch_script_model_path=torch_script_path)

  def test_prediction_result_model_id_with_torch_script_model(self):
    torch_model = PytorchLinearRegression(2, 1)
    torch_script_model = torch.jit.script(torch_model)
    torch_script_path = os.path.join(self.tmpdir, 'torch_script_model.pt')
    torch.jit.save(torch_script_model, torch_script_path)

    model_handler = PytorchModelHandlerTensor(
        torch_script_model_path=torch_script_path)

    def check_torch_script_model_id(element):
      assert ('torch_script_model.pt' in element.model_id) is True

    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(model_handler)
      _ = predictions | beam.Map(check_torch_script_model_id)

  def test_prediction_result_model_id_with_torch_model(self):
    # weights associated with PytorchLinearRegression class
    state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                              ('linear.bias', torch.Tensor([0.5]))])
    torch_path = os.path.join(self.tmpdir, 'torch_model.pt')
    torch.save(state_dict, torch_path)

    model_handler = PytorchModelHandlerTensor(
        state_dict_path=torch_path,
        model_class=PytorchLinearRegression,
        model_params={
            'input_dim': 2, 'output_dim': 1
        })

    def check_torch_script_model_id(element):
      assert ('torch_model.pt' in element.model_id) is True

    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(model_handler)
      _ = predictions | beam.Map(check_torch_script_model_id)

  def test_env_vars_set_correctly_tensor_handler(self):
    torch_model = PytorchLinearRegression(2, 1)
    torch_model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                     ('linear.bias', torch.Tensor([0.5]))]))

    torch_script_model = torch.jit.script(torch_model)

    torch_script_path = os.path.join(self.tmpdir, 'torch_script_model.pt')

    torch.jit.save(torch_script_model, torch_script_path)

    handler_with_vars = PytorchModelHandlerTensor(
        torch_script_model_path=torch_script_path, env_vars={'FOO': 'bar'})
    os.environ.pop('FOO', None)
    self.assertFalse('FOO' in os.environ)
    with TestPipeline() as pipeline:
      _ = (
          pipeline
          | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
          | RunInference(handler_with_vars))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue((os.environ['FOO']) == 'bar')

  def test_env_vars_set_correctly_keyed_tensor_handler(self):
    os.environ.pop('FOO', None)
    self.assertFalse('FOO' in os.environ)
    with TestPipeline() as pipeline:
      inference_args = {
          'prediction_param_array': torch.from_numpy(
              np.array([1, 2], dtype="float32")),
          'prediction_param_bool': True
      }

      state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      path = os.path.join(self.tmpdir, 'my_state_dict_path')
      torch.save(state_dict, path)

      handler_with_vars = PytorchModelHandlerKeyedTensor(
          env_vars={'FOO': 'bar'},
          state_dict_path=path,
          model_class=PytorchLinearRegressionKeyedBatchAndExtraInferenceArgs,
          model_params={
              'input_dim': 1, 'output_dim': 1
          })
      inference_args_side_input = (
          pipeline | 'create side' >> beam.Create(inference_args))

      _ = (
          pipeline
          | 'start' >> beam.Create(KEYED_TORCH_EXAMPLES)
          | RunInference(
              model_handler=handler_with_vars,
              inference_args=beam.pvalue.AsDict(inference_args_side_input)))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue((os.environ['FOO']) == 'bar')


@pytest.mark.uses_pytorch
class PytorchInferenceTestWithMocks(unittest.TestCase):
  def setUp(self):
    self._load_model = pytorch_inference._load_model
    pytorch_inference._load_model = unittest.mock.MagicMock(
        return_value=("model", "device"))
    self.tmpdir = tempfile.mkdtemp()
    self.state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                                   ('linear.bias', torch.Tensor([0.5]))])
    self.torch_path = os.path.join(self.tmpdir, 'torch_model.pt')
    torch.save(self.state_dict, self.torch_path)
    self.model_params = {'input_dim': 2, 'output_dim': 1}

  def tearDown(self):
    pytorch_inference._load_model = self._load_model
    shutil.rmtree(self.tmpdir)

  def test_load_model_args_tensor(self):
    load_model_args = {'weights_only': True}
    model_handler = PytorchModelHandlerTensor(
        state_dict_path=self.torch_path,
        model_class=PytorchLinearRegression,
        model_params=self.model_params,
        load_model_args=load_model_args)
    model_handler.load_model()
    pytorch_inference._load_model.assert_called_with(
        model_class=PytorchLinearRegression,
        state_dict_path=self.torch_path,
        device=torch.device('cpu'),
        model_params=self.model_params,
        torch_script_model_path=None,
        load_model_args=load_model_args)

  def test_load_model_args_keyed_tensor(self):
    load_model_args = {'weights_only': True}
    model_handler = PytorchModelHandlerKeyedTensor(
        state_dict_path=self.torch_path,
        model_class=PytorchLinearRegression,
        model_params=self.model_params,
        load_model_args=load_model_args)
    model_handler.load_model()
    pytorch_inference._load_model.assert_called_with(
        model_class=PytorchLinearRegression,
        state_dict_path=self.torch_path,
        device=torch.device('cpu'),
        model_params=self.model_params,
        torch_script_model_path=None,
        load_model_args=load_model_args)


if __name__ == '__main__':
  unittest.main()
