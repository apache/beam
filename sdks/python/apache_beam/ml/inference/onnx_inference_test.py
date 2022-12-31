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

# Protect against environments where onnx and pytorch library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import onnx
  import onnxruntime.InferenceSession
  import torch
  from apache_beam.ml.inference.base import PredictionResult
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference.onnx_inference import OnnxModelHandlerTensor
except ImportError:
  raise unittest.SkipTest('Onnx and PyTorch dependencies are not installed')

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
    PredictionResult(ex, pred) for ex,
    pred in zip(
        TWO_FEATURES_EXAMPLES,
        torch.Tensor(
            [f1 * 2.0 + f2 * 3 + 0.5
             for f1, f2 in TWO_FEATURES_EXAMPLES]).reshape(-1, 1))
]

TWO_FEATURES_DICT_OUT_PREDICTIONS = [
    PredictionResult(
        p.example, {
            "output1": p.inference, "output2": p.inference
        }) for p in TWO_FEATURES_PREDICTIONS
]


class TestOnnxModelHandler(OnnxModelHandlerTensor):
  def __init__(self,model_uri: str,*,inference_fn: NumpyInferenceFn = _default_numpy_inference_fn):
    self._model_uri = model_uri
    self._model_inference_fn = inference_fn


def _compare_prediction_result(x, y):
  if isinstance(x.example, dict):
    example_equals = all(
        torch.equal(x, y) for x,
        y in zip(x.example.values(), y.example.values()))
  else:
    example_equals = torch.equal(x.example, y.example)
  if not example_equals:
    return False

  if isinstance(x.inference, dict):
    return all(
        torch.equal(x, y) for x,
        y in zip(x.inference.values(), y.inference.values()))

  return torch.equal(x.inference, y.inference)

def _to_numpy(tensor):
      return tensor.detach().cpu().numpy() if tensor.requires_grad else tensor.cpu().numpy()

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


'''
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
'''

@pytest.mark.uses_pytorch
class OnnxPytorchRunInferenceTest(unittest.TestCase):

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_onnx_pytorch_run_inference(self):
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
            [example * 2.0 + 0.5
                      for example in examples])
    ]

    model = PytorchLinearRegression(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    path = os.path.join(self.tmpdir, 'my_onnx_path')
    torch.onnx.export(model,
                      example,               # model input (or a tuple for multiple inputs)
                      path,   # where to save the model (can be a file or file-like object)
                      export_params=True,        # store the trained parameter weights inside the model file
                      opset_version=10,          # the ONNX version to export the model to
                      do_constant_folding=True,  # whether to execute constant folding for optimization
                      input_names = ['input'],   # the model's input names
                      output_names = ['output'], # the model's output names
                      dynamic_axes={'input' : {0 : 'batch_size'},    # variable length axes
                                    'output' : {0 : 'batch_size'}})
    
    inference_runner = TestOnnxModelHandler(path)
    predictions = inference_runner.run_inference(examples, model)
    for actual, expected in zip(predictions[0], expected_predictions):
      self.assertEqual(actual, _to_numpy(expected))

  def test_num_bytes(self):
    inference_runner = TestOnnxModelHandler("dummy")
    examples = torch.from_numpy(
        np.array([1, 5, 3, 10, -14, 0, 0.5, 0.5],
                 dtype="float32")).reshape(-1, 2)
    self.assertEqual((examples[0].element_size()) * 8,
                     inference_runner.get_num_bytes(examples))

  def test_namespace(self):
    inference_runner = TestOnnxModelHandler("dummy")
    self.assertEqual('BeamML_Onnx', inference_runner.get_metrics_namespace())


@pytest.mark.uses_pytorch
class OnnxPytorchRunInferencePipelineTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def exportModelToOnnx(self, state_dict, path):
    model = PytorchLinearRegression(input_dim=2, output_dim=1)
    model.load_state_dict(state_dict)
    torch.onnx.export(model,
                      TWO_FEATURES_EXAMPLES,               # model input (or a tuple for multiple inputs)
                      path,   # where to save the model (can be a file or file-like object)
                      export_params=True,        # store the trained parameter weights inside the model file
                      opset_version=10,          # the ONNX version to export the model to
                      do_constant_folding=True,  # whether to execute constant folding for optimization
                      input_names = ['input'],   # the model's input names
                      output_names = ['output'], # the model's output names
                      dynamic_axes={'input' : {0 : 'batch_size'},    # variable length axes
                                    'output' : {0 : 'batch_size'}})


  def test_pipeline_local_model_simple(self):
    with TestPipeline() as pipeline:
      path = os.path.join(self.tmpdir, 'my_onnx_path')
      torch_state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      self.exportModelToOnnx(torch_state_dict, path)
      model_handler = OnnxModelHandlerTensor(path)

      pcoll = pipeline | 'start' >> beam.Create(_to_numpy(TWO_FEATURES_EXAMPLES))
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              _to_numpy(TWO_FEATURES_PREDICTIONS), equals_fn=_compare_prediction_result))

  # need to put onnx in gs path
  '''
  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
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
  '''
  def test_invalid_input_type(self):
    with self.assertRaisesRegex(TypeError, "expected Nunmpy as element"):
      with TestPipeline() as pipeline:
        examples = torch.from_numpy(np.array([1, 5, 3, 10], dtype="float32").reshape(-1, 1))
        path = os.path.join(self.tmpdir, 'my_onnx_path')
        state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                                  ('linear.bias', torch.Tensor([0.5]))])
        self.exportModelToOnnx(state_dict, path)

        model_handler = onnxModelHandlerTensor(path)

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)


if __name__ == '__main__':
  unittest.main()
