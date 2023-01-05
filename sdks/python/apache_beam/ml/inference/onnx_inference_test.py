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
import sys
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
  import onnxruntime as ort
  import torch
  from onnxruntime.capi.onnxruntime_pybind11_state import InvalidArgument
  import tensorflow as tf
  import tf2onnx
  from tensorflow import keras
  from tensorflow.keras import layers
  from sklearn import linear_model
  from skl2onnx import convert_sklearn
  from skl2onnx.common.data_types import FloatTensorType
  from apache_beam.ml.inference.base import PredictionResult
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference.onnx_inference import default_numpy_inference_fn
  from apache_beam.ml.inference.onnx_inference import OnnxModelHandler
except ImportError:
  raise unittest.SkipTest('Onnx dependencies are not installed')

try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  GCSFileSystem = None  # type: ignore

TWO_FEATURES_EXAMPLES = [
    np.array([1, 5], dtype="float32"),
    np.array([3, 10], dtype="float32"),
    np.array([-14, 0], dtype="float32"),
    np.array([0.5, 0.5], dtype="float32")
]

TWO_FEATURES_PREDICTIONS = [
    PredictionResult(ex, pred) for ex,
    pred in zip(
        TWO_FEATURES_EXAMPLES,
        [f1 * 2.0 + f2 * 3 + 0.5
             for f1, f2 in TWO_FEATURES_EXAMPLES])
]

TWO_FEATURES_DICT_OUT_PREDICTIONS = [
    PredictionResult(
        p.example, {
            "output1": p.inference, "output2": p.inference
        }) for p in TWO_FEATURES_PREDICTIONS
]


class TestOnnxModelHandler(OnnxModelHandler):
  def __init__(self,model_uri: str,*,inference_fn = default_numpy_inference_fn):
    self._model_uri = model_uri
    self._model_inference_fn = inference_fn

def _compare_prediction_result(a, b):
  example_equal = np.array_equal(a.example, b.example)
  if isinstance(a.inference, dict):
    return all(
        x == y for x, y in zip(a.inference.values(),
                               b.inference.values())) and example_equal
  return a.inference == b.inference and example_equal

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

@pytest.mark.uses_pytorch
class OnnxPytorchRunInferenceTest(unittest.TestCase):

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_onnx_pytorch_run_inference(self):
    examples = [
        np.array([1], dtype="float32"),
        np.array([5], dtype="float32"),
        np.array([-3], dtype="float32"),
        np.array([10.0], dtype="float32"),
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
    path = os.path.join(self.tmpdir, 'my_onnx_pytorch_path')
    dummy_input = torch.randn(4, 1, requires_grad=True)
    torch.onnx.export(model,
                      dummy_input,               # model input (or a tuple for multiple inputs)
                      path,   # where to save the model (can be a file or file-like object)
                      export_params=True,        # store the trained parameter weights inside the model file
                      opset_version=10,          # the ONNX version to export the model to
                      do_constant_folding=True,  # whether to execute constant folding for optimization
                      input_names = ['input'],   # the model's input names
                      output_names = ['output'], # the model's output names
                      dynamic_axes={'input' : {0 : 'batch_size'},    # variable length axes
                                    'output' : {0 : 'batch_size'}})
    
    inference_runner = TestOnnxModelHandler(path)
    inference_session = ort.InferenceSession(path, providers=['CUDAExecutionProvider', 'CPUExecutionProvider']) # this list specifies priority - prioritize gpu if cuda kernel exists
    predictions = inference_runner.run_inference(examples, inference_session)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)

  def test_num_bytes(self):
    inference_runner = TestOnnxModelHandler("dummy")
    batched_examples_int = [
        np.array([1, 2, 3]), np.array([4, 5, 6]), np.array([7, 8, 9])
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_int[0]) * 3,
        inference_runner.get_num_bytes(batched_examples_int))

    batched_examples_float = [
        np.array([1.0, 2.0, 3.0]),
        np.array([4.1, 5.2, 6.3]),
        np.array([7.7, 8.8, 9.9])
    ]
    self.assertEqual(
        sys.getsizeof(batched_examples_float[0]) * 3,
        inference_runner.get_num_bytes(batched_examples_float))

  def test_namespace(self):
    inference_runner = TestOnnxModelHandler("dummy")
    self.assertEqual('BeamML_Onnx', inference_runner.get_metrics_namespace())

@pytest.mark.uses_tensorflow
class OnnxTensorflowRunInferenceTest(unittest.TestCase):

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def test_onnx_tensorflow_run_inference(self):
    examples = [
        np.array([1], dtype="float32"),
        np.array([5], dtype="float32"),
        np.array([-3], dtype="float32"),
        np.array([10.0], dtype="float32"),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex,
        pred in zip(
            examples,
            [example * 2.0 + 0.5
                      for example in examples])
    ]

    params = [np.array([[2.0]], dtype="float32"), np.array([0.5], dtype="float32")]
    linear_layer = layers.Dense(units=1, weights=params)
    linear_model = tf.keras.Sequential([linear_layer])
    
    path = os.path.join(self.tmpdir, 'my_onnx_tf_path')
    spec = (tf.TensorSpec((None, 1), tf.float32, name="input"),)
    model_proto, _ = tf2onnx.convert.from_keras(linear_model, input_signature=spec, opset=13, output_path=path)
    
    inference_runner = TestOnnxModelHandler(path)
    inference_session = ort.InferenceSession(path, providers=['CUDAExecutionProvider', 'CPUExecutionProvider']) # this list specifies priority - prioritize gpu if cuda kernel exists
    predictions = inference_runner.run_inference(examples, inference_session)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)

@pytest.mark.uses_sklearn
class OnnxSklearnRunInferenceTest(unittest.TestCase):

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def build_model(self):
    x = [[0],[1]]
    y = [0.5, 2.5]
    model = linear_model.LinearRegression()
    model.fit(x, y)
    return model

  def save_model(self, model, input_dim, path):
    # assume float input
    initial_type = [('float_input', FloatTensorType([None, input_dim]))]
    onx = convert_sklearn(model, initial_types=initial_type)
    with open(path, "wb") as f:
      f.write(onx.SerializeToString())

  def test_onnx_sklearn_run_inference(self):
    examples = [
        np.array([1], dtype="float32"),
        np.array([5], dtype="float32"),
        np.array([-3], dtype="float32"),
        np.array([10.0], dtype="float32"),
    ]
    expected_predictions = [
        PredictionResult(ex, pred) for ex,
        pred in zip(
            examples,
            [example * 2.0 + 0.5
                      for example in examples])
    ]

    linear_model = self.build_model()
    path = os.path.join(self.tmpdir, 'my_onnx_sklearn_path')
    self.save_model(linear_model, 1, path)

    inference_runner = TestOnnxModelHandler(path)
    inference_session = ort.InferenceSession(path, providers=['CUDAExecutionProvider', 'CPUExecutionProvider']) # this list specifies priority - prioritize gpu if cuda kernel exists
    predictions = inference_runner.run_inference(examples, inference_session)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)


@pytest.mark.uses_pytorch
class OnnxPytorchRunInferencePipelineTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def exportModelToOnnx(self, state_dict, path):
    model = PytorchLinearRegression(input_dim=2, output_dim=1)
    model.load_state_dict(state_dict)
    dummy_input = torch.randn(4, 2, requires_grad=True)
    torch.onnx.export(model,
                      dummy_input,               # model input (or a tuple for multiple inputs)
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
      path = os.path.join(self.tmpdir, 'my_onnx_pytorch_path')
      torch_state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                                ('linear.bias', torch.Tensor([0.5]))])
      self.exportModelToOnnx(torch_state_dict, path)
      model_handler = TestOnnxModelHandler(path)

      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              TWO_FEATURES_PREDICTIONS, equals_fn=_compare_prediction_result))

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
    with self.assertRaisesRegex(InvalidArgument, "Got invalid dimensions for input: input for the following indices"):
      with TestPipeline() as pipeline:
        examples = [np.array([1], dtype="float32")]
        path = os.path.join(self.tmpdir, 'my_onnx_pytorch_path')
        state_dict = OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                                ('linear.bias', torch.Tensor([0.5]))])
        self.exportModelToOnnx(state_dict, path)

        model_handler = TestOnnxModelHandler(path)

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)


@pytest.mark.uses_tensorflow
class OnnxTensorflowRunInferencePipelineTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def exportModelToOnnx(self, state_dict, path):
    
    params = [state_dict["linear.weight"], state_dict["linear.bias"]]
    linear_layer = layers.Dense(units=1, weights=params)
    linear_model = tf.keras.Sequential([linear_layer])
    spec = (tf.TensorSpec((None, 2), tf.float32, name="input"),)
    model_proto, _ = tf2onnx.convert.from_keras(linear_model, input_signature=spec, opset=13, output_path=path)

  def test_pipeline_local_model_simple(self):
    with TestPipeline() as pipeline:
      path = os.path.join(self.tmpdir, 'my_onnx_tensorflow_path')
      state_dict = OrderedDict([('linear.weight', np.array([[2.0], [3]], dtype="float32")),
                                ('linear.bias', np.array([0.5], dtype="float32"))])
      self.exportModelToOnnx(state_dict, path)
      model_handler = TestOnnxModelHandler(path)

      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              TWO_FEATURES_PREDICTIONS, equals_fn=_compare_prediction_result))

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


  # need to figure out what type of error this is
  def test_invalid_input_type(self):
    with self.assertRaisesRegex(InvalidArgument, "Got invalid dimensions for input: input for the following indices"):
      with TestPipeline() as pipeline:
        examples = [np.array([1], dtype="float32")]
        path = os.path.join(self.tmpdir, 'my_onnx_tensorflow_path')
        state_dict = OrderedDict([('linear.weight', np.array([[2.0], [3]], dtype="float32")),
                                ('linear.bias', np.array([0.5], dtype="float32"))])
        self.exportModelToOnnx(state_dict, path)

        model_handler = TestOnnxModelHandler(path)

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)


@pytest.mark.uses_sklearn
class OnnxSklearnRunInferencePipelineTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)

  def build_model(self):
    x = [[1,5],[3,2],[1,0]]
    y = [17.5, 12.5, 2.5]
    model = linear_model.LinearRegression()
    model.fit(x, y)
    return model

  def save_model(self, model, input_dim, path):
    # assume float input
    initial_type = [('float_input', FloatTensorType([None, input_dim]))]
    onx = convert_sklearn(model, initial_types=initial_type)
    with open(path, "wb") as f:
      f.write(onx.SerializeToString())

  def test_pipeline_local_model_simple(self):
    with TestPipeline() as pipeline:
      path = os.path.join(self.tmpdir, 'my_onnx_sklearn_path')
      model = self.build_model()
      self.save_model(model, 2, path)
      model_handler = TestOnnxModelHandler(path)

      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              TWO_FEATURES_PREDICTIONS, equals_fn=_compare_prediction_result))

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
    with self.assertRaises(InvalidArgument):
      with TestPipeline() as pipeline:
        examples = [np.array([1], dtype="float32")]
        path = os.path.join(self.tmpdir, 'my_onnx_sklearn_path')
        model = self.build_model()
        self.save_model(model, 2, path)

        model_handler = TestOnnxModelHandler(path)

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)


if __name__ == '__main__':
  unittest.main()
