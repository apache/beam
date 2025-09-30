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

if bool(1):  # lint doesn't like an unconditional `raise`.
  raise unittest.SkipTest(
      'TODO: fix https://github.com/apache/beam/issues/31254')

# Protect against environments where onnx and pytorch library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import onnxruntime as ort
  import torch
  import tensorflow as tf
  import tf2onnx
  from tensorflow.keras import layers
  from sklearn import linear_model
  from skl2onnx import convert_sklearn
  from skl2onnx.common.data_types import FloatTensorType
  from apache_beam.ml.inference.base import PredictionResult
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference.onnx_inference import default_numpy_inference_fn
  from apache_beam.ml.inference.onnx_inference import OnnxModelHandlerNumpy
except ImportError:
  raise unittest.SkipTest('Onnx dependencies are not installed')

try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  GCSFileSystem = None  # type: ignore


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


class TestDataAndModel():
  def get_one_feature_samples(self):
    return [
        np.array([1], dtype="float32"),
        np.array([5], dtype="float32"),
        np.array([-3], dtype="float32"),
        np.array([10.0], dtype="float32"),
    ]

  def get_one_feature_predictions(self):
    return [
        PredictionResult(ex, pred) for ex, pred in zip(
            self.get_one_feature_samples(),
            [example * 2.0 + 0.5 for example in self.get_one_feature_samples()])
    ]

  def get_two_feature_examples(self):
    return [
        np.array([1, 5], dtype="float32"),
        np.array([3, 10], dtype="float32"),
        np.array([-14, 0], dtype="float32"),
        np.array([0.5, 0.5], dtype="float32")
    ]

  def get_two_feature_predictions(self):
    return [
        PredictionResult(ex, pred) for ex, pred in zip(
            self.get_two_feature_examples(), [
                f1 * 2.0 + f2 * 3 + 0.5
                for f1, f2 in self.get_two_feature_examples()
            ])
    ]

  def get_torch_one_feature_model(self):
    model = PytorchLinearRegression(input_dim=1, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    return model

  def get_tf_one_feature_model(self):
    params = [
        np.array([[2.0]], dtype="float32"), np.array([0.5], dtype="float32")
    ]
    linear_layer = layers.Dense(units=1, weights=params)
    linear_model = tf.keras.Sequential([linear_layer])
    return linear_model

  def get_sklearn_one_feature_model(self):
    x = [[0], [1]]
    y = [0.5, 2.5]
    model = linear_model.LinearRegression()
    model.fit(x, y)
    return model

  def get_torch_two_feature_model(self):
    model = PytorchLinearRegression(input_dim=2, output_dim=1)
    model.load_state_dict(
        OrderedDict([('linear.weight', torch.Tensor([[2.0, 3]])),
                     ('linear.bias', torch.Tensor([0.5]))]))
    return model

  def get_tf_two_feature_model(self):
    params = [np.array([[2.0], [3]]), np.array([0.5], dtype="float32")]
    linear_layer = layers.Dense(units=1, weights=params)
    linear_model = tf.keras.Sequential([linear_layer])
    return linear_model

  def get_sklearn_two_feature_model(self):
    x = [[1, 5], [3, 2], [1, 0]]
    y = [17.5, 12.5, 2.5]
    model = linear_model.LinearRegression()
    model.fit(x, y)
    return model


def _compare_prediction_result(a, b):
  example_equal = np.array_equal(a.example, b.example)
  if isinstance(a.inference, dict):
    return all(
        x == y for x, y in zip(a.inference.values(),
                               b.inference.values())) and example_equal
  return a.inference == b.inference and example_equal


def _to_numpy(tensor):
  return tensor.detach().cpu().numpy() if tensor.requires_grad else tensor.cpu(
  ).numpy()


class TestOnnxModelHandler(OnnxModelHandlerNumpy):
  def __init__( #pylint: disable=dangerous-default-value
      self,
      model_uri: str,
      session_options=None,
      providers=['CUDAExecutionProvider', 'CPUExecutionProvider'],
      provider_options=None,
      *,
      inference_fn=default_numpy_inference_fn,
      large_model=False,
      **kwargs):
    self._model_uri = model_uri
    self._session_options = session_options
    self._providers = providers
    self._provider_options = provider_options
    self._model_inference_fn = inference_fn
    self._env_vars = kwargs.get('env_vars', {})
    self._large_model = large_model


class OnnxTestBase(unittest.TestCase):
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    self.test_data_and_model = TestDataAndModel()

  def tearDown(self):
    shutil.rmtree(self.tmpdir)


@pytest.mark.uses_onnx
class OnnxPytorchRunInferenceTest(OnnxTestBase):
  def test_onnx_pytorch_run_inference(self):
    examples = self.test_data_and_model.get_one_feature_samples()
    expected_predictions = self.test_data_and_model.get_one_feature_predictions(
    )

    model = self.test_data_and_model.get_torch_one_feature_model()
    path = os.path.join(self.tmpdir, 'my_onnx_pytorch_path')
    dummy_input = torch.randn(4, 1, requires_grad=True)
    torch.onnx.export(
        model,
        dummy_input,  # model input
        path,  # where to save the model
        export_params=True,  # store the trained parameter weights
        opset_version=10,  # the ONNX version
        do_constant_folding=True,  # whether to execute constant-
        # folding for optimization
        input_names=['input'],  # model's input names
        output_names=['output'],  # model's output names
        dynamic_axes={
            'input': {
                0: 'batch_size'
            }, 'output': {
                0: 'batch_size'
            }
        })

    inference_runner = TestOnnxModelHandler(path)
    inference_session = ort.InferenceSession(
        path, providers=['CUDAExecutionProvider', 'CPUExecutionProvider']
    )  # this list specifies priority - prioritize gpu if cuda kernel exists
    predictions = inference_runner.run_inference(examples, inference_session)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)

  def test_num_bytes(self):
    inference_runner = TestOnnxModelHandler("dummy")
    batched_examples_int = [
        np.array([1, 2, 3]), np.array([4, 5, 6]), np.array([7, 8, 9])
    ]
    self.assertEqual(
        batched_examples_int[0].itemsize * 3,
        inference_runner.get_num_bytes(batched_examples_int))

    batched_examples_float = [
        np.array([1, 5], dtype=np.float32),
        np.array([3, 10], dtype=np.float32),
        np.array([-14, 0], dtype=np.float32),
        np.array([0.5, 0.5], dtype=np.float32)
    ]
    self.assertEqual(
        batched_examples_float[0].itemsize * 4,
        inference_runner.get_num_bytes(batched_examples_float))

  def test_namespace(self):
    inference_runner = TestOnnxModelHandler("dummy")
    self.assertEqual('BeamML_Onnx', inference_runner.get_metrics_namespace())


@pytest.mark.uses_onnx
class OnnxTensorflowRunInferenceTest(OnnxTestBase):
  def test_onnx_tensorflow_run_inference(self):
    examples = self.test_data_and_model.get_one_feature_samples()
    expected_predictions = self.test_data_and_model.get_one_feature_predictions(
    )
    linear_model = self.test_data_and_model.get_tf_one_feature_model()

    path = os.path.join(self.tmpdir, 'my_onnx_tf_path')
    spec = (tf.TensorSpec((None, 1), tf.float32, name="input"), )
    _, _ = tf2onnx.convert.from_keras(linear_model,
    input_signature=spec,
    opset=13,
    output_path=path)

    inference_runner = TestOnnxModelHandler(path)
    inference_session = ort.InferenceSession(
        path, providers=['CUDAExecutionProvider', 'CPUExecutionProvider']
    )  # this list specifies priority - prioritize gpu if cuda kernel exists
    predictions = inference_runner.run_inference(examples, inference_session)
    for actual, expected in zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)


@pytest.mark.uses_onnx
class OnnxSklearnRunInferenceTest(OnnxTestBase):
  def save_model(self, model, input_dim, path):
    # assume float input
    initial_type = [('float_input', FloatTensorType([None, input_dim]))]
    onx = convert_sklearn(model, initial_types=initial_type)
    with open(path, "wb") as f:
      f.write(onx.SerializeToString())

  def test_onnx_sklearn_run_inference(self):
    examples = self.test_data_and_model.get_one_feature_samples()
    expected_predictions = self.test_data_and_model.get_one_feature_predictions(
    )
    linear_model = self.test_data_and_model.get_sklearn_one_feature_model()
    path = os.path.join(self.tmpdir, 'my_onnx_sklearn_path')
    self.save_model(linear_model, 1, path)

    inference_runner = TestOnnxModelHandler(path)
    inference_session = ort.InferenceSession(
        path, providers=['CUDAExecutionProvider', 'CPUExecutionProvider']
    )  # this list specifies priority - prioritize gpu if cuda kernel exists
    predictions = inference_runner.run_inference(examples, inference_session)
    for actual, expected in \
        zip(predictions, expected_predictions):
      self.assertEqual(actual, expected)


@pytest.mark.uses_onnx
class OnnxPytorchRunInferencePipelineTest(OnnxTestBase):
  def exportModelToOnnx(self, model, path):
    dummy_input = torch.randn(4, 2, requires_grad=True)
    torch.onnx.export(
        model,
        dummy_input,  # model input
        path,  # where to save the model
        export_params=True,  # store the trained parameter weights
        opset_version=10,  # the ONNX version
        do_constant_folding=True,  # whether to execute constant
        # folding for optimization
        input_names=['input'],  # odel's input names
        output_names=['output'],  # model's output names
        dynamic_axes={
            'input': {
                0: 'batch_size'
            }, 'output': {
                0: 'batch_size'
            }
        })

  def test_pipeline_local_model_simple(self):
    with TestPipeline() as pipeline:
      path = os.path.join(self.tmpdir, 'my_onnx_pytorch_path')
      model = self.test_data_and_model.get_torch_two_feature_model()
      self.exportModelToOnnx(model, path)
      model_handler = TestOnnxModelHandler(path)

      pcoll = pipeline | 'start' >> beam.Create(
          self.test_data_and_model.get_two_feature_examples())
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              self.test_data_and_model.get_two_feature_predictions(),
              equals_fn=_compare_prediction_result))

  def test_model_handler_sets_env_vars(self):
    with TestPipeline() as pipeline:
      path = os.path.join(self.tmpdir, 'my_onnx_pytorch_path')
      model = self.test_data_and_model.get_torch_two_feature_model()
      self.exportModelToOnnx(model, path)
      model_handler = OnnxModelHandlerNumpy(
          model_uri=path, env_vars={'FOO': 'bar'})
      self.assertFalse('FOO' in os.environ)
      _ = (
          pipeline
          | 'start' >> beam.Create(
              self.test_data_and_model.get_two_feature_examples())
          | RunInference(model_handler))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue('bar'.equals(os.environ['FOO']))

  def test_model_handler_large_model(self):
    with TestPipeline() as pipeline:

      def onxx_numpy_inference_fn(
          inference_session: ort.InferenceSession, batch, inference_args=None):
        multi_process_shared_loaded = "multi_process_shared" in str(
            type(inference_session))
        if not multi_process_shared_loaded:
          raise Exception(
              f'Loaded model of type {type(model)}, was ' +
              'expecting multi_process_shared_model')
        return default_numpy_inference_fn(
            inference_session, batch, inference_args)

      path = os.path.join(self.tmpdir, 'my_onnx_pytorch_path')
      model = self.test_data_and_model.get_torch_two_feature_model()
      self.exportModelToOnnx(model, path)
      model_handler = OnnxModelHandlerNumpy(
          model_uri=path,
          env_vars={'FOO': 'bar'},
          inference_fn=onxx_numpy_inference_fn,
          large_model=True)
      self.assertFalse('FOO' in os.environ)
      _ = (
          pipeline
          | 'start' >> beam.Create(
              self.test_data_and_model.get_two_feature_examples())
          | RunInference(model_handler))
      pipeline.run()

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_gcs_model(self):
    with TestPipeline() as pipeline:
      examples = self.test_data_and_model.get_one_feature_samples()
      expected_predictions = (
          self.test_data_and_model.get_one_feature_predictions())
      gs_path = 'gs://apache-beam-ml/models/torch_2xplus5_onnx'
      # first need to download model from remote
      model_handler = TestOnnxModelHandler(gs_path)

      pcoll = pipeline | 'start' >> beam.Create(examples)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(expected_predictions, equals_fn=_compare_prediction_result))

  def test_invalid_input_type(self):
    with self.assertRaisesRegex(Exception, "Got invalid dimensions for input"):
      with TestPipeline() as pipeline:
        examples = [np.array([1], dtype="float32")]
        path = os.path.join(self.tmpdir, 'my_onnx_pytorch_path')
        model = self.test_data_and_model.get_torch_two_feature_model()
        self.exportModelToOnnx(model, path)

        model_handler = TestOnnxModelHandler(path)

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)


@pytest.mark.uses_onnx
class OnnxTensorflowRunInferencePipelineTest(OnnxTestBase):
  def exportModelToOnnx(self, model, path):
    spec = (tf.TensorSpec((None, 2), tf.float32, name="input"), )
    _, _ = tf2onnx.convert.from_keras(model,
    input_signature=spec, opset=13, output_path=path)

  def test_pipeline_local_model_simple(self):
    with TestPipeline() as pipeline:
      path = os.path.join(self.tmpdir, 'my_onnx_tensorflow_path')
      model = self.test_data_and_model.get_tf_two_feature_model()
      self.exportModelToOnnx(model, path)
      model_handler = TestOnnxModelHandler(path)

      pcoll = pipeline | 'start' >> beam.Create(
          self.test_data_and_model.get_two_feature_examples())
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              self.test_data_and_model.get_two_feature_predictions(),
              equals_fn=_compare_prediction_result))

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_gcs_model(self):
    with TestPipeline() as pipeline:
      examples = self.test_data_and_model.get_one_feature_samples()
      expected_predictions = (
          self.test_data_and_model.get_one_feature_predictions())
      gs_path = 'gs://apache-beam-ml/models/tf_2xplus5_onnx'

      model_handler = TestOnnxModelHandler(gs_path)

      pcoll = pipeline | 'start' >> beam.Create(examples)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(expected_predictions, equals_fn=_compare_prediction_result))

  def test_invalid_input_type(self):
    with self.assertRaisesRegex(Exception, "Got invalid dimensions for input"):
      with TestPipeline() as pipeline:
        examples = [np.array([1], dtype="float32")]
        path = os.path.join(self.tmpdir, 'my_onnx_tensorflow_path')
        model = self.test_data_and_model.get_tf_two_feature_model()
        self.exportModelToOnnx(model, path)

        model_handler = TestOnnxModelHandler(path)

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)


@pytest.mark.uses_onnx
class OnnxSklearnRunInferencePipelineTest(OnnxTestBase):
  def save_model(self, model, input_dim, path):
    # assume float input
    initial_type = [('float_input', FloatTensorType([None, input_dim]))]
    onx = convert_sklearn(model, initial_types=initial_type)
    with open(path, "wb") as f:
      f.write(onx.SerializeToString())

  def test_pipeline_local_model_simple(self):
    with TestPipeline() as pipeline:
      path = os.path.join(self.tmpdir, 'my_onnx_sklearn_path')
      model = self.test_data_and_model.get_sklearn_two_feature_model()
      self.save_model(model, 2, path)
      model_handler = TestOnnxModelHandler(path)

      pcoll = pipeline | 'start' >> beam.Create(
          self.test_data_and_model.get_two_feature_examples())
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(
              self.test_data_and_model.get_two_feature_predictions(),
              equals_fn=_compare_prediction_result))

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_gcs_model(self):
    with TestPipeline() as pipeline:
      examples = (self.test_data_and_model.get_one_feature_samples())
      expected_predictions = (
          self.test_data_and_model.get_one_feature_predictions())
      gs_path = 'gs://apache-beam-ml/models/skl_2xplus5_onnx'

      model_handler = TestOnnxModelHandler(gs_path)
      pcoll = pipeline | 'start' >> beam.Create(examples)
      predictions = pcoll | RunInference(model_handler)
      assert_that(
          predictions,
          equal_to(expected_predictions, equals_fn=_compare_prediction_result))

  def test_invalid_input_type(self):
    with self.assertRaisesRegex(Exception, "InvalidArgument"):
      with TestPipeline() as pipeline:
        examples = [np.array([1], dtype="float32")]
        path = os.path.join(self.tmpdir, 'my_onnx_sklearn_path')
        model = self.test_data_and_model.get_sklearn_two_feature_model()
        self.save_model(model, 2, path)

        model_handler = TestOnnxModelHandler(path)

        pcoll = pipeline | 'start' >> beam.Create(examples)
        # pylint: disable=expression-not-assigned
        pcoll | RunInference(model_handler)


if __name__ == '__main__':
  unittest.main()
