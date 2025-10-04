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
import tempfile
import unittest

import numpy as np
import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where TensorRT python library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import tensorrt as trt
  from apache_beam.ml.inference import utils
  from apache_beam.ml.inference.base import PredictionResult, RunInference
  from apache_beam.ml.inference.trt_handler_numpy_compact import (
      TensorRTEngine,
      TensorRTEngineHandlerNumPy,
  )
except ImportError:
  raise unittest.SkipTest('TensorRT 10.x dependencies are not installed')

try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  GCSFileSystem = None  # type: ignore


# Test data
SINGLE_FEATURE_EXAMPLES = np.array(
    [[1.0], [5.0], [-3.0], [10.0]], dtype=np.float32
)

SINGLE_FEATURE_PREDICTIONS = [
    PredictionResult(
        SINGLE_FEATURE_EXAMPLES[i],
        [np.array([SINGLE_FEATURE_EXAMPLES[i][0] * 2.0 + 0.5], dtype=np.float32)]
    ) for i in range(len(SINGLE_FEATURE_EXAMPLES))
]

TWO_FEATURES_EXAMPLES = np.array(
    [[1, 5], [3, 10], [-14, 0], [0.5, 0.5]], dtype=np.float32
)

TWO_FEATURES_PREDICTIONS = [
    PredictionResult(
        TWO_FEATURES_EXAMPLES[i],
        [np.array([TWO_FEATURES_EXAMPLES[i][0] * 2.0 + TWO_FEATURES_EXAMPLES[i][1] * 3 + 0.5], dtype=np.float32)]
    ) for i in range(len(TWO_FEATURES_EXAMPLES))
]


def _compare_prediction_result(a, b):
  """Compare two PredictionResult objects."""
  return ((a.example == b.example).all() and all(
      np.array_equal(actual, expected)
      for actual, expected in zip(a.inference, b.inference)))


def _build_simple_onnx_model(input_size=1, output_path=None):
  """Build a simple ONNX model for testing: y = 2x + 0.5"""
  try:
    import onnx
    from onnx import helper, TensorProto
  except ImportError:
    raise unittest.SkipTest('ONNX dependencies are not installed')

  # Create a simple linear model: y = 2*x + 0.5
  input_tensor = helper.make_tensor_value_info(
      'input', TensorProto.FLOAT, [None, input_size]
  )
  output_tensor = helper.make_tensor_value_info(
      'output', TensorProto.FLOAT, [None, input_size]
  )

  # Weight tensor: 2.0
  weight_init = helper.make_tensor(
      'weight', TensorProto.FLOAT, [input_size, input_size],
      [2.0] * (input_size * input_size)
  )
  # Bias tensor: 0.5
  bias_init = helper.make_tensor(
      'bias', TensorProto.FLOAT, [input_size], [0.5] * input_size
  )

  # MatMul node
  matmul_node = helper.make_node('MatMul', ['input', 'weight'], ['matmul_out'])
  # Add node
  add_node = helper.make_node('Add', ['matmul_out', 'bias'], ['output'])

  # Create graph
  graph = helper.make_graph(
      [matmul_node, add_node],
      'simple_linear',
      [input_tensor],
      [output_tensor],
      [weight_init, bias_init]
  )

  # Create model
  model = helper.make_model(graph, producer_name='trt_test')
  model.opset_import[0].version = 13

  if output_path:
    with open(output_path, 'wb') as f:
      f.write(model.SerializeToString())

  return model


@pytest.mark.uses_tensorrt
class TensorRTEngineHandlerNumPyTest(unittest.TestCase):
  """Tests for TensorRTEngineHandlerNumPy with TensorRT 10.x Tensor API."""

  def test_handler_initialization(self):
    """Test that handler initializes correctly with required parameters."""
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=1,
        max_batch_size=4,
        engine_path='/tmp/test.engine'
    )
    self.assertEqual(handler.min_batch_size, 1)
    self.assertEqual(handler.max_batch_size, 4)
    self.assertEqual(handler.engine_path, '/tmp/test.engine')

  def test_handler_initialization_both_paths_error(self):
    """Test that providing both engine_path and onnx_path raises an error."""
    with self.assertRaises(ValueError):
      TensorRTEngineHandlerNumPy(
          min_batch_size=1,
          max_batch_size=4,
          engine_path='/tmp/test.engine',
          onnx_path='/tmp/test.onnx'
      )

  def test_handler_initialization_no_path_error(self):
    """Test that providing neither engine_path nor onnx_path raises an error."""
    with self.assertRaises(ValueError):
      TensorRTEngineHandlerNumPy(
          min_batch_size=1,
          max_batch_size=4
      )

  def test_handler_initialization_invalid_engine_path(self):
    """Test that providing a non-.engine path raises an error."""
    with self.assertRaises(ValueError):
      TensorRTEngineHandlerNumPy(
          min_batch_size=1,
          max_batch_size=4,
          engine_path='/tmp/test.onnx'
      )

  def test_handler_initialization_invalid_onnx_path(self):
    """Test that providing a .engine path as onnx_path raises an error."""
    with self.assertRaises(ValueError):
      TensorRTEngineHandlerNumPy(
          min_batch_size=1,
          max_batch_size=4,
          onnx_path='/tmp/test.engine'
      )

  def test_batch_elements_kwargs(self):
    """Test that batch_elements_kwargs returns correct values."""
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=2,
        max_batch_size=8,
        max_batch_duration_secs=10,
        engine_path='/tmp/test.engine'
    )
    kwargs = handler.batch_elements_kwargs()
    self.assertEqual(kwargs['min_batch_size'], 2)
    self.assertEqual(kwargs['max_batch_size'], 8)
    self.assertEqual(kwargs['max_batch_duration_secs'], 10)

  def test_get_num_bytes_ndarray(self):
    """Test get_num_bytes with numpy ndarray."""
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=1,
        max_batch_size=4,
        engine_path='/tmp/test.engine'
    )
    batch = np.array([[1, 2], [3, 4]], dtype=np.float32)
    num_bytes = handler.get_num_bytes(batch)
    self.assertEqual(num_bytes, batch.nbytes)

  def test_get_num_bytes_list(self):
    """Test get_num_bytes with list of ndarrays."""
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=1,
        max_batch_size=4,
        engine_path='/tmp/test.engine'
    )
    batch = [
        np.array([1, 2], dtype=np.float32),
        np.array([3, 4], dtype=np.float32)
    ]
    num_bytes = handler.get_num_bytes(batch)
    expected = sum(a.nbytes for a in batch)
    self.assertEqual(num_bytes, expected)

  def test_get_metrics_namespace(self):
    """Test that metrics namespace is correct."""
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=1,
        max_batch_size=4,
        engine_path='/tmp/test.engine'
    )
    self.assertEqual(handler.get_metrics_namespace(), 'BeamML_TensorRT10')

  def test_share_model_across_processes(self):
    """Test share_model_across_processes flag."""
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=1,
        max_batch_size=4,
        engine_path='/tmp/test.engine',
        large_model=True
    )
    self.assertTrue(handler.share_model_across_processes())

  def test_model_copies(self):
    """Test model_copies parameter."""
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=1,
        max_batch_size=4,
        engine_path='/tmp/test.engine',
        model_copies=3
    )
    self.assertEqual(handler.model_copies(), 3)

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_inference_with_onnx_build_on_worker(self):
    """Test loading ONNX and building engine on worker."""
    with tempfile.TemporaryDirectory() as tmpdir:
      onnx_path = os.path.join(tmpdir, 'test_model.onnx')
      _build_simple_onnx_model(input_size=1, output_path=onnx_path)

      handler = TensorRTEngineHandlerNumPy(
          min_batch_size=1,
          max_batch_size=4,
          onnx_path=onnx_path,
          build_on_worker=True
      )

      # Load model
      engine = handler.load_model()
      self.assertIsInstance(engine, TensorRTEngine)

      # Run inference
      batch = np.array([[1.0], [2.0], [3.0], [4.0]], dtype=np.float32)
      predictions = list(handler.run_inference(batch, engine))

      # Verify predictions
      self.assertEqual(len(predictions), 4)
      for i, pred in enumerate(predictions):
        expected = batch[i][0] * 2.0 + 0.5
        np.testing.assert_allclose(
            pred.inference[0], [expected], rtol=1e-5
        )

  def test_env_vars_setting(self):
    """Test that environment variables are set correctly."""
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=1,
        max_batch_size=4,
        engine_path='/tmp/test.engine',
        env_vars={'TEST_VAR': 'test_value'}
    )

    # Remove the variable if it exists
    os.environ.pop('TEST_VAR', None)
    self.assertFalse('TEST_VAR' in os.environ)

    # This would normally be tested during load_model, but we'll just verify
    # the env_vars are stored
    self.assertEqual(handler._env_vars, {'TEST_VAR': 'test_value'})


@pytest.mark.uses_tensorrt
class TensorRTEngineTest(unittest.TestCase):
  """Tests for TensorRTEngine wrapper class."""

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_engine_initialization(self):
    """Test that TensorRTEngine initializes correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
      onnx_path = os.path.join(tmpdir, 'test_model.onnx')
      _build_simple_onnx_model(input_size=1, output_path=onnx_path)

      handler = TensorRTEngineHandlerNumPy(
          min_batch_size=1,
          max_batch_size=4,
          onnx_path=onnx_path,
          build_on_worker=True
      )

      engine = handler.load_model()

      # Verify engine attributes
      self.assertIsNotNone(engine.engine)
      self.assertIsNotNone(engine.context)
      self.assertIsNotNone(engine.context_lock)
      self.assertIsInstance(engine.input_names, list)
      self.assertIsInstance(engine.output_names, list)
      self.assertGreater(len(engine.input_names), 0)
      self.assertGreater(len(engine.output_names), 0)


@pytest.mark.uses_tensorrt
class TensorRTRunInferencePipelineTest(unittest.TestCase):
  """Integration tests for TensorRT handler in Beam pipelines."""

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_with_onnx_model(self):
    """Test full pipeline with ONNX model built on worker."""
    with tempfile.TemporaryDirectory() as tmpdir:
      onnx_path = os.path.join(tmpdir, 'test_model.onnx')
      _build_simple_onnx_model(input_size=1, output_path=onnx_path)

      with TestPipeline() as pipeline:
        handler = TensorRTEngineHandlerNumPy(
            min_batch_size=4,
            max_batch_size=4,
            onnx_path=onnx_path,
            build_on_worker=True
        )

        examples = [
            np.array([[1.0]], dtype=np.float32),
            np.array([[2.0]], dtype=np.float32),
            np.array([[3.0]], dtype=np.float32),
            np.array([[4.0]], dtype=np.float32),
        ]

        pcoll = pipeline | 'Create' >> beam.Create(examples)
        predictions = pcoll | 'RunInference' >> RunInference(handler)

        def check_predictions(predictions):
          self.assertEqual(len(predictions), 4)
          for i, pred in enumerate(predictions):
            expected = examples[i][0][0] * 2.0 + 0.5
            np.testing.assert_allclose(
                pred.inference[0], [expected], rtol=1e-5
            )

        assert_that(predictions, check_predictions)


if __name__ == '__main__':
  unittest.main()
