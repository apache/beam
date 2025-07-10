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
  from apache_beam.ml.inference.tensorrt_inference import \
      TensorRTEngineHandlerNumPy
except ImportError:
  raise unittest.SkipTest('TensorRT dependencies are not installed')

try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  GCSFileSystem = None  # type: ignore

LOGGER = trt.Logger(trt.Logger.INFO)

SINGLE_FEATURE_EXAMPLES = [
    np.array(1, dtype=np.float32),
    np.array(5, dtype=np.float32),
    np.array(-3, dtype=np.float32),
    np.array(10.0, dtype=np.float32)
]

SINGLE_FEATURE_PREDICTIONS = [
    PredictionResult(ex, pred) for ex, pred in zip(
        SINGLE_FEATURE_EXAMPLES,
        [[np.array([example * 2.0 + 0.5], dtype=np.float32)]
         for example in SINGLE_FEATURE_EXAMPLES])
]

SINGLE_FEATURE_CUSTOM_PREDICTIONS = [
    PredictionResult(ex, pred) for ex, pred in zip(
        SINGLE_FEATURE_EXAMPLES,
        [[np.array([(example * 2.0 + 0.5) * 2], dtype=np.float32)]
         for example in SINGLE_FEATURE_EXAMPLES])
]

TWO_FEATURES_EXAMPLES = [
    np.array([1, 5], dtype=np.float32),
    np.array([3, 10], dtype=np.float32),
    np.array([-14, 0], dtype=np.float32),
    np.array([0.5, 0.5], dtype=np.float32)
]

TWO_FEATURES_PREDICTIONS = [
    PredictionResult(ex, pred) for ex, pred in zip(
        TWO_FEATURES_EXAMPLES, [[
            np.array([example[0] * 2.0 + example[1] * 3 +
                      0.5], dtype=np.float32)
        ] for example in TWO_FEATURES_EXAMPLES])
]


def _compare_prediction_result(a, b):
  return ((a.example == b.example).all() and all(
      np.array_equal(actual, expected)
      for actual, expected in zip(a.inference, b.inference)))


def _assign_or_fail(args):
  """CUDA error checking."""
  from cuda import cuda
  err, ret = args[0], args[1:]
  if isinstance(err, cuda.CUresult):
    if err != cuda.CUresult.CUDA_SUCCESS:
      raise RuntimeError("Cuda Error: {}".format(err))
  else:
    raise RuntimeError("Unknown error type: {}".format(err))
  # Special case so that no unpacking is needed at call-site.
  if len(ret) == 1:
    return ret[0]
  return ret


def _custom_tensorRT_inference_fn(batch, engine, inference_args):
  from cuda import cuda
  (
      engine,
      context,
      context_lock,
      inputs,
      outputs,
      gpu_allocations,
      cpu_allocations,
      stream) = engine.get_engine_attrs()

  # Process I/O and execute the network
  with context_lock:
    _assign_or_fail(
        cuda.cuMemcpyHtoDAsync(
            inputs[0]['allocation'],
            np.ascontiguousarray(batch),
            inputs[0]['size'],
            stream))
    context.execute_async_v2(gpu_allocations, stream)
    for output in range(len(cpu_allocations)):
      _assign_or_fail(
          cuda.cuMemcpyDtoHAsync(
              cpu_allocations[output],
              outputs[output]['allocation'],
              outputs[output]['size'],
              stream))
    _assign_or_fail(cuda.cuStreamSynchronize(stream))

    return [
        PredictionResult(
            x, [prediction[idx] * 2 for prediction in cpu_allocations])
        for idx, x in enumerate(batch)
    ]


@pytest.mark.uses_tensorrt
class TensorRTRunInferenceTest(unittest.TestCase):
  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_inference_single_tensor_feature_onnx(self):
    """
    This tests ONNX parser and TensorRT engine creation from parsed ONNX
    network. Single feature tensors batched into size of 4 are used as input.
    """
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=4,
        max_batch_size=4,
        onnx_path="gs://apache-beam-ml/models/single_tensor_features_model.onnx"
    )
    network, builder = inference_runner.load_onnx()
    engine = inference_runner.build_engine(network, builder)
    predictions = inference_runner.run_inference(
        SINGLE_FEATURE_EXAMPLES, engine)
    for actual, expected in zip(predictions, SINGLE_FEATURE_PREDICTIONS):
      self.assertEqual(actual, expected)

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_inference_multiple_tensor_features_onnx(self):
    """
    This tests ONNX parser and TensorRT engine creation from parsed ONNX
    network. Two feature tensors batched into size of 4 are used as input.
    """
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=4,
        max_batch_size=4,
        onnx_path=
        'gs://apache-beam-ml/models/multiple_tensor_features_model.onnx')
    network, builder = inference_runner.load_onnx()
    engine = inference_runner.build_engine(network, builder)
    predictions = inference_runner.run_inference(TWO_FEATURES_EXAMPLES, engine)
    for actual, expected in zip(predictions, TWO_FEATURES_PREDICTIONS):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_inference_single_tensor_feature(self):
    """
    This tests creating TensorRT network from scratch. Test replicates the same
    ONNX network above but natively in TensorRT. After network creation, network
    is used to build a TensorRT engine. Single feature tensors batched into size
    of 4 are used as input.
    """
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=4, max_batch_size=4)
    builder = trt.Builder(LOGGER)
    network = builder.create_network(
        flags=1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH))
    input_tensor = network.add_input(
        name="input", dtype=trt.float32, shape=(4, 1))
    weight_const = network.add_constant(
        (1, 1), trt.Weights((np.ascontiguousarray([2.0], dtype=np.float32))))
    mm = network.add_matrix_multiply(
        input_tensor,
        trt.MatrixOperation.NONE,
        weight_const.get_output(0),
        trt.MatrixOperation.NONE)
    bias_const = network.add_constant(
        (1, 1), trt.Weights((np.ascontiguousarray([0.5], dtype=np.float32))))
    bias_add = network.add_elementwise(
        mm.get_output(0),
        bias_const.get_output(0),
        trt.ElementWiseOperation.SUM)
    bias_add.get_output(0).name = "output"
    network.mark_output(tensor=bias_add.get_output(0))

    engine = inference_runner.build_engine(network, builder)
    predictions = inference_runner.run_inference(
        SINGLE_FEATURE_EXAMPLES, engine)
    for actual, expected in zip(predictions, SINGLE_FEATURE_PREDICTIONS):
      self.assertEqual(actual, expected)

  def test_inference_custom_single_tensor_feature(self):
    """
    This tests creating TensorRT network from scratch. Test replicates the same
    ONNX network above but natively in TensorRT. After network creation, network
    is used to build a TensorRT engine. Single feature tensors batched into size
    of 4 are used as input. This routes through a custom inference function.
    """
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=4,
        max_batch_size=4,
        inference_fn=_custom_tensorRT_inference_fn)
    builder = trt.Builder(LOGGER)
    network = builder.create_network(
        flags=1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH))
    input_tensor = network.add_input(
        name="input", dtype=trt.float32, shape=(4, 1))
    weight_const = network.add_constant(
        (1, 1), trt.Weights((np.ascontiguousarray([2.0], dtype=np.float32))))
    mm = network.add_matrix_multiply(
        input_tensor,
        trt.MatrixOperation.NONE,
        weight_const.get_output(0),
        trt.MatrixOperation.NONE)
    bias_const = network.add_constant(
        (1, 1), trt.Weights((np.ascontiguousarray([0.5], dtype=np.float32))))
    bias_add = network.add_elementwise(
        mm.get_output(0),
        bias_const.get_output(0),
        trt.ElementWiseOperation.SUM)
    bias_add.get_output(0).name = "output"
    network.mark_output(tensor=bias_add.get_output(0))

    engine = inference_runner.build_engine(network, builder)
    predictions = inference_runner.run_inference(
        SINGLE_FEATURE_EXAMPLES, engine)
    for actual, expected in zip(predictions, SINGLE_FEATURE_CUSTOM_PREDICTIONS):
      self.assertEqual(actual, expected)

  def test_inference_multiple_tensor_features(self):
    """
    This tests creating TensorRT network from scratch. Test replicates the same
    ONNX network above but natively in TensorRT. After network creation, network
    is used to build a TensorRT engine. Two feature tensors batched into size of
    4 are used as input.
    """
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=4, max_batch_size=4)
    builder = trt.Builder(LOGGER)
    network = builder.create_network(
        flags=1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH))
    input_tensor = network.add_input(
        name="input", dtype=trt.float32, shape=(4, 2))
    weight_const = network.add_constant(
        (1, 2), trt.Weights((np.ascontiguousarray([2.0, 3], dtype=np.float32))))
    mm = network.add_matrix_multiply(
        input_tensor,
        trt.MatrixOperation.NONE,
        weight_const.get_output(0),
        trt.MatrixOperation.TRANSPOSE)
    bias_const = network.add_constant(
        (1, 1), trt.Weights((np.ascontiguousarray([0.5], dtype=np.float32))))
    bias_add = network.add_elementwise(
        mm.get_output(0),
        bias_const.get_output(0),
        trt.ElementWiseOperation.SUM)
    bias_add.get_output(0).name = "output"
    network.mark_output(tensor=bias_add.get_output(0))

    engine = inference_runner.build_engine(network, builder)
    predictions = inference_runner.run_inference(TWO_FEATURES_EXAMPLES, engine)
    for actual, expected in zip(predictions, TWO_FEATURES_PREDICTIONS):
      self.assertTrue(_compare_prediction_result(actual, expected))

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_inference_single_tensor_feature_built_engine(self):
    """
    This tests already pre-built TensorRT engine from ONNX network. To execute
    this test succesfully, TensorRT engine that is used here, must have been
    built in the same environment with the same GPU that will be used when
    running a test. In other words, using the same environment and same GPU we
    must pre-build the engine and after we run this test. Otherwise behavior
    might be unpredictable, read more:
    https://docs.nvidia.com/deeplearning/tensorrt/developer-guide/index.html#compatibility-serialized-engines
    Single feature tensors batched into size of 4 are used as input.
    """
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=4,
        max_batch_size=4,
        engine_path=
        'gs://apache-beam-ml/models/single_tensor_features_engine.trt')
    engine = inference_runner.load_model()
    predictions = inference_runner.run_inference(
        SINGLE_FEATURE_EXAMPLES, engine)
    for actual, expected in zip(predictions, SINGLE_FEATURE_PREDICTIONS):
      self.assertEqual(actual, expected)

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_inference_multiple_tensor_feature_built_engine(self):
    """
    This tests already pre-built TensorRT engine from ONNX network. To execute
    this test succesfully, TensorRT engine that is used here, must have been
    built in the same environment with the same GPU that will be used when
    running a test. In other words, using the same environment and same GPU we
    must pre-build the engine and after we run this test. Otherwise behavior
    might be unpredictable, read more:
    https://docs.nvidia.com/deeplearning/tensorrt/developer-guide/index.html#compatibility-serialized-engines
    Two feature tensors batched into size of 4 are used as input.
    """
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=4,
        max_batch_size=4,
        engine_path=
        'gs://apache-beam-ml/models/multiple_tensor_features_engine.trt')
    engine = inference_runner.load_model()
    predictions = inference_runner.run_inference(TWO_FEATURES_EXAMPLES, engine)
    for actual, expected in zip(predictions, TWO_FEATURES_PREDICTIONS):
      self.assertTrue(_compare_prediction_result(actual, expected))

  def test_num_bytes(self):
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=1, max_batch_size=1)
    examples = [
        np.array([1, 5], dtype=np.float32),
        np.array([3, 10], dtype=np.float32),
        np.array([-14, 0], dtype=np.float32),
        np.array([0.5, 0.5], dtype=np.float32)
    ]
    self.assertEqual((examples[0].itemsize) * 4,
                     inference_runner.get_num_bytes(examples))

  def test_namespace(self):
    inference_runner = TensorRTEngineHandlerNumPy(
        min_batch_size=4, max_batch_size=4)
    self.assertEqual(
        'RunInferenceTensorRT', inference_runner.get_metrics_namespace())


@pytest.mark.uses_tensorrt
class TensorRTRunInferencePipelineTest(unittest.TestCase):
  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_single_tensor_feature_built_engine(self):
    with TestPipeline() as pipeline:
      engine_handler = TensorRTEngineHandlerNumPy(
          min_batch_size=4,
          max_batch_size=4,
          engine_path=
          'gs://apache-beam-ml/models/single_tensor_features_engine.trt')
      pcoll = pipeline | 'start' >> beam.Create(SINGLE_FEATURE_EXAMPLES)
      predictions = pcoll | RunInference(engine_handler)
      assert_that(
          predictions,
          equal_to(
              SINGLE_FEATURE_PREDICTIONS, equals_fn=_compare_prediction_result))

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_single_tensor_feature_built_engine_large_engine(self):
    with TestPipeline() as pipeline:

      def fake_inference_fn(batch, engine, inference_args=None):
        multi_process_shared_loaded = "multi_process_shared" in str(
            type(engine))
        if not multi_process_shared_loaded:
          raise Exception(
              f'Loaded engine of type {type(engine)}, was ' +
              'expecting multi_process_shared engine')
        from cuda import cuda
        (
            engine,
            context,
            context_lock,
            inputs,
            outputs,
            gpu_allocations,
            cpu_allocations,
            stream) = engine.get_engine_attrs()

        # Process I/O and execute the network
        with context_lock:
          _assign_or_fail(
              cuda.cuMemcpyHtoDAsync(
                  inputs[0]['allocation'],
                  np.ascontiguousarray(batch),
                  inputs[0]['size'],
                  stream))
          context.execute_async_v2(gpu_allocations, stream)
          for output in range(len(cpu_allocations)):
            _assign_or_fail(
                cuda.cuMemcpyDtoHAsync(
                    cpu_allocations[output],
                    outputs[output]['allocation'],
                    outputs[output]['size'],
                    stream))
          _assign_or_fail(cuda.cuStreamSynchronize(stream))

          predictions = []
          for idx in range(len(batch)):
            predictions.append(
                [prediction[idx] for prediction in cpu_allocations])

          return utils._convert_to_result(batch, predictions)

      engine_handler = TensorRTEngineHandlerNumPy(
          min_batch_size=4,
          max_batch_size=4,
          engine_path=
          'gs://apache-beam-ml/models/single_tensor_features_engine.trt',
          inference_fn=fake_inference_fn,
          large_model=True)
      pcoll = pipeline | 'start' >> beam.Create(SINGLE_FEATURE_EXAMPLES)
      predictions = pcoll | RunInference(engine_handler)
      assert_that(
          predictions,
          equal_to(
              SINGLE_FEATURE_PREDICTIONS, equals_fn=_compare_prediction_result))

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_sets_env_vars_correctly(self):
    with TestPipeline() as pipeline:
      engine_handler = TensorRTEngineHandlerNumPy(
          env_vars={'FOO': 'bar'},
          min_batch_size=4,
          max_batch_size=4,
          engine_path=
          'gs://apache-beam-ml/models/single_tensor_features_engine.trt')
      os.environ.pop('FOO', None)
      self.assertFalse('FOO' in os.environ)
      _ = (
          pipeline
          | 'start' >> beam.Create(SINGLE_FEATURE_EXAMPLES)
          | RunInference(engine_handler))
      pipeline.run()
      self.assertTrue('FOO' in os.environ)
      self.assertTrue((os.environ['FOO']) == 'bar')

  @unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
  def test_pipeline_multiple_tensor_feature_built_engine(self):
    with TestPipeline() as pipeline:
      engine_handler = TensorRTEngineHandlerNumPy(
          min_batch_size=4,
          max_batch_size=4,
          engine_path=
          'gs://apache-beam-ml/models/multiple_tensor_features_engine.trt')
      pcoll = pipeline | 'start' >> beam.Create(TWO_FEATURES_EXAMPLES)
      predictions = pcoll | RunInference(engine_handler)
      assert_that(
          predictions,
          equal_to(
              TWO_FEATURES_PREDICTIONS, equals_fn=_compare_prediction_result))


if __name__ == '__main__':
  unittest.main()
