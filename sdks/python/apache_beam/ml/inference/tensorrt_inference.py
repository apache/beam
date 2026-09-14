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

from __future__ import annotations

import functools
import logging
import threading
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional

import numpy as np

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

LOGGER = logging.getLogger("TensorRTEngineHandlerNumPy")
# This try/catch block allows users to submit jobs from a machine without
# GPU and other dependencies (tensorrt, cuda, etc.) at job submission time.
try:
  import tensorrt as trt
  TRT_LOGGER = trt.Logger(trt.Logger.INFO)
  trt.init_libnvinfer_plugins(TRT_LOGGER, namespace="")
  LOGGER.info('tensorrt module successfully imported.')
except ModuleNotFoundError:
  TRT_LOGGER = None
  msg = 'tensorrt module was not found. This is ok as long as the specified ' \
    'runner has tensorrt dependencies installed.'
  LOGGER.warning(msg)

MIN_TRT_MAJOR_VERSION = 10


@functools.lru_cache(maxsize=1)
def _check_trt_version() -> None:
  """Fails fast if the installed TensorRT is older than we support.

  TensorRT 10 removed the index based "binding" API this module used to be
  written against. Without this check the failure surfaces as an obscure
  AttributeError deep inside engine setup.

  Cached rather than checked at import time because the module is importable
  without TensorRT, so that jobs can be submitted from a machine that does not
  have it installed.
  """
  import tensorrt as trt
  try:
    major = int(trt.__version__.split('.')[0])
  except (AttributeError, IndexError, ValueError):
    # Fall back to probing for an attribute that only exists from 10 onwards.
    major = 10 if hasattr(trt.ICudaEngine, 'num_io_tensors') else 8
  if major < MIN_TRT_MAJOR_VERSION:
    raise RuntimeError(
        'RunInference requires TensorRT %d or later, but found %s. Support '
        'for TensorRT 8.x was removed because TensorRT 10 replaced the '
        'engine binding API this handler depends on.' %
        (MIN_TRT_MAJOR_VERSION, getattr(trt, '__version__', 'unknown')))


@functools.lru_cache(maxsize=1)
def _import_cuda_driver():
  """Imports the CUDA driver bindings.

  ``cuda.bindings.driver`` is the module path used by cuda-python 12.8 and
  later. It replaced the ``cuda.cuda`` alias, which was removed in
  cuda-python 13.0, so only fall back to that for older installations.

  Cached because this is called from _assign_or_fail, which runs on every
  CUDA call.
  """
  try:
    from cuda.bindings import driver as cuda
  except ImportError:
    from cuda import cuda
  return cuda


def _load_engine(engine_path):
  import tensorrt as trt
  file = FileSystems.open(engine_path, 'rb')
  runtime = trt.Runtime(TRT_LOGGER)
  engine = runtime.deserialize_cuda_engine(file.read())
  assert engine
  return engine


def _load_onnx(onnx_path):
  import tensorrt as trt
  _check_trt_version()
  builder = trt.Builder(TRT_LOGGER)
  # Explicit batch is the only supported mode from TensorRT 10 onwards, so no
  # network creation flags are needed.
  network = builder.create_network()
  parser = trt.OnnxParser(network, TRT_LOGGER)
  with FileSystems.open(onnx_path) as f:
    if not parser.parse(f.read()):
      LOGGER.error("Failed to load ONNX file: %s", onnx_path)
      for error in range(parser.num_errors):
        LOGGER.error(parser.get_error(error))
      raise ValueError(f"Failed to load ONNX file: {onnx_path}")
  return network, builder


def _build_engine(network, builder):
  import tensorrt as trt
  config = builder.create_builder_config()
  runtime = trt.Runtime(TRT_LOGGER)
  plan = builder.build_serialized_network(network, config)
  engine = runtime.deserialize_cuda_engine(plan)
  builder.reset()
  return engine


def _assign_or_fail(args):
  """CUDA error checking."""
  cuda = _import_cuda_driver()
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


class TensorRTEngine:
  def __init__(self, engine: trt.ICudaEngine):
    """Implementation of the TensorRTEngine class which handles
    allocations associated with TensorRT engine.

    Example Usage::

      TensorRTEngine(engine)

    Args:
      engine: trt.ICudaEngine object that contains TensorRT engine
    """
    import tensorrt as trt
    _check_trt_version()
    cuda = _import_cuda_driver()
    self.engine = engine
    self.context = engine.create_execution_context()
    self.context_lock = threading.RLock()
    self.inputs = []
    self.outputs = []
    self.gpu_allocations = []
    self.cpu_allocations = []

    # Setup I/O tensors. Device addresses are bound to the context once here
    # because execute_async_v3 takes no allocation list at execution time.
    for i in range(self.engine.num_io_tensors):
      name = self.engine.get_tensor_name(i)
      dtype = self.engine.get_tensor_dtype(name)
      shape = self.engine.get_tensor_shape(name)
      size = trt.volume(shape) * dtype.itemsize
      allocation = _assign_or_fail(cuda.cuMemAlloc(size))
      binding = {
          'index': i,
          'name': name,
          'dtype': np.dtype(trt.nptype(dtype)),
          'shape': list(shape),
          'allocation': allocation,
          'size': size
      }
      self.gpu_allocations.append(allocation)
      self.context.set_tensor_address(name, int(allocation))
      if self.engine.get_tensor_mode(name) == trt.TensorIOMode.INPUT:
        self.inputs.append(binding)
      else:
        self.outputs.append(binding)

    assert self.context
    assert len(self.inputs) > 0
    assert len(self.outputs) > 0
    assert len(self.gpu_allocations) > 0

    for output in self.outputs:
      self.cpu_allocations.append(np.zeros(output['shape'], output['dtype']))
    # Create CUDA Stream.
    self.stream = _assign_or_fail(cuda.cuStreamCreate(0))

  def get_engine_attrs(self):
    """Returns TensorRT engine attributes."""
    return (
        self.engine,
        self.context,
        self.context_lock,
        self.inputs,
        self.outputs,
        self.gpu_allocations,
        self.cpu_allocations,
        self.stream)


TensorRTInferenceFn = Callable[
    [Sequence[np.ndarray], TensorRTEngine, Optional[dict[str, Any]]],
    Iterable[PredictionResult]]


def _default_tensorRT_inference_fn(
    batch: Sequence[np.ndarray],
    engine: TensorRTEngine,
    inference_args: Optional[dict[str,
                                  Any]] = None) -> Iterable[PredictionResult]:
  cuda = _import_cuda_driver()
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
    # Host buffers are passed as explicit addresses rather than as arrays.
    # A numpy array holding exactly one element is coerced to a scalar, which
    # is then read as a null host pointer and fails with CUDA_ERROR_INVALID_
    # VALUE. Single element outputs are common, for example the num_detections
    # output of an object detection model.
    # host_input must stay referenced until the stream is synchronized below,
    # because the copy is asynchronous.
    host_input = np.ascontiguousarray(batch)
    _assign_or_fail(
        cuda.cuMemcpyHtoDAsync(
            inputs[0]['allocation'],
            host_input.ctypes.data,
            inputs[0]['size'],
            stream))
    # Tensor addresses were bound when the engine was created.
    context.execute_async_v3(stream)
    for output in range(len(cpu_allocations)):
      _assign_or_fail(
          cuda.cuMemcpyDtoHAsync(
              cpu_allocations[output].ctypes.data,
              outputs[output]['allocation'],
              outputs[output]['size'],
              stream))
    _assign_or_fail(cuda.cuStreamSynchronize(stream))

    predictions = []
    for idx in range(len(batch)):
      predictions.append([prediction[idx] for prediction in cpu_allocations])

    return utils._convert_to_result(batch, predictions)


class TensorRTEngineHandlerNumPy(ModelHandler[np.ndarray,
                                              PredictionResult,
                                              TensorRTEngine]):
  def __init__(
      self,
      min_batch_size: int,
      max_batch_size: int,
      *,
      inference_fn: TensorRTInferenceFn = _default_tensorRT_inference_fn,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      batch_length_fn: Optional[Callable[[Any], int]] = None,
      batch_bucket_boundaries: Optional[list[int]] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for TensorRT.

    Example Usage::

      pcoll | RunInference(
          TensorRTEngineHandlerNumPy(
            min_batch_size=1,
            max_batch_size=1,
            engine_path="my_uri"))

    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.

    Args:
      min_batch_size: minimum accepted batch size.
      max_batch_size: maximum accepted batch size.
      inference_fn: the inference function to use on RunInference calls.
        default: _default_tensorRT_inference_fn
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      max_batch_duration_secs: the maximum amount of time to buffer
        a batch before emitting; used in streaming contexts.
      max_batch_weight: the maximum total weight of a batch.
      element_size_fn: a function that returns the size (weight) of an element.
      batch_length_fn: a callable that returns the length of an element for
        length-aware batching.
      batch_bucket_boundaries: a sorted list of positive boundary values for
        length-aware batching buckets.
      kwargs: Additional arguments like 'engine_path' and 'onnx_path' are
        currently supported. 'env_vars' can be used to set environment variables
        before loading the model.

    See https://docs.nvidia.com/deeplearning/tensorrt/api/python_api/
    for details
    """
    super().__init__(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        max_batch_weight=max_batch_weight,
        element_size_fn=element_size_fn,
        batch_length_fn=batch_length_fn,
        batch_bucket_boundaries=batch_bucket_boundaries,
        large_model=large_model,
        model_copies=model_copies,
        **kwargs)
    self.inference_fn = inference_fn
    if 'engine_path' in kwargs:
      self.engine_path = kwargs.get('engine_path')
    elif 'onnx_path' in kwargs:
      self.onnx_path = kwargs.get('onnx_path')

  def load_model(self) -> TensorRTEngine:
    """Loads and initializes a TensorRT engine for processing."""
    engine = _load_engine(self.engine_path)
    return TensorRTEngine(engine)

  def load_onnx(self) -> tuple[trt.INetworkDefinition, trt.Builder]:
    """Loads and parses an onnx model for processing."""
    return _load_onnx(self.onnx_path)

  def build_engine(
      self, network: trt.INetworkDefinition,
      builder: trt.Builder) -> TensorRTEngine:
    """Build an engine according to parsed/created network."""
    engine = _build_engine(network, builder)
    return TensorRTEngine(engine)

  def run_inference(
      self,
      batch: Sequence[np.ndarray],
      engine: TensorRTEngine,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Tensors and returns an Iterable of
    TensorRT Predictions.

    Args:
      batch: A np.ndarray or a np.ndarray that represents a concatenation
        of multiple arrays as a batch.
      engine: A TensorRT engine.
      inference_args: Any additional arguments for an inference
        that are not applicable to TensorRT.

    Returns:
      An Iterable of type PredictionResult.
    """
    return self.inference_fn(batch, engine, inference_args)

  def get_num_bytes(self, batch: Sequence[np.ndarray]) -> int:
    """
    Returns:
      The number of bytes of data for a batch of Tensors.
    """
    return sum((np_array.itemsize for np_array in batch))

  def get_metrics_namespace(self) -> str:
    """
    Returns a namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_TensorRT'

  def validate_inference_args(self, inference_args: Optional[dict[str, Any]]):
    """
    Currently, this model handler does not support inference args. Given that,
    we will throw if any are passed in.
    """
    if inference_args:
      raise ValueError(
          'inference_args were provided, but should be None because this '
          'framework does not expect extra arguments on inferences.')
