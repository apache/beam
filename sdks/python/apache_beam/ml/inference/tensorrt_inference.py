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

import logging
import threading
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Tuple

import numpy as np

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils.annotations import experimental

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


def _load_engine(engine_path):
  import tensorrt as trt
  file = FileSystems.open(engine_path, 'rb')
  runtime = trt.Runtime(TRT_LOGGER)
  engine = runtime.deserialize_cuda_engine(file.read())
  assert engine
  return engine


def _load_onnx(onnx_path):
  import tensorrt as trt
  builder = trt.Builder(TRT_LOGGER)
  network = builder.create_network(
      flags=1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH))
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


class TensorRTEngine:
  def __init__(self, engine: trt.ICudaEngine):
    """Implementation of the TensorRTEngine class which handles
    allocations associated with TensorRT engine.

    Example Usage::

      TensorRTEngine(engine)

    Args:
      engine: trt.ICudaEngine object that contains TensorRT engine
    """
    from cuda import cuda
    import tensorrt as trt
    self.engine = engine
    self.context = engine.create_execution_context()
    self.context_lock = threading.RLock()
    self.inputs = []
    self.outputs = []
    self.gpu_allocations = []
    self.cpu_allocations = []
    """Setup I/O bindings."""
    for i in range(self.engine.num_bindings):
      name = self.engine.get_binding_name(i)
      dtype = self.engine.get_binding_dtype(i)
      shape = self.engine.get_binding_shape(i)
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
      if self.engine.binding_is_input(i):
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


@experimental(extra_message="No backwards-compatibility guarantees.")
class TensorRTEngineHandlerNumPy(ModelHandler[np.ndarray,
                                              PredictionResult,
                                              TensorRTEngine]):
  def __init__(self, min_batch_size: int, max_batch_size: int, **kwargs):
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
      kwargs: Additional arguments like 'engine_path' and 'onnx_path' are
        currently supported.

    See https://docs.nvidia.com/deeplearning/tensorrt/api/python_api/
    for details
    """
    self.min_batch_size = min_batch_size
    self.max_batch_size = max_batch_size
    if 'engine_path' in kwargs:
      self.engine_path = kwargs.get('engine_path')
    elif 'onnx_path' in kwargs:
      self.onnx_path = kwargs.get('onnx_path')

  def batch_elements_kwargs(self):
    """Sets min_batch_size and max_batch_size of a TensorRT engine."""
    return {
        'min_batch_size': self.min_batch_size,
        'max_batch_size': self.max_batch_size
    }

  def load_model(self) -> TensorRTEngine:
    """Loads and initializes a TensorRT engine for processing."""
    engine = _load_engine(self.engine_path)
    return TensorRTEngine(engine)

  def load_onnx(self) -> Tuple[trt.INetworkDefinition, trt.Builder]:
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
      inference_args: Optional[Dict[str, Any]] = None
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
              x, [prediction[idx] for prediction in cpu_allocations]) for idx,
          x in enumerate(batch)
      ]

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
    return 'RunInferenceTensorRT'
