#
# SPDX-FileCopyrightText: NVIDIA CORPORATION & AFFILIATES
# Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

import numpy as np
import pycuda.autoinit  # pylint: disable=unused-import
import pycuda.driver as cuda
import sys
import tensorrt as trt
from typing import Any, Dict, Iterable, Optional, Sequence

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ModelHandler, PredictionResult

LOGGER = trt.Logger(trt.Logger.INFO)


def _load_engine(engine_path):
  file = FileSystems.open(engine_path, 'rb')
  runtime = trt.Runtime(LOGGER)
  engine = runtime.deserialize_cuda_engine(file.read())
  assert engine
  return engine


def _load_onnx(onnx_path):
  builder = trt.Builder(LOGGER)
  network = builder.create_network(
      flags=1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH))
  parser = trt.OnnxParser(network, LOGGER)
  with FileSystems.open(onnx_path) as f:
    if not parser.parse(f.read()):
      print("Failed to load ONNX file: {}".format(onnx_path))
      for error in range(parser.num_errors):
        print(parser.get_error(error))
      sys.exit(1)
  builder.reset()
  return network


def _build_engine(network):
  builder = trt.Builder(LOGGER)
  config = builder.create_builder_config()
  runtime = trt.Runtime(LOGGER)
  plan = builder.build_serialized_network(network, config)
  engine = runtime.deserialize_cuda_engine(plan)
  builder.reset()
  return engine


def _validate_inference_args(inference_args):
  """Confirms that inference_args is None.

  TensorRT engines do not need extra arguments in their execute_v2() call.
  However, since inference_args is an argument in the RunInference interface,
  we want to make sure it is not passed here in TensorRT's implementation of
  RunInference.
  """
  if inference_args:
    raise ValueError(
        'inference_args were provided, but should be None because TensorRT '
        'engines do not need extra arguments in their execute_v2() call.')


class TensorRTEngineHandlerNumPy(ModelHandler[np.ndarray,
                                              PredictionResult,
                                              trt.ICudaEngine]):
  def __init__(self, min_batch_size: int, max_batch_size: int, **kwargs):
    """Implementation of the ModelHandler interface for TensorRT.

    Example Usage:
      pcoll | RunInference(
        TensorRTEngineHandlerNumPy(
          min_batch_size=1,
          max_batch_size=1,
          engine_path="my_uri"))

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

    trt.init_libnvinfer_plugins(LOGGER, namespace="")

  def batch_elements_kwargs(self):
    """Sets min_batch_size and max_batch_size of a TensorRT engine."""
    return {
        'min_batch_size': self.min_batch_size,
        'max_batch_size': self.max_batch_size
    }

  def load_model(self) -> trt.ICudaEngine:
    """Loads and initializes a TensorRT engine for processing."""
    return _load_engine(self.engine_path)

  def load_onnx(self) -> trt.INetworkDefinition:
    """Loads and parses an onnx model for processing."""
    return _load_onnx(self.onnx_path)

  #
  def build_engine(self, network: trt.INetworkDefinition) -> trt.ICudaEngine:
    """Build an engine according to parsed/created network."""
    return _build_engine(network)

  def run_inference(
      self,
      batch: np.ndarray,
      engine: trt.ICudaEngine,
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
    _validate_inference_args(inference_args)
    context = engine.create_execution_context()
    assert context
    # Setup I/O bindings
    inputs = []
    outputs = []
    allocations = []
    for i in range(engine.num_bindings):
      is_input = False
      if engine.binding_is_input(i):
        is_input = True
      name = engine.get_binding_name(i)
      dtype = engine.get_binding_dtype(i)
      shape = engine.get_binding_shape(i)
      if is_input:
        batch_size = shape[0]
      size = np.dtype(trt.nptype(dtype)).itemsize
      for s in shape:
        size *= s
      allocation = cuda.mem_alloc(size)
      binding = {
          'index': i,
          'name': name,
          'dtype': np.dtype(trt.nptype(dtype)),
          'shape': list(shape),
          'allocation': allocation,
      }
      allocations.append(allocation)
      if engine.binding_is_input(i):
        inputs.append(binding)
      else:
        outputs.append(binding)

    assert batch_size > 0
    assert len(inputs) > 0
    assert len(outputs) > 0
    assert len(allocations) > 0
    # Prepare the output data
    predictions = []
    for output in outputs:
      predictions.append(np.zeros(output['shape'], output['dtype']))
    # Process I/O and execute the network
    cuda.memcpy_htod(inputs[0]['allocation'], np.ascontiguousarray(batch))
    context.execute_v2(allocations)
    for output in range(len(predictions)):
      cuda.memcpy_dtoh(predictions[output], outputs[output]['allocation'])
    return [
        PredictionResult(x, [prediction[idx] for prediction in predictions])
        for idx,
        x in enumerate(batch)
    ]

  def get_num_bytes(self, batch: Sequence[np.ndarray]) -> int:
    """
    Returns:
      The number of bytes of data for a batch of Tensors.
    """
    return sum((el.itemsize for np_array in batch for el in np_array))

  def get_metrics_namespace(self) -> str:
    """
    Returns a namespace for metrics collected by the RunInference transform.
    """
    return 'RunInferenceTensorRT'
