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
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

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


def _load_engine(engine_path):
  import tensorrt as trt
  file = FileSystems.open(engine_path, 'rb')
  runtime = trt.Runtime(TRT_LOGGER)
  engine = runtime.deserialize_cuda_engine(file.read())
  assert engine
  return engine


def _tensorrt_supports_tensor_api() -> bool:
  try:
    import tensorrt as trt  # noqa: F401
  except Exception:
    return False

  engine_reqs = ("num_io_tensors", "get_tensor_name")
  ctx_reqs = ("execute_async_v3", "set_input_shape", "set_tensor_address")
  return all(hasattr(trt.ICudaEngine, m) for m in engine_reqs) and all(
      hasattr(trt.IExecutionContext, m) for m in ctx_reqs)


def _require_tensorrt_10() -> None:
  if not _tensorrt_supports_tensor_api():
    raise RuntimeError(
        "TensorRT 10.x+ required for Tensor API execution on this worker.")


def _load_engine_trt10(engine_path):
  _require_tensorrt_10()
  import tensorrt as trt

  with FileSystems.open(engine_path, 'rb') as f:
    blob = f.read()

  logger = trt.Logger(trt.Logger.INFO)
  trt.init_libnvinfer_plugins(logger, "")
  rt = trt.Runtime(logger)
  eng = rt.deserialize_cuda_engine(blob)
  if eng is None:
    raise RuntimeError(
        "Failed to deserialize TensorRT engine. "
        "The plan may be corrupt or built with an incompatible TRT.")
  return eng


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


def _apply_builder_config_args(trt, config, builder_config_args):
  if not builder_config_args:
    return
  memory_pool_limit = builder_config_args.get("memory_pool_limit", None)
  if memory_pool_limit is not None:
    config.set_memory_pool_limit(
        trt.MemoryPoolType.WORKSPACE, int(memory_pool_limit))
  for flag in builder_config_args.get("builder_flags", []):
    config.set_flag(flag)


def _load_onnx_build_engine_trt10(onnx_path,
                                 builder_config_args: Optional[Dict[str,
                                                                    Any]]):
  if onnx_path.lower().endswith(".engine"):
    raise ValueError(
        "Provided onnx_path points to .engine; pass it as engine_path instead.")

  _require_tensorrt_10()
  import tensorrt as trt

  logger = trt.Logger(trt.Logger.INFO)
  trt.init_libnvinfer_plugins(logger, "")

  builder = trt.Builder(logger)
  flags = 1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH)
  network = builder.create_network(flags)
  parser = trt.OnnxParser(network, logger)

  with FileSystems.open(onnx_path, 'rb') as f:
    data = f.read()

  if not parser.parse(data):
    LOGGER.error("Failed to parse ONNX: %s", onnx_path)
    for i in range(parser.num_errors):
      LOGGER.error(parser.get_error(i))
    raise ValueError(f"Failed to parse ONNX: {onnx_path}")

  config = builder.create_builder_config()
  config.set_memory_pool_limit(trt.MemoryPoolType.WORKSPACE, 1 << 30)
  _apply_builder_config_args(trt, config, builder_config_args)

  if getattr(builder, "platform_has_fast_fp16", False):
    config.set_flag(trt.BuilderFlag.FP16)

  if network.num_inputs > 0:
    prof = builder.create_optimization_profile()
    for i in range(network.num_inputs):
      inp = network.get_input(i)
      shp = list(inp.shape)

      def _d(v: int, default: int) -> int:
        return default if v < 0 else v

      if len(shp) == 4:
        min_shape: Tuple[int, ...] = (
            _d(shp[0], 1), _d(shp[1], 3), _d(shp[2], 224), _d(shp[3], 224))
        opt_shape: Tuple[int, ...] = (
            _d(shp[0], 4), _d(shp[1], 3), _d(shp[2], 224), _d(shp[3], 224))
        max_shape: Tuple[int, ...] = (
            _d(shp[0], 8), _d(shp[1], 3), _d(shp[2], 224), _d(shp[3], 224))
      else:
        min_shape = tuple(_d(x, 1) for x in shp)
        opt_shape = tuple(_d(x, 4 if j == 0 else 1) for j, x in enumerate(shp))
        max_shape = tuple(_d(x, 8 if j == 0 else 1) for j, x in enumerate(shp))

      prof.set_shape(inp.name, min=min_shape, opt=opt_shape, max=max_shape)
    config.add_optimization_profile(prof)

  plan = builder.build_serialized_network(network, config)
  if plan is None:
    raise RuntimeError(
        "build_serialized_network() returned None; check ONNX and profiles.")

  rt = trt.Runtime(logger)
  eng = rt.deserialize_cuda_engine(bytes(plan))
  if eng is None:
    raise RuntimeError("Failed to deserialize engine after build.")
  return eng


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


def _resolve_output_shape(shape: Sequence[int] | None,
                          batch_size: int) -> Tuple[int, ...] | None:
  if shape is None:
    return None
  shp = list(shape)
  if shp and shp[0] < 0:
    shp[0] = int(batch_size)
  if any(d < 0 for d in shp[1:]):
    raise RuntimeError(f"Unresolved non-batch dims in output shape: {shape}")
  return tuple(shp)


def _to_contiguous_batch(x: Sequence[np.ndarray] | np.ndarray) -> np.ndarray:
  if isinstance(x, np.ndarray):
    return np.ascontiguousarray(x)
  if isinstance(x, (list, tuple)):
    if len(x) == 1 and isinstance(x[0], np.ndarray):
      return np.ascontiguousarray(x[0])
    if all(isinstance(a, np.ndarray) for a in x):
      first = x[0].shape
      for a in x[1:]:
        if len(a.shape) != len(first) or any(
            sa != sb for sa, sb in zip(a.shape[1:], first[1:])):
          raise ValueError(
              "Inconsistent element shapes for concatenation: "
              f"{first} vs {a.shape}")
      return np.ascontiguousarray(np.concatenate(x, axis=0))
  raise ValueError(
      "Batch must be ndarray or sequence of ndarrays of same "
      "rank/shape (except batch).")


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

    # TODO(https://github.com/NVIDIA/TensorRT/issues/2557):
    # Clean up when fixed upstream.
    try:
      _ = np.bool
    except AttributeError:
      # numpy >= 1.24.0
      np.bool = np.bool_  # type: ignore

    # Setup I/O bindings.
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


class TensorRTEngineTensorApi:
  """TRT 10.x engine wrapper using the Tensor API."""
  def __init__(self, engine: Any):
    import tensorrt as trt

    self.engine = engine
    self.context = engine.create_execution_context()
    self.context_lock = threading.RLock()

    self.input_names: List[str] = []
    self.output_names: List[str] = []
    self.dtypes: Dict[str, np.dtype] = {}
    self.profile_index = 0
    self._stream = None
    self._device_ptrs: Dict[str, int] = {}
    self._host_out: Dict[str, np.ndarray] = {}
    self._in_nbytes: Optional[int] = None

    for i in range(engine.num_io_tensors):
      name = engine.get_tensor_name(i)
      mode = engine.get_tensor_mode(name)
      if mode == trt.TensorIOMode.INPUT:
        self.input_names.append(name)
      else:
        self.output_names.append(name)
      self.dtypes[name] = np.dtype(trt.nptype(engine.get_tensor_dtype(name)))

  @property
  def is_trt10(self) -> bool:
    return True

  def _ensure_stream(self) -> None:
    if self._stream is None:
      from cuda import cuda
      self._stream = _assign_or_fail(cuda.cuStreamCreate(0))

  def _free_ptr(self, ptr: int) -> None:
    if ptr is None:
      return
    from cuda import cuda
    try:
      _assign_or_fail(cuda.cuMemFree(ptr))
    except RuntimeError as exc:
      LOGGER.warning('Failed to free CUDA memory pointer: %s', exc)

  def _select_profile(self) -> None:
    if hasattr(self.context, "set_optimization_profile_async"):
      self._ensure_stream()
      self.context.set_optimization_profile_async(
          self.profile_index, self._stream)
    elif hasattr(self.context, "set_optimization_profile"):
      self.context.set_optimization_profile(self.profile_index)

  def _check_shape_in_profile(self, name: str, shape: Sequence[int]) -> None:
    mi, _, ma = self.engine.get_tensor_profile_shape(name, self.profile_index)

    def ok(dim: int, lo: int, hi: int) -> bool:
      if lo < 0:
        lo = dim
      if hi < 0:
        hi = dim
      return lo <= dim <= hi

    if len(shape) != len(mi):
      raise RuntimeError(
          f"Input '{name}' rank mismatch: given {tuple(shape)}, "
          f"profile[{self.profile_index}] min={tuple(mi)} max={tuple(ma)}")
    for i, dim in enumerate(shape):
      if not ok(dim, mi[i], ma[i]):
        raise RuntimeError(
            f"Input '{name}' dim {i}={dim} outside "
            f"profile[{self.profile_index}] bounds "
            f"[min={mi[i]}, max={ma[i]}]. Given={tuple(shape)}, "
            f"min={tuple(mi)}, max={tuple(ma)}")

  def ensure_buffers(
      self,
      batch: np.ndarray,
      input_shapes: Optional[Dict[str, Sequence[int]]] = None,
  ) -> None:
    from cuda import cuda

    self._select_profile()

    shapes: Dict[str, List[int]] = {}
    if len(self.input_names) == 1:
      shapes[self.input_names[0]] = list(batch.shape)
    else:
      if not input_shapes:
        raise RuntimeError(
            f"Engine expects multiple inputs {self.input_names}; "
            "provide shapes via "
            "inference_args={'input_shapes': {name: shape, ...}}")
      for name in self.input_names:
        if name not in input_shapes:
          raise RuntimeError(f"Missing shape for input tensor '{name}'")
        shapes[name] = list(map(int, input_shapes[name]))

    for name, shp in shapes.items():
      self._check_shape_in_profile(name, shp)
      self.context.set_input_shape(name, shp)

    in_name = self.input_names[0]
    in_dtype = self.dtypes[in_name]
    in_nbytes = int(np.prod(shapes[in_name])) * in_dtype.itemsize
    if self._device_ptrs.get(in_name) is None or self._in_nbytes != in_nbytes:
      if self._device_ptrs.get(in_name) is not None:
        self._free_ptr(self._device_ptrs[in_name])
      self._device_ptrs[in_name] = _assign_or_fail(cuda.cuMemAlloc(in_nbytes))
      self._in_nbytes = in_nbytes

    batch_size = shapes[in_name][0]
    for name in self.output_names:
      dtype = self.dtypes[name]
      raw_shape = list(self.context.get_tensor_shape(name))
      shape = _resolve_output_shape(raw_shape, batch_size)
      if shape is None:
        raise RuntimeError(f"Context returned None shape for output '{name}'")
      nbytes = int(np.prod(shape)) * dtype.itemsize
      need_new = (
          self._device_ptrs.get(name) is None or
          self._host_out.get(name) is None or
          self._host_out[name].nbytes != nbytes)
      if need_new:
        if self._device_ptrs.get(name) is not None:
          self._free_ptr(self._device_ptrs[name])
        self._device_ptrs[name] = _assign_or_fail(cuda.cuMemAlloc(nbytes))
        self._host_out[name] = np.empty(shape, dtype=dtype)

    self.context.set_tensor_address(in_name, int(self._device_ptrs[in_name]))
    for name in self.output_names:
      self.context.set_tensor_address(name, int(self._device_ptrs[name]))


TensorRTInferenceFn = Callable[
    [Sequence[np.ndarray], Any, Optional[dict[str, Any]]],
    Iterable[PredictionResult]]


def _legacy_tensorRT_inference_fn(
    batch: Sequence[np.ndarray],
    engine: TensorRTEngine,
    inference_args: Optional[dict[str,
                                  Any]] = None) -> Iterable[PredictionResult]:
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
      predictions.append([prediction[idx] for prediction in cpu_allocations])

    return utils._convert_to_result(batch, predictions)


def _trt10_inference_fn(
    batch: Sequence[np.ndarray] | np.ndarray,
    engine_obj: TensorRTEngineTensorApi,
    inference_args: Optional[dict[str, Any]] = None,
) -> Iterable[PredictionResult]:
  from cuda import cuda

  batch_arr = _to_contiguous_batch(batch)

  input_shapes = None
  if inference_args:
    if "profile_index" in inference_args:
      engine_obj.profile_index = int(inference_args["profile_index"])
    input_shapes = inference_args.get("input_shapes", None)

  ctx = engine_obj.context
  with engine_obj.context_lock:
    engine_obj._ensure_stream()
    engine_obj.ensure_buffers(batch_arr, input_shapes)

    in_name = engine_obj.input_names[0]
    _assign_or_fail(
        cuda.cuMemcpyHtoDAsync(
            engine_obj._device_ptrs[in_name],
            batch_arr.ctypes.data,
            batch_arr.nbytes,
            engine_obj._stream,
        ))

    ok = ctx.execute_async_v3(engine_obj._stream)
    if not ok:
      eng = engine_obj.engine
      mi, oi, ma = eng.get_tensor_profile_shape(
          in_name, engine_obj.profile_index)
      raise RuntimeError(
          "TensorRT execute_async_v3 failed. "
          f"Batch shape={tuple(batch_arr.shape)}; "
          f"profile[{engine_obj.profile_index}] {in_name} "
          f"min={tuple(mi)} opt={tuple(oi)} max={tuple(ma)}")

    for name in engine_obj.output_names:
      host = engine_obj._host_out[name]
      _assign_or_fail(
          cuda.cuMemcpyDtoHAsync(
              host.ctypes.data,
              engine_obj._device_ptrs[name],
              host.nbytes,
              engine_obj._stream,
          ))
    _assign_or_fail(cuda.cuStreamSynchronize(engine_obj._stream))

    outs = [engine_obj._host_out[name] for name in engine_obj.output_names]

  per_item = [[o[i] for o in outs] for i in range(batch_arr.shape[0])]
  return utils._convert_to_result(batch_arr, per_item)


def _default_tensorRT_inference_fn(
    batch: Sequence[np.ndarray],
    engine: Any,
    inference_args: Optional[dict[str, Any]] = None
) -> Iterable[PredictionResult]:
  if isinstance(engine, TensorRTEngineTensorApi):
    return _trt10_inference_fn(batch, engine, inference_args)
  return _legacy_tensorRT_inference_fn(batch, engine, inference_args)


class TensorRTEngineHandlerNumPy(ModelHandler[np.ndarray,
                                              PredictionResult,
                                              Any]):
  def __init__(
      self,
      min_batch_size: int,
      max_batch_size: int,
      *,
      inference_fn: TensorRTInferenceFn = _default_tensorRT_inference_fn,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      build_on_worker: bool = False,
      onnx_builder_config_args: Optional[Dict[str, Any]] = None,
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
      build_on_worker: if True and onnx_path is supplied, build a TensorRT
        engine on the worker and use it for RunInference.
      onnx_builder_config_args: optional configuration overrides applied to the
        TensorRT builder config when building from ONNX on worker.
      kwargs: Additional arguments like 'engine_path' and 'onnx_path' are
        currently supported. 'env_vars' can be used to set environment variables
        before loading the model.

    See https://docs.nvidia.com/deeplearning/tensorrt/api/python_api/
    for details
    """
    self.min_batch_size = min_batch_size
    self.max_batch_size = max_batch_size
    self.max_batch_duration_secs = max_batch_duration_secs
    self.inference_fn = inference_fn
    self.build_on_worker = build_on_worker
    self.onnx_builder_config_args = onnx_builder_config_args or {}
    if 'engine_path' in kwargs:
      self.engine_path = kwargs.get('engine_path')
    elif 'onnx_path' in kwargs:
      self.onnx_path = kwargs.get('onnx_path')
    self._env_vars = kwargs.get('env_vars', {})
    self._share_across_processes = large_model or (model_copies is not None)
    self._model_copies = model_copies or 1

  def batch_elements_kwargs(self):
    """Sets min_batch_size and max_batch_size of a TensorRT engine."""
    return {
        'min_batch_size': self.min_batch_size,
        'max_batch_size': self.max_batch_size,
        'max_batch_duration_secs': self.max_batch_duration_secs
    }

  def load_model(self) -> Any:
    """Loads and initializes a TensorRT engine for processing."""
    if hasattr(self, 'engine_path'):
      if _tensorrt_supports_tensor_api():
        engine = _load_engine_trt10(self.engine_path)
        if hasattr(engine, "num_io_tensors"):
          return TensorRTEngineTensorApi(engine)
      engine = _load_engine(self.engine_path)
      return TensorRTEngine(engine)

    if hasattr(self, 'onnx_path'):
      if not self.build_on_worker:
        raise ValueError(
            "onnx_path provided but build_on_worker=False; "
            "use load_onnx/build_engine directly or set build_on_worker=True.")
      if _tensorrt_supports_tensor_api():
        engine = _load_onnx_build_engine_trt10(
            self.onnx_path, self.onnx_builder_config_args)
        return TensorRTEngineTensorApi(engine)
      network, builder = _load_onnx(self.onnx_path)
      engine = _build_engine(network, builder)
      return TensorRTEngine(engine)

    raise ValueError("Expected engine_path or onnx_path to load TensorRT model.")

  def load_onnx(self) -> tuple[trt.INetworkDefinition, trt.Builder]:
    """Loads and parses an onnx model for processing."""
    return _load_onnx(self.onnx_path)

  def build_engine(
      self, network: trt.INetworkDefinition,
      builder: trt.Builder) -> Any:
    """Build an engine according to parsed/created network."""
    engine = _build_engine(network, builder)
    if _tensorrt_supports_tensor_api() and hasattr(engine, "num_io_tensors"):
      return TensorRTEngineTensorApi(engine)
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

  def share_model_across_processes(self) -> bool:
    return self._share_across_processes

  def model_copies(self) -> int:
    return self._model_copies
