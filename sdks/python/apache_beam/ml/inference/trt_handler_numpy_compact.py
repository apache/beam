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

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler, PredictionResult

LOGGER = logging.getLogger("TensorRTEngineHandlerNumPy_TRT10")

__all__ = [
    "TensorRTEngine",
    "TensorRTEngineHandlerNumPy",
]


# ---------------------------------------------------------------------
# CUDA / TensorRT helpers
# ---------------------------------------------------------------------
def _assign_or_fail(args):
  """CUDA error checking for cuda-python (Driver API)."""
  from cuda import cuda  # lazy import to avoid submit-time dependency

  err, *ret = args
  if isinstance(err, cuda.CUresult):
    if err != cuda.CUresult.CUDA_SUCCESS:
      raise RuntimeError(f"CUDA error: {err}")
  else:
    raise RuntimeError(f"Unknown CUDA error type: {err}")
  return ret[0] if len(ret) == 1 else tuple(ret)


def _require_tensorrt_10() -> None:
  """Assert that TensorRT 10.x Tensor API is available on this worker."""
  try:
    import tensorrt as trt  # noqa: F401
  except Exception as e:  # pragma: no cover
    raise RuntimeError("TensorRT is not installed on this worker.") from e

  # TRT 10.x indicators:
  #  - Engine exposes the Tensor API (num_io_tensors / get_tensor_name)
  #  - ExecutionContext exposes execute_async_v3 / set_input_shape / set_tensor_address
  engine_reqs = ("num_io_tensors", "get_tensor_name")
  ctx_reqs = ("execute_async_v3", "set_input_shape", "set_tensor_address")

  import tensorrt as trt  # type: ignore
  missing_engine = [m for m in engine_reqs if not hasattr(trt.ICudaEngine, m)]
  missing_ctx = [m for m in ctx_reqs if not hasattr(trt.IExecutionContext, m)]

  if missing_engine or missing_ctx:
    raise RuntimeError(
        "This handler requires TensorRT 10.x+. "
        f"Missing on ICudaEngine: {missing_engine or 'OK'}, "
        f"Missing on IExecutionContext: {missing_ctx or 'OK'}")


# ---------------------------------------------------------------------
# Engine load / build (TRT 10)
# ---------------------------------------------------------------------
def _load_engine(engine_path: str):
  """Deserialize a .engine (plan) from FileSystems into a TRT engine."""
  _require_tensorrt_10()

  import tensorrt as trt

  with FileSystems.open(engine_path, "rb") as f:
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


def _load_onnx_build_engine(onnx_path: str):
  """Parse ONNX and build a TRT engine immediately (Tensor API)."""
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

  with FileSystems.open(onnx_path, "rb") as f:
    data = f.read()

  if not parser.parse(data):
    LOGGER.error("Failed to parse ONNX: %s", onnx_path)
    for i in range(parser.num_errors):
      LOGGER.error(parser.get_error(i))
    raise ValueError(f"Failed to parse ONNX: {onnx_path}")

  config = builder.create_builder_config()
  # Workbench: ~1GiB workspace (tune for your model/infra)
  config.set_memory_pool_limit(trt.MemoryPoolType.WORKSPACE, 1 << 30)

  if getattr(builder, "platform_has_fast_fp16", False):
    config.set_flag(trt.BuilderFlag.FP16)

  # Generic optimization profile for dynamic inputs.
  if network.num_inputs > 0:
    prof = builder.create_optimization_profile()
    for i in range(network.num_inputs):
      inp = network.get_input(i)
      shp = list(inp.shape)

      def _d(v: int, default: int) -> int:
        return default if v < 0 else v

      if len(shp) == 4:
        # Assume NCHW; supply defaults where dims are dynamic.
        min_shape = (
            _d(shp[0], 1), _d(shp[1], 3), _d(shp[2], 224), _d(shp[3], 224))
        opt_shape = (
            _d(shp[0], 4), _d(shp[1], 3), _d(shp[2], 224), _d(shp[3], 224))
        max_shape = (
            _d(shp[0], 8), _d(shp[1], 3), _d(shp[2], 224), _d(shp[3], 224))
      else:
        # Fallback: make batch dynamic, keep others as-is or 1.
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


# ---------------------------------------------------------------------
# Shape & batch helpers
# ---------------------------------------------------------------------
def _resolve_output_shape(shape: Sequence[int] | None,
                          batch_size: int) -> Tuple[int, ...] | None:
  """Replace a leading -1 (batch) with batch_size; any other -1 is an error."""
  if shape is None:
    return None
  shp = list(shape)
  if len(shp) > 0 and shp[0] < 0:
    shp[0] = int(batch_size)
  if any(d < 0 for d in shp[1:]):
    raise RuntimeError(f"Unresolved non-batch dims in output shape: {shape}")
  return tuple(shp)


def _to_contiguous_batch(x: Sequence[np.ndarray] | np.ndarray) -> np.ndarray:
  """
    Accept either an ndarray (already a batch) or a list of ndarrays (concat on axis 0).
    This avoids accidental rank-5 shapes from upstream batching.
    """
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
              f"Inconsistent element shapes for concatenation: {first} vs {a.shape}"
          )
      return np.ascontiguousarray(np.concatenate(x, axis=0))

  raise ValueError(
      "Batch must be ndarray or sequence of ndarrays of same rank/shape (except batch)."
  )


# ---------------------------------------------------------------------
# TRT 10.x engine wrapper (Tensor API only)
# ---------------------------------------------------------------------
class TensorRTEngine:
  """TRT 10.x engine wrapper using the Tensor API."""
  def __init__(self, engine: Any):
    import tensorrt as trt  # type: ignore

    self.engine = engine
    self.context = engine.create_execution_context()
    self.context_lock = threading.RLock()

    # Tensor API enumeration
    self.input_names: List[str] = []
    self.output_names: List[str] = []
    self.dtypes: Dict[str, np.dtype] = {}

    for i in range(engine.num_io_tensors):
      name = engine.get_tensor_name(i)
      mode = engine.get_tensor_mode(name)
      if mode == trt.TensorIOMode.INPUT:
        self.input_names.append(name)
      else:
        self.output_names.append(name)
      self.dtypes[name] = np.dtype(trt.nptype(engine.get_tensor_dtype(name)))

    # Lazy allocations
    self._device_ptrs: Dict[str, int] = {}  # tensor name -> CUdeviceptr
    self._host_out: Dict[str, np.ndarray] = {}
    self._in_nbytes: int = 0
    self._stream: Optional[int] = None
    self.profile_index: int = 0

  def _ensure_stream(self) -> None:
    if self._stream is None:
      from cuda import cuda
      self._stream = _assign_or_fail(cuda.cuStreamCreate(0))

  def _free_ptr(self, ptr: Optional[int]) -> None:
    if not ptr:
      return
    from cuda import cuda
    try:
      _assign_or_fail(cuda.cuMemFree(ptr))
    except Exception:
      pass

  def _select_profile(self) -> None:
    # Pick optimization profile (sync or async depending on TRT)
    if hasattr(self.context, "set_optimization_profile_async"):
      self._ensure_stream()
      self.context.set_optimization_profile_async(
          self.profile_index, self._stream)
    elif hasattr(self.context, "set_optimization_profile"):
      self.context.set_optimization_profile(self.profile_index)

  def _check_shape_in_profile(self, name: str, shape: Sequence[int]) -> None:
    mi, _oi, ma = self.engine.get_tensor_profile_shape(name, self.profile_index)

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
            f"Input '{name}' dim {i}={dim} outside profile[{self.profile_index}] bounds "
            f"[min={mi[i]}, max={ma[i]}]. Given={tuple(shape)}, "
            f"min={tuple(mi)}, max={tuple(ma)}")

  def ensure_buffers(
      self,
      batch: np.ndarray,
      input_shapes: Optional[Dict[str, Sequence[int]]] = None,
  ) -> None:
    """
        Validate shapes, set input shapes, (re)allocate device + host buffers,
        and set tensor addresses for Tensor API execution.
        """
    from cuda import cuda

    self._select_profile()

    # Derive shapes for inputs
    shapes: Dict[str, List[int]] = {}
    if len(self.input_names) == 1:
      shapes[self.input_names[0]] = list(batch.shape)
    else:
      if not input_shapes:
        raise RuntimeError(
            f"Engine expects multiple inputs {self.input_names}; "
            "provide shapes via inference_args={'input_shapes': {name: shape, ...}}"
        )
      for name in self.input_names:
        if name not in input_shapes:
          raise RuntimeError(f"Missing shape for input tensor '{name}'")
        shapes[name] = list(map(int, input_shapes[name]))

    # Validate and set shapes
    for name, shp in shapes.items():
      self._check_shape_in_profile(name, shp)
      self.context.set_input_shape(name, shp)

    # Allocate first input device buffer (we copy only this from 'batch')
    in_name = self.input_names[0]
    in_dtype = self.dtypes[in_name]
    in_nbytes = int(np.prod(shapes[in_name])) * in_dtype.itemsize
    if self._device_ptrs.get(in_name) is None or self._in_nbytes != in_nbytes:
      if self._device_ptrs.get(in_name) is not None:
        self._free_ptr(self._device_ptrs[in_name])
      self._device_ptrs[in_name] = _assign_or_fail(cuda.cuMemAlloc(in_nbytes))
      self._in_nbytes = in_nbytes

    # Outputs
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

    # Set tensor addresses
    self.context.set_tensor_address(in_name, int(self._device_ptrs[in_name]))
    for name in self.output_names:
      self.context.set_tensor_address(name, int(self._device_ptrs[name]))


# ---------------------------------------------------------------------
# Inference function (TRT 10)
# ---------------------------------------------------------------------
def _trt10_inference_fn(
    batch: Sequence[np.ndarray] | np.ndarray,
    engine_obj: TensorRTEngine,
    inference_args: Optional[dict[str, Any]] = None,
) -> Iterable[PredictionResult]:
  """Default inference fn using TensorRT 10 Tensor API."""
  from cuda import cuda

  # Normalize batch to contiguous ndarray (NCHW or whatever the model expects)
  batch_arr = _to_contiguous_batch(batch)

  # Optional args
  input_shapes = None
  if inference_args:
    if "profile_index" in inference_args:
      engine_obj.profile_index = int(inference_args["profile_index"])
    input_shapes = inference_args.get("input_shapes", None)

  ctx = engine_obj.context
  with engine_obj.context_lock:
    engine_obj._ensure_stream()
    engine_obj.ensure_buffers(batch_arr, input_shapes)

    # HtoD: first input buffer
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
      mi, oi, ma = eng.get_tensor_profile_shape(in_name, engine_obj.profile_index)
      raise RuntimeError(
          "TensorRT execute_async_v3 failed. "
          f"Batch shape={tuple(batch_arr.shape)}; "
          f"profile[{engine_obj.profile_index}] {in_name} "
          f"min={tuple(mi)} opt={tuple(oi)} max={tuple(ma)}")

    # DtoH outputs
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

  # One PredictionResult per item
  per_item = [[o[i] for o in outs] for i in range(batch_arr.shape[0])]
  return utils._convert_to_result(batch_arr, per_item)


# ---------------------------------------------------------------------
# Beam ModelHandler (TRT 10 only)
# ---------------------------------------------------------------------
class TensorRTEngineHandlerNumPy(ModelHandler[np.ndarray,
                                              PredictionResult,
                                              TensorRTEngine]):
  """Beam ModelHandler pinned to TensorRT 10.x Tensor API.

    Provide exactly one of:
      - engine_path: path to a serialized TensorRT plan (.engine)
      - onnx_path: path to an ONNX file (requires build_on_worker=True)
    """
  def __init__(
      self,
      min_batch_size: int,
      max_batch_size: int,
      *,
      engine_path: Optional[str] = None,
      onnx_path: Optional[str] = None,
      build_on_worker: bool = False,  # only used if onnx_path is given
      inference_fn=_trt10_inference_fn,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      env_vars: Optional[Dict[str, str]] = None,
  ):
    if engine_path and onnx_path:
      raise ValueError(
          "Provide only one of engine_path or onnx_path, not both.")
    if not engine_path and not onnx_path:
      raise ValueError("Provide engine_path (.engine) or onnx_path (.onnx).")
    if engine_path and not engine_path.lower().endswith(".engine"):
      raise ValueError(f"engine_path must end with .engine, got: {engine_path}")
    if onnx_path and onnx_path.lower().endswith(".engine"):
      raise ValueError(
          f"onnx_path points to .engine: {onnx_path}. Use engine_path instead.")

    self.min_batch_size = int(min_batch_size)
    self.max_batch_size = int(max_batch_size)
    self.max_batch_duration_secs = max_batch_duration_secs
    self.inference_fn = inference_fn

    self.engine_path = engine_path
    self.onnx_path = onnx_path
    self.build_on_worker = bool(build_on_worker)
    self._env_vars = env_vars or {}

    self._share_across_processes = bool(
        large_model or (model_copies is not None))
    self._model_copies = int(model_copies or 1)

  # --- ModelHandler API -------------------------------------------------

  def batch_elements_kwargs(self) -> Dict[str, Any]:
    return {
        "min_batch_size": self.min_batch_size,
        "max_batch_size": self.max_batch_size,
        "max_batch_duration_secs": self.max_batch_duration_secs,
    }

  def load_model(self) -> TensorRTEngine:
    # Ensure environment variables are set before touching TRT
    import os

    for k, v in self._env_vars.items():
      os.environ[str(k)] = str(v)

    if self.engine_path:
      eng = _load_engine(self.engine_path)
      return TensorRTEngine(eng)

    if not self.build_on_worker:
      raise RuntimeError(
          "onnx_path provided but build_on_worker=False. "
          "Enable build_on_worker=True to compile ONNX on workers, or prebuild an engine."
      )

    eng = _load_onnx_build_engine(self.onnx_path)  # type: ignore[arg-type]
    return TensorRTEngine(eng)

  def run_inference(
      self,
      batch: Sequence[np.ndarray] | np.ndarray,
      model: TensorRTEngine,
      inference_args: Optional[dict[str, Any]] = None,
  ) -> Iterable[PredictionResult]:
    return self.inference_fn(batch, model, inference_args)

  def get_num_bytes(self, batch: Sequence[np.ndarray] | np.ndarray) -> int:
    if isinstance(batch, np.ndarray):
      return int(batch.nbytes)
    if isinstance(batch, (list, tuple)) and all(isinstance(a, np.ndarray)
                                                for a in batch):
      return int(sum(a.nbytes for a in batch))
    arr = np.asarray(batch)
    return int(arr.nbytes)

  def get_metrics_namespace(self) -> str:
    return "BeamML_TensorRT10"

  def share_model_across_processes(self) -> bool:
    return self._share_across_processes

  def model_copies(self) -> int:
    return self._model_copies
