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

from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Sequence

import numpy

import onnx
import onnxruntime as ort
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

__all__ = ['OnnxModelHandlerNumpy']

NumpyInferenceFn = Callable[
    [Sequence[numpy.ndarray], ort.InferenceSession, Optional[Dict[str, Any]]],
    Iterable[PredictionResult]]


def default_numpy_inference_fn(
    inference_session: ort.InferenceSession,
    batch: Sequence[numpy.ndarray],
    inference_args: Optional[Dict[str, Any]] = None) -> Any:
  ort_inputs = {
      inference_session.get_inputs()[0].name: numpy.stack(batch, axis=0)
  }
  if inference_args:
    ort_inputs = {**ort_inputs, **inference_args}
  ort_outs = inference_session.run(None, ort_inputs)[0]
  return ort_outs


class OnnxModelHandlerNumpy(ModelHandler[numpy.ndarray,
                                         PredictionResult,
                                         ort.InferenceSession]):
  def __init__( #pylint: disable=dangerous-default-value
      self,
      model_uri: str,
      session_options=None,
      providers=['CUDAExecutionProvider', 'CPUExecutionProvider'],
      provider_options=None,
      *,
      inference_fn: NumpyInferenceFn = default_numpy_inference_fn,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      **kwargs):
    """ Implementation of the ModelHandler interface for onnx
    using numpy arrays as input.
    Note that inputs to ONNXModelHandler should be of the same sizes

    Example Usage::

      pcoll | RunInference(OnnxModelHandler(model_uri="my_uri"))

    Args:
      model_uri: The URI to where the model is saved.
      inference_fn: The inference function to use on RunInference calls.
        default=default_numpy_inference_fn
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
      max_batch_duration_secs: the maximum amount of time to buffer a batch
        before emitting; used in streaming contexts.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.
    """
    self._model_uri = model_uri
    self._session_options = session_options
    self._providers = providers
    self._provider_options = provider_options
    self._model_inference_fn = inference_fn
    self._env_vars = kwargs.get('env_vars', {})
    self._share_across_processes = large_model or (model_copies is not None)
    self._model_copies = model_copies or 1
    self._batching_kwargs = {}
    if min_batch_size is not None:
      self._batching_kwargs["min_batch_size"] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs["max_batch_size"] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs

  def load_model(self) -> ort.InferenceSession:
    """Loads and initializes an onnx inference session for processing."""
    # when path is remote, we should first load into memory then deserialize
    f = FileSystems.open(self._model_uri, "rb")
    model_proto = onnx.load(f)
    model_proto_bytes = model_proto
    if not isinstance(model_proto, bytes):
      if (hasattr(model_proto, "SerializeToString") and
          callable(model_proto.SerializeToString)):
        model_proto_bytes = model_proto.SerializeToString()
      else:
        raise TypeError(
          "No SerializeToString method is detected on loaded model. " +
          f"Type of model: {type(model_proto)}"
        )
    ort_session = ort.InferenceSession(
        model_proto_bytes,
        sess_options=self._session_options,
        providers=self._providers,
        provider_options=self._provider_options)
    return ort_session

  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      inference_session: ort.InferenceSession,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of numpy arrays.

    Args:
      batch: A sequence of examples as numpy arrays. They should
        be single examples.
      inference_session: An onnx inference session.
        Must be runnable with input x where x is sequence of numpy array
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    predictions = self._model_inference_fn(
        inference_session, batch, inference_args)

    return utils._convert_to_result(batch, predictions)

  def get_num_bytes(self, batch: Sequence[numpy.ndarray]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return sum((np_array.itemsize for np_array in batch))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_Onnx'

  def share_model_across_processes(self) -> bool:
    return self._share_across_processes

  def model_copies(self) -> int:
    return self._model_copies

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    return self._batching_kwargs
