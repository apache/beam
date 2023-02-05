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
      inference_fn: NumpyInferenceFn = default_numpy_inference_fn):
    """ Implementation of the ModelHandler interface for onnx
    using numpy arrays as input.
    Note that inputs to ONNXModelHandler should be of the same sizes

    Example Usage::

      pcoll | RunInference(OnnxModelHandler(model_uri="my_uri"))

    Args:
      model_uri: The URI to where the model is saved.
      inference_fn: The inference function to use on RunInference calls.
        default=default_numpy_inference_fn
    """
    self._model_uri = model_uri
    self._session_options = session_options
    self._providers = providers
    self._provider_options = provider_options
    self._model_inference_fn = inference_fn

  def load_model(self) -> ort.InferenceSession:
    """Loads and initializes an onnx inference session for processing."""
    # when path is remote, we should first load into memory then deserialize
    f = FileSystems.open(self._model_uri, "rb")
    model_proto = onnx.load(f)
    model_proto_bytes = onnx._serialize(model_proto)
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
