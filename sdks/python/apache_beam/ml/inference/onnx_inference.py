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

import pickle
import sys
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

import numpy
import pandas
import onnx
import onnxruntime.InferenceSession

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils.annotations import experimental

try:
  import joblib
except ImportError:
  # joblib is an optional dependency.
  pass

__all__ = [
    'OnnxModelHandler'
]

NumpyInferenceFn = Callable[
    [Sequence[numpy.ndarray], InferenceSession, Optional[Dict[str, Any]]]]


def _load_model(model_uri):
  ort_session = InferenceSession(model_uri)
  return ort_session


def _convert_to_result(
    batch: Iterable, predictions: Union[Iterable, Dict[Any, Iterable]]
) -> Iterable[PredictionResult]:
  if isinstance(predictions, dict):
    # Go from one dictionary of type: {key_type1: Iterable<val_type1>,
    # key_type2: Iterable<val_type2>, ...} where each Iterable is of
    # length batch_size, to a list of dictionaries:
    # [{key_type1: value_type1, key_type2: value_type2}]
    predictions_per_tensor = [
        dict(zip(predictions.keys(), v)) for v in zip(*predictions.values())
    ]
    return [
        PredictionResult(x, y) for x, y in zip(batch, predictions_per_tensor)
    ]
  return [PredictionResult(x, y) for x, y in zip(batch, predictions)]


def _default_numpy_inference_fn(
    inference_session: InferenceSession,
    batch: Sequence[numpy.ndarray],
    inference_args: Optional[Dict[str, Any]] = None) -> Any:
  ort_inputs = {ort_session.get_inputs()[0].name: batch}
  ort_outs = ort_session.run(None, ort_inputs)
  return ort_outs


class OnnxModelHandler(ModelHandler[numpy.ndarray,
                                    PredictionResult,
                                    InferenceSession]):
  def __init__(
      self,
      model_uri: str,
      *,
      inference_fn: NumpyInferenceFn = _default_numpy_inference_fn):
    """ Implementation of the ModelHandler interface for onnx
    using numpy arrays as input.

    Example Usage::

      pcoll | RunInference(OnnxModelHandler(model_uri="my_uri"))

    Args:
      model_uri: The URI to where the model is saved.
      inference_fn: The inference function to use.
        default=_default_numpy_inference_fn
    """
    self._model_uri = model_uri
    self._model_inference_fn = inference_fn

  def load_model(self) -> InferenceSession:
    """Loads and initializes an onnx inference session for processing."""
    return _load_model(self._model_uri)

  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      inference_session: InferenceSession,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of numpy arrays.

    Args:
      batch: A sequence of examples as numpy arrays. They should
        be single examples. [???]
      inference_session: An onnx inference session. Must be runnable with input x where x is sequence of numpy array
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    predictions = self._model_inference_fn(inference_session, batch, inference_args)

    return _convert_to_result(batch, predictions)

  def get_num_bytes(self, batch: Sequence[numpy.DataFrame]) -> int:
    """
    Returns:
      The number of bytes of data for a batch.
    """
    return sum(sys.getsizeof(element) for element in batch)

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_Onnx'
