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

import typing
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import SupportsFloat

import numpy
import pandas

import apache_beam as beam
from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import torch
except ImportError:
  torch = None

try:
  import tensorflow as tf
except ImportError:
  tf = None


def _to_numpy_array(row: beam.Row):
  """Converts an Apache Beam Row to a NumPy array."""
  return numpy.array(list(row))


def _to_pandas_dataframe(row: beam.Row):
  """Converts an Apache Beam Row to a Pandas DataFrame."""
  return pandas.DataFrame.from_records([row._asdict()])


def _to_pytorch_tensor(row: beam.Row):
  """Converts an Apache Beam Row to a PyTorch Tensor."""
  return torch.Tensor(list(row))


def _to_pytorch_keyed_tensor(row: beam.Row):
  """Converts an Apache Beam Row to a dictionary of PyTorch Tensors."""
  return {str(k): torch.Tensor(v) for k, v in row._asdict().items()}


def _to_tensorflow_tensor(row: beam.Row):
  """Converts an Apache Beam Row to a TensorFlow Tensor."""
  return tf.convert_to_tensor(list(row))


def _to_tensorflow_keyed_tensor(row: beam.Row):
  """Converts an Apache Beam Row to a dictionary of TensorFlow Tensors."""
  return {str(k): tf.constant(v) for k, v in row._asdict().items()}


class InputConverter():
  """A utility class for converting Apache Beam Rows into different formats."""
  _map: Dict[type, Callable[[beam.Row], Any]] = {}
  _map[numpy.ndarray] = _to_numpy_array
  _map[pandas.DataFrame] = _to_pandas_dataframe
  if torch:
    _map[torch.Tensor] = _to_pytorch_tensor
    _map[Dict[str, torch.Tensor]] = _to_pytorch_keyed_tensor
  if tf:
    _map[tf.Tensor] = _to_tensorflow_tensor
    _map[Dict[str, tf.Tensor]] = _to_tensorflow_keyed_tensor

  @classmethod
  def convert_to(cls, x, to_type):
    """Converts an input to a specified type.

    Args:
      x: The input to convert.
      to_type: The target type for conversion.

    Returns:
      The converted input.

    Raises:
      ValueError: If the target type is unknown or conversion fails.
    """
    if isinstance(to_type, type) and issubclass(to_type, beam.Row):
      return x

    if to_type in cls._map:
      return cls._map[to_type](x)

    raise ValueError(
        f"Unknown input type {to_type} for value {x}. "
        f"Please provide input_convert_fn to convert {to_type} to Beam Rows")


class OutputConverter():
  """A utility class for converting model prediction results to float values."""
  @staticmethod
  def convert_from(result: PredictionResult, from_type=None) -> float:
    """Converts RunInference's PredictionResult to a float value.

    Args:
      result: The PredictionResult object.
      from_type: The original type of the inference result (optional).

    Returns:
      The converted float value.

    Raises:
      ValueError: If the output type is unknown or conversion fails.
    """
    x = result.inference
    from_type = from_type or type(x)

    if isinstance(x, SupportsFloat):
      # Handles int, float, and other numeric types
      return float(x)
    elif isinstance(x, numpy.number):
      # Handles numpy numeric types
      return float(x)
    elif torch is not None and isinstance(x, torch.Tensor):
      return float(x.item())
    elif tf is not None and isinstance(x, tf.Tensor):
      if x.ndim >= 1:
        return float(x.numpy()[0])
      else:
        return float(x.numpy())
    else:
      raise ValueError(
          f"Unknown output type {from_type} of value {x}. "
          f"Please provide output_convert_fn to convert PredictionResult "
          f"(with inference field of type {from_type}) to float.")


def get_input_type(model_handler: ModelHandler):
  """Extracts the input (example) type from a ModelHandler.

  Args:
    model_handler: The ModelHandler instance.

  Returns:
    The input type expected by the model handler.
  """
  # TODO: Python 3.12 introduced types.get_original_bases() to access
  # __orig_bases__, but prior to that we will need to access the special
  # attribute directly.
  # Here we get input_type from
  #   ModelHandler(Generic[ExampleT, PredictionT, ModelT])
  input_type = typing.get_args(type(model_handler).__orig_bases__[0])[0]

  is_keyed = typing.get_origin(input_type) is dict and \
      typing.get_args(input_type)[0] is str

  if is_keyed:
    input_type = typing.get_args(input_type)[1]

  if tf and torch:
    if input_type == typing.Union[torch.Tensor, tf.Tensor]:
      # check framework to tell if it is from pytorch or tensorflow
      input_type = torch.Tensor if model_handler._framework == 'pt' \
          else tf.Tensor

  return Dict[str, input_type] if is_keyed else input_type


@specifiable
class CustomDetector(AnomalyDetector):
  """A custom anomaly detector that uses a provided model handler for scoring.

  Args:
    model_handler: The ModelHandler to use for inference.
    run_inference_args: Optional arguments to pass to RunInference
    input_convert_fn: Optional function to convert input Beam Rows to the
      model's expected input type.
    output_convert_fn: Optional function to convert model PredictionResults
      to float scores.
    **kwargs: Additional keyword arguments to pass to the base
      AnomalyDetector class.
  """
  def __init__(
      self,
      model_handler: ModelHandler,
      run_inference_args: Optional[Dict[str, Any]] = None,
      input_convert_fn: Optional[Callable[[beam.Row], Any]] = None,
      output_convert_fn: Optional[Callable[[PredictionResult], float]] = None,
      **kwargs):
    super().__init__(**kwargs)

    self._model_handler = model_handler
    self._keyed_model_handler = KeyedModelHandler(model_handler)
    self._input_type = get_input_type(self._model_handler)
    self._run_inference_args = run_inference_args or {}
    self.convert_input = input_convert_fn or self._default_convert_input
    self.convert_output = output_convert_fn or self._default_convert_output

    # always override model_identifier with model_id from the detector
    self._run_inference_args["model_identifier"] = self._model_id

  def _default_convert_input(self, x: beam.Row) -> Any:
    return InputConverter.convert_to(x, self._input_type)

  def _default_convert_output(self, x: PredictionResult) -> float:
    return OutputConverter.convert_from(x)

  def learn_one(self, x: beam.Row) -> None:
    """Not implemented since CustomDetector invokes RunInference directly."""
    raise NotImplementedError

  def score_one(self, x: beam.Row) -> Optional[float]:
    """Not implemented since CustomDetector invokes RunInference directly."""
    raise NotImplementedError
