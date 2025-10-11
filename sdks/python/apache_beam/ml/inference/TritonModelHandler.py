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

"""Apache Beam ModelHandler implementation for Triton Inference Server."""

from typing import Sequence, Dict, Any, Iterable, Optional
import logging
import json

from apache_beam.ml.inference.base import ModelHandler, PredictionResult

try:
  import tritonserver
  from tritonserver import Model, Server
except ImportError:
  tritonserver = None  # type: ignore

LOGGER = logging.getLogger(__name__)


class TritonModelWrapper:
  """Wrapper to manage Triton Server lifecycle with the model."""
  def __init__(self, server: 'Server', model: 'Model'):
    self.server = server
    self.model = model
    self._cleaned_up = False

  def cleanup(self):
    """Explicitly cleanup server resources.

    This method should be called when the model is no longer needed.
    It's safe to call multiple times.
    """
    if self._cleaned_up:
      return

    try:
      if self.server:
        self.server.stop()
        self._cleaned_up = True
    except Exception as e:
      LOGGER.warning("Error stopping Triton server: %s", e)
      raise

  def __del__(self):
    """Cleanup server when model is garbage collected.

    Note: __del__ is not guaranteed to be called. Prefer using cleanup()
    explicitly when possible.
    """
    if not self._cleaned_up:
      try:
        if self.server:
          self.server.stop()
      except Exception as e:
        LOGGER.warning("Error stopping Triton server in __del__: %s", e)


class TritonModelHandler(ModelHandler[Any, PredictionResult,
                                      TritonModelWrapper]):
  """Beam ModelHandler for Triton Inference Server.

  This handler supports loading models from a Triton model repository and
  running inference using the Triton Python API.

  Example usage::

    pcoll | RunInference(
      TritonModelHandler(
        model_repository="/workspace/models",
        model_name="my_model",
        input_tensor_name="input",
        output_tensor_name="output"
      )
    )

  Args:
    model_repository: Path to the Triton model repository directory.
    model_name: Name of the model to load from the repository.
    input_tensor_name: Name of the input tensor (default: "INPUT").
    output_tensor_name: Name of the output tensor (default: "OUTPUT").
    parse_output_fn: Optional custom function to parse model outputs.
      Should take (outputs_dict, output_tensor_name) and return parsed result.
  """
  def __init__(
      self,
      model_repository: str,
      model_name: str,
      input_tensor_name: str = "INPUT",
      output_tensor_name: str = "OUTPUT",
      parse_output_fn: Optional[callable] = None,
  ):
    if tritonserver is None:
      raise ImportError(
          "tritonserver is not installed. "
          "Install it with: pip install tritonserver")

    self._model_repository = model_repository
    self._model_name = model_name
    self._input_tensor_name = input_tensor_name
    self._output_tensor_name = output_tensor_name
    self._parse_output_fn = parse_output_fn

  def load_model(self) -> TritonModelWrapper:
    """Loads and initializes a Triton model for processing.

    Returns:
      TritonModelWrapper containing the server and model instances.

    Raises:
      RuntimeError: If server fails to start or model fails to load.
    """
    try:
      server = tritonserver.Server(model_repository=self._model_repository)
      server.start()
    except Exception as e:
      raise RuntimeError(
          f"Failed to start Triton server with repository "
          f"'{self._model_repository}': {e}") from e

    try:
      model = server.model(self._model_name)
      if model is None:
        raise RuntimeError(
            f"Model '{self._model_name}' not found in repository")
    except Exception as e:
      server.stop()
      raise RuntimeError(
          f"Failed to load model '{self._model_name}': {e}") from e

    return TritonModelWrapper(server, model)

  def run_inference(
      self,
      batch: Sequence[Any],
      model: TritonModelWrapper,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of inputs.

    Args:
      batch: A sequence of examples (can be strings, arrays, etc.).
      model: TritonModelWrapper returned by load_model().
      inference_args: Optional dict with 'input_tensor_name' and/or
        'output_tensor_name' to override defaults for this batch.

    Returns:
      An Iterable of PredictionResult objects.

    Raises:
      RuntimeError: If inference fails.
    """
    # Allow per-batch tensor name overrides
    input_name = self._input_tensor_name
    output_name = self._output_tensor_name
    if inference_args:
      input_name = inference_args.get('input_tensor_name', input_name)
      output_name = inference_args.get('output_tensor_name', output_name)

    try:
      responses = model.model.infer(inputs={input_name: batch})
    except Exception as e:
      raise RuntimeError(
          f"Triton inference failed for model '{self._model_name}': {e}") from e

    # Parse outputs
    predictions = []
    try:
      for response in responses:
        if output_name not in response.outputs:
          raise RuntimeError(
              f"Output tensor '{output_name}' not found in response. "
              f"Available outputs: {list(response.outputs.keys())}")

        output_tensor = response.outputs[output_name]

        # Use custom parser if provided
        if self._parse_output_fn:
          parsed = self._parse_output_fn(response.outputs, output_name)
        else:
          # Default parsing: try string array, fallback to raw
          try:
            parsed = [
                json.loads(val)
                for val in output_tensor.to_string_array().tolist()
            ]
          except Exception:
            # If JSON parsing fails, return raw output
            parsed = output_tensor.to_bytes_array().tolist()

        predictions.extend(parsed if isinstance(parsed, list) else [parsed])

    except Exception as e:
      raise RuntimeError(f"Failed to parse model outputs: {e}") from e

    if len(predictions) != len(batch):
      LOGGER.warning(
          "Prediction count (%d) doesn't match "
          "batch size (%d). Truncating or padding.",
          len(predictions),
          len(batch))

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def get_metrics_namespace(self) -> str:
    """Returns namespace for metrics."""
    return "BeamML_Triton"
