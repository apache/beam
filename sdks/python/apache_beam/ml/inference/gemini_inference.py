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

import logging
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional

from google import genai
from google.genai import errors

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RemoteModelHandler

LOGGER = logging.getLogger("GeminiModelHandler")


def _retry_on_appropriate_service_error(exception: Exception) -> bool:
  """
  Retry filter that returns True if a returned HTTP error code is 5xx or 429.
  This is used to retry remote requests that fail, most notably 429
  (throttling by the service)

  Args:
    exception: the returned exception encountered during the request/response
      loop.

  Returns:
    boolean indication whether or not the exception is a ServerError (5xx) or
      a 429 error.
  """
  if not isinstance(exception, errors.APIError):
    return False
  return exception.code == 429 or exception.code >= 500


def generate_from_string(
    model_name: str,
    batch: Sequence[str],
    model: genai.Client,
    inference_args: dict[str, Any]):
  return model.models.generate_content(
      model=model_name, contents=batch, **inference_args)


class GeminiModelHandler(RemoteModelHandler[Any, PredictionResult,
                                            genai.Client]):
  def __init__(
      self,
      model_name: str,
      request_fn: Callable[[str, Sequence[Any], genai.Client, dict[str, Any]],
                           Any],
      api_key: Optional[str] = None,
      project: Optional[str] = None,
      location: Optional[str] = None,
      *,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for Google Gemini.
    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.
    Gemini can be accessed through either the Vertex AI API or the Gemini
    Developer API, and this handler chooses which to connect to based upon
    the arguments provided. As written, this model handler operates solely on
    string input.

    Args:
      model_name: the Gemini model to send the request to
      request_fn: the function to use to send the request. Should take the
        model name and the parameters from request() and return the responses
        from Gemini. The class will handle bundling the inputs and responses
        together.
      api_key: the Gemini Developer API key to use for the requests. Setting
        this parameter sends requests for this job to the Gemini Developer API.
        If this paramter is provided, do not set the project or location
        parameters.
      project: the GCP project to use for Vertex AI requests. Setting this
        parameter routes requests to Vertex AI. If this paramter is provided,
        location must also be provided and api_key should not be set.
      location: the GCP project to use for Vertex AI requests. Setting this 
        parameter routes requests to Vertex AI. If this paramter is provided,
        project must also be provided and api_key should not be set.
      min_batch_size: optional. the minimum batch size to use when batching
        inputs.
      max_batch_size: optional. the maximum batch size to use when batching
        inputs.
      max_batch_duration_secs: optional. the maximum amount of time to buffer 
        a batch before emitting; used in streaming contexts.
    """
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs["min_batch_size"] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs["max_batch_size"] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs

    self.model_name = model_name
    self.request_fn = request_fn

    if api_key:
      if project or location:
        raise ValueError("project and location must be None if api_key is set")
      self.api_key = api_key
      self.use_vertex = False
    else:
      if project is None or location is None:
        raise ValueError(
            "project and location must both be provided if api_key is None")
      self.project = project
      self.location = location
      self.use_vertex = True

    super().__init__(
        namespace='GeminiModelHandler',
        retry_filter=_retry_on_appropriate_service_error,
        **kwargs)

  def create_client(self) -> genai.Client:
    """Creates the GenAI client used to send requests. Creates a version for
    the Vertex AI API or the Gemini Developer API based on the arguments
    provided when the GeminiModelHandler class is instantiated.
    """
    if self.use_vertex:
      return genai.Client(
          vertexai=True, project=self.project, location=self.location)
    return genai.Client(api_key=self.api_key)

  def request(
      self,
      batch: Sequence[Any],
      model: genai.Client,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """ Sends a prediction request to a Gemini service containing a batch
    of inputs and matches that input with the prediction response from
    the endpoint as an iterable of PredictionResults.

    Args:
      batch: a sequence of any values to be passed to the Gemini service.
        Should be inputs accepted by the provided inference function.
      model: a genai.Client object configured to access the desired service.
      inference_args: any additional arguments to send as part of the
        prediction request.

    Returns:
      An iterable of Predictions.
    """
    if inference_args is None:
      inference_args = {}
    responses = self.request_fn(self.model_name, batch, model, inference_args)
    return utils._convert_to_result(batch, responses, self.model_name)
