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

# pylint: disable=wrong-import-order, wrong-import-position
try:
  import openai
  from openai import APIError
  from openai import OpenAIError
  from openai import RateLimitError
except ImportError:
  raise ImportError(
      'OpenAI dependencies are not installed. To use OpenAI model handler,'
      'run pip install apache-beam[gcp,openai]')

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RemoteModelHandler

LOGGER = logging.getLogger("OpenAIModelHandler")


def _retry_on_appropriate_openai_error(exception: Exception) -> bool:
  """
  Retry filter that returns True if a returned HTTP error code is 5xx or 429
  (RateLimitError).
  """
  LOGGER.debug(
      f"Checking exception for retry: {type(exception)} - {str(exception)}")
  if isinstance(exception, RateLimitError):
    LOGGER.debug("RateLimitError detected, retrying.")
    return True  # Always retry RateLimitError (HTTP 429)

  if isinstance(exception, APIError):  # This covers APIStatusError as well
    status_code = getattr(exception, 'status_code', None)
    LOGGER.debug(f"APIError detected. Status code from getattr: {status_code}")
    if status_code is not None:
      LOGGER.debug(
          f"Condition check: {status_code} >= 500 is {status_code >= 500}")
      return status_code >= 500  # Retry on 5xx errors
    else:
      LOGGER.debug("APIError but status_code is None.")

  LOGGER.debug("Exception not eligible for retry by this filter.")
  return False  # Do not retry for other errors or if status_code is not available


def generate_completion(
    model_name: str,
    batch: Sequence[str],
    client: openai.OpenAI,
    inference_args: dict[str, Any]):
  """
  Generates completions for a batch of prompts using the OpenAI API.
  """
  responses = []
  for prompt in batch:
    try:
      # Note: OpenAI's library expects a single prompt for completions.create,
      # so we iterate and call. Batching is handled by RunInference.
      # For chat models, multiple messages can be part of a single request.
      if "chat.completions" in client.chat.completions.with_raw_response.create.binary_relative_path:  # rough check
        # Assuming chat model if path indicates chat completions
        # User might need to format input as list of messages
        # For simplicity, we'll assume a single user message per prompt string
        # for now.
        if not isinstance(prompt, list):  # basic check for message format
          messages = [{"role": "user", "content": prompt}]
        else:  # assume prompt is already in message format
          messages = prompt
        response = client.chat.completions.create(
            model=model_name, messages=messages, **inference_args)
      else:
        response = client.completions.create(
            model=model_name, prompt=prompt, **inference_args)
      responses.append(response)
    except OpenAIError as e:
      # Capture individual errors to potentially return partial results
      # or raise a combined error. For now, let it propagate to be caught
      # by the RemoteModelHandler's retry logic.
      LOGGER.error("OpenAI API error for prompt '%s': %s", prompt, e)
      raise e

  # Parse responses within the generate_completion function
  parsed_responses = []
  for response_obj in responses:
    if hasattr(response_obj, 'choices'):
      if response_obj.choices:
        # For ChatCompletion, the message is nested
        if hasattr(response_obj.choices[0], 'message') and \
            hasattr(response_obj.choices[0].message, 'content'):
          parsed_responses.append(response_obj.choices[0].message.content)
        # For Completion (older models)
        elif hasattr(response_obj.choices[0], 'text'):
          parsed_responses.append(response_obj.choices[0].text)
        else:
          LOGGER.warning(
              "Unrecognized OpenAI response choice format: %s",
              response_obj.choices[0])
          parsed_responses.append(None)  # Or raise error
      else:
        LOGGER.warning("OpenAI response had no choices: %s", response_obj)
        parsed_responses.append(None)  # Or raise error
    else:
      LOGGER.warning("Unrecognized OpenAI response format: %s", response_obj)
      parsed_responses.append(None)  # Or raise error
  return parsed_responses


class OpenAIModelHandler(RemoteModelHandler[Any,
                                            PredictionResult,
                                            openai.OpenAI]):
  def __init__(
      self,
      api_key: str,
      model: str,  # Recommended to use 'model' like in openai library
      request_fn: Callable[[str, Sequence[Any], openai.OpenAI, dict[str, Any]],
                           Any] = generate_completion,
      *,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for OpenAI.
    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.

    Args:
      api_key: the OpenAI API key to use for the requests.
      model: The OpenAI model to use for inference (e.g., "gpt-3.5-turbo-instruct", "gpt-3.5-turbo").
      request_fn: the function to use to send the request. Should take the
        model name and the parameters from request() and return the responses
        from OpenAI. The class will handle bundling the inputs and responses
        together. Defaults to `generate_completion`.
      min_batch_size: optional. the minimum batch size to use when batching
        inputs.
      max_batch_size: optional. the maximum batch size to use when batching
        inputs.
      max_batch_duration_secs: optional. the maximum amount of time to buffer
        a batch before emitting; used in streaming contexts.
      kwargs: Other arguments to pass to the underlying RemoteModelHandler.
    """
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs["min_batch_size"] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs["max_batch_size"] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs

    self.api_key = api_key
    self.model_name = model  # Renamed from model_name to model for consistency
    self.request_fn = request_fn

    # OpenAI client will be initialized in create_client
    self._client: Optional[openai.OpenAI] = None

    super().__init__(
        namespace='OpenAIModelHandler',
        retry_filter=_retry_on_appropriate_openai_error,
        **kwargs)

  def create_client(self) -> openai.OpenAI:
    """Creates the OpenAI client used to send requests."""
    if not self._client:
      self._client = openai.OpenAI(api_key=self.api_key)
    return self._client

  def request(
      self,
      batch: Sequence[Any],
      model_client: openai.OpenAI,  # Parameter name changed for clarity
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """ Sends a prediction request to an OpenAI model containing a batch
    of inputs and matches that input with the prediction response from
    the endpoint as an iterable of PredictionResults.

    Args:
      batch: a sequence of any values to be passed to the OpenAI model.
        Should be inputs accepted by the provided request_fn.
      model_client: an openai.OpenAI client object.
      inference_args: any additional arguments to send as part of the
        prediction request to OpenAI (e.g., temperature, max_tokens).

    Returns:
      An iterable of PredictionResults.
    """
    if inference_args is None:
      inference_args = {}

    # The `generate_completion` function now iterates through the batch
    # and makes individual API calls if necessary (e.g. for non-chat models)
    # or a single call if the underlying API supports batching (e.g. future chat models).
    # The RunInference transform handles the primary batching of elements from PCollection.
    try:
      # request_fn (generate_completion) now returns a list of parsed strings/content
      parsed_responses = self.request_fn(
          self.model_name, batch, model_client, inference_args)
    except Exception as e:
      LOGGER.error(
          "Error during OpenAI request for batch: %s. Error: %s", batch, e)
      # Propagate the error to allow RemoteModelHandler's retry logic to kick in
      raise

    return utils._convert_to_result(batch, parsed_responses, self.model_name)

  def batch_elements_kwargs(self) -> dict[str, Any]:
    return self._batching_kwargs

  def validate_inference_args(self, inference_args: Optional[dict[str, Any]]):
    # OpenAI's API takes various arguments, most are optional.
    # No specific validation needed at this level for common args like
    # temperature, max_tokens, etc. The API itself will validate.
    pass
