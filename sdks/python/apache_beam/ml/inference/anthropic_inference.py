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

"""A ModelHandler for Anthropic Claude models using the Messages API.

This module provides an integration between Apache Beam's RunInference
transform and Anthropic's Claude models, enabling batch inference in
Beam pipelines.

Example usage::

  from apache_beam.ml.inference.anthropic_inference import (
      AnthropicModelHandler,
      message_from_string,
  )
  from apache_beam.ml.inference.base import RunInference

  # Basic usage
  model_handler = AnthropicModelHandler(
      model_name='claude-haiku-4-5',
      api_key='your-api-key',
      request_fn=message_from_string,
  )

  # With system prompt and structured output
  model_handler = AnthropicModelHandler(
      model_name='claude-haiku-4-5',
      api_key='your-api-key',
      request_fn=message_from_string,
      system='You are a helpful assistant that responds concisely.',
      output_config={
          'format': {
              'type': 'json_schema',
              'schema': {
                  'type': 'object',
                  'properties': {
                      'answer': {'type': 'string'},
                      'confidence': {'type': 'number'},
                  },
                  'required': ['answer', 'confidence'],
                  'additionalProperties': False,
              },
          },
      },
  )

  with beam.Pipeline() as p:
    results = (
        p
        | beam.Create(['What is Apache Beam?', 'Explain MapReduce.'])
        | RunInference(model_handler)
    )
"""

import logging
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import Union

from anthropic import Anthropic
from anthropic import APIStatusError

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RemoteModelHandler

LOGGER = logging.getLogger("AnthropicModelHandler")


def _retry_on_appropriate_error(exception: Exception) -> bool:
  """Retry filter that returns True for 5xx errors or 429 (rate limiting).

  Args:
    exception: the exception encountered during the request/response loop.

  Returns:
    True if the exception is retriable (429 or 5xx), False otherwise.
  """
  if not isinstance(exception, APIStatusError):
    return False
  return exception.status_code == 429 or exception.status_code >= 500


def message_from_string(
    model_name: str,
    batch: Sequence[str],
    client: Anthropic,
    inference_args: dict[str, Any]):
  """Request function that sends string prompts to Claude as user messages.

  Each string in the batch is sent as a separate request. The results are
  returned as a list of response objects.

  Args:
    model_name: the Claude model to use (e.g. 'claude-haiku-4-5').
    batch: the string inputs to send to Claude.
    client: the Anthropic client instance.
    inference_args: additional arguments passed to the messages.create call.
      Common args include 'max_tokens', 'system', 'temperature', 'top_p'.
  """
  max_tokens = inference_args.pop('max_tokens', 1024)
  responses = []
  for prompt in batch:
    response = client.messages.create(
        model=model_name,
        max_tokens=max_tokens,
        messages=[{
            "role": "user", "content": prompt
        }],
        **inference_args)
    responses.append(response)
  return responses


def message_from_conversation(
    model_name: str,
    batch: Sequence[list[dict[str, str]]],
    client: Anthropic,
    inference_args: dict[str, Any]):
  """Request function that sends multi-turn conversations to Claude.

  Each element in the batch is a list of message dicts with 'role' and
  'content' keys, representing a multi-turn conversation.

  Args:
    model_name: the Claude model to use.
    batch: a sequence of conversations (each a list of message dicts).
    client: the Anthropic client instance.
    inference_args: additional arguments passed to the messages.create call.
  """
  max_tokens = inference_args.pop('max_tokens', 1024)
  responses = []
  for messages in batch:
    response = client.messages.create(
        model=model_name,
        max_tokens=max_tokens,
        messages=messages,
        **inference_args)
    responses.append(response)
  return responses


class AnthropicModelHandler(RemoteModelHandler[Any, PredictionResult,
                                               Anthropic]):
  def __init__(
      self,
      model_name: str,
      request_fn: Callable[[str, Sequence[Any], Anthropic, dict[str, Any]],
                           Any],
      api_key: Optional[str] = None,
      *,
      system: Optional[Union[str, list[dict[str, str]]]] = None,
      output_config: Optional[dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for Anthropic Claude.

    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.

    This handler connects to the Anthropic Messages API to run inference
    using Claude models. It supports text generation from string prompts
    or multi-turn conversations, with optional system prompts and
    structured output schemas.

    Args:
      model_name: the Claude model to send requests to (e.g.
        'claude-sonnet-4-6', 'claude-haiku-4-5').
      request_fn: the function to use to send requests. Should take the
        model name, batch, client, and inference_args and return the
        responses from Claude. Built-in options are message_from_string
        and message_from_conversation.
      api_key: the Anthropic API key. If not provided, the client will
        look for the ANTHROPIC_API_KEY environment variable.
      system: optional system prompt to set the model's behavior for all
        requests. Can be a string or a list of content blocks (dicts
        with 'type' and 'text' keys). This is applied to every request
        in the pipeline. Per-request overrides can be passed via
        inference_args.
      output_config: optional output configuration to constrain
        responses to a structured schema. The value is passed directly
        to the Anthropic API as the 'output_config' parameter. This
        uses the GA API shape with a nested 'format' key. Example::

          output_config={
              'format': {
                  'type': 'json_schema',
                  'schema': {
                      'type': 'object',
                      'properties': {
                          'answer': {'type': 'string'},
                      },
                      'required': ['answer'],
                      'additionalProperties': False,
                  },
              },
          }

      min_batch_size: optional. the minimum batch size to use when
        batching inputs.
      max_batch_size: optional. the maximum batch size to use when
        batching inputs.
      max_batch_duration_secs: optional. the maximum amount of time to
        buffer a batch before emitting; used in streaming contexts.
      max_batch_weight: optional. the maximum total weight of a batch.
      element_size_fn: optional. a function that returns the size
        (weight) of an element.
    """
    self._batching_kwargs = {}
    self._env_vars = kwargs.get('env_vars', {})
    if min_batch_size is not None:
      self._batching_kwargs["min_batch_size"] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs["max_batch_size"] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs
    if max_batch_weight is not None:
      self._batching_kwargs["max_batch_weight"] = max_batch_weight
    if element_size_fn is not None:
      self._batching_kwargs['element_size_fn'] = element_size_fn

    self.model_name = model_name
    self.request_fn = request_fn
    self.api_key = api_key
    self.system = system
    self.output_config = output_config

    super().__init__(
        namespace='AnthropicModelHandler',
        retry_filter=_retry_on_appropriate_error,
        **kwargs)

  def batch_elements_kwargs(self):
    return self._batching_kwargs

  def create_client(self) -> Anthropic:
    """Creates the Anthropic client used to send requests.

    If api_key was provided at construction time, it is used directly.
    Otherwise, the client will fall back to the ANTHROPIC_API_KEY
    environment variable.
    """
    if self.api_key:
      return Anthropic(api_key=self.api_key)
    return Anthropic()

  def request(
      self,
      batch: Sequence[Any],
      model: Anthropic,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Sends a prediction request to the Anthropic API.

    Handler-level system and output_config are injected into
    inference_args before calling the request function. Per-request
    values in inference_args take precedence over handler-level values.

    Args:
      batch: a sequence of inputs to be passed to the request function.
      model: an Anthropic client instance.
      inference_args: additional arguments to send as part of the
        prediction request (e.g. max_tokens, temperature, system).

    Returns:
      An iterable of PredictionResults.
    """
    if inference_args is None:
      inference_args = {}
    if self.system is not None and 'system' not in inference_args:
      inference_args['system'] = self.system
    if self.output_config is not None and 'output_config' not in inference_args:
      inference_args['output_config'] = self.output_config
    responses = self.request_fn(self.model_name, batch, model, inference_args)
    return utils._convert_to_result(batch, responses, self.model_name)
