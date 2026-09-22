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

"""A ModelHandler for OpenAI models using the OpenAI Python SDK.

This module provides an integration between Apache Beam's RunInference
transform and OpenAI's API, enabling batch inference and embeddings in
Beam pipelines.

Example usage::

  import apache_beam as beam
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference.openai_inference import (
      OpenAIModelHandler,
      chat_completion_from_string,
  )

  # Basic text generation with chat completions
  model_handler = OpenAIModelHandler(
      model_name='gpt-4o-mini',
      api_key='your-api-key',
      request_fn=chat_completion_from_string,
  )

  # With system prompt and structured output
  model_handler = OpenAIModelHandler(
      model_name='gpt-4o-mini',
      api_key='your-api-key',
      request_fn=chat_completion_from_string,
      system='You are a helpful assistant that responds concisely.',
      response_format={
          'type': 'json_schema',
          'json_schema': {
              'name': 'answer_response',
              'schema': {
                  'type': 'object',
                  'properties': {
                      'answer': {'type': 'string'},
                      'confidence': {'type': 'number'},
                  },
                  'required': ['answer', 'confidence'],
                  'additionalProperties': False,
              },
              'strict': True,
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

from openai import APIConnectionError
from openai import APIStatusError
from openai import OpenAI

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RemoteModelHandler

__all__ = [
    'OpenAIModelHandler',
    'chat_completion_from_string',
    'chat_completion_from_conversation',
    'embedding_from_string',
]

LOGGER = logging.getLogger("OpenAIModelHandler")


def _retry_on_appropriate_error(exception: Exception) -> bool:
  """Retry filter that returns True for retriable OpenAI API errors.

  Retries on HTTP 429 (rate limiting), HTTP 5xx (server errors), and
  connection / timeout errors.

  Args:
    exception: the exception encountered during the request/response loop.

  Returns:
    True if the exception is retriable (429, 5xx, or connection error),
    False otherwise.
  """
  if isinstance(exception, APIConnectionError):
    return True
  if isinstance(exception, APIStatusError):
    return exception.status_code == 429 or exception.status_code >= 500
  return False


def chat_completion_from_string(
    model_name: str,
    batch: Sequence[str],
    client: OpenAI,
    inference_args: dict[str, Any]) -> list[Any]:
  """Request function that sends string prompts to OpenAI's Chat Completions API.

  Each string in the batch is sent as a user message. If a 'system' parameter
  is provided in inference_args, a system message is prepended. The results
  are returned as a list of ChatCompletion response objects.

  Args:
    model_name: the OpenAI model to use (e.g. 'gpt-4o', 'gpt-4o-mini').
    batch: the string prompts to send to OpenAI.
    client: the OpenAI client instance.
    inference_args: additional arguments passed to the chat.completions.create
      call (e.g. 'temperature', 'max_tokens', 'response_format', 'system').
  """
  inf_args = dict(inference_args)
  system = inf_args.pop('system', None)
  responses = []
  for prompt in batch:
    messages: list[dict[str, Any]] = []
    if system is not None:
      messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    response = client.chat.completions.create(
        model=model_name, messages=messages, **inf_args)
    responses.append(response)
  return responses


def chat_completion_from_conversation(
    model_name: str,
    batch: Sequence[list[dict[str, Any]]],
    client: OpenAI,
    inference_args: dict[str, Any]) -> list[Any]:
  """Request function that sends multi-turn conversations to OpenAI.

  Each element in the batch is a list of message dicts (e.g. with 'role' and
  'content' keys), representing a multi-turn conversation. If a 'system'
  parameter is provided in inference_args, a system message is prepended.

  Args:
    model_name: the OpenAI model to use.
    batch: a sequence of conversations (each a list of message dicts).
    client: the OpenAI client instance.
    inference_args: additional arguments passed to the chat.completions.create
      call.
  """
  inf_args = dict(inference_args)
  system = inf_args.pop('system', None)
  responses = []
  for conversation in batch:
    messages: list[dict[str, Any]] = []
    if system is not None:
      messages.append({"role": "system", "content": system})
    messages.extend(conversation)
    response = client.chat.completions.create(
        model=model_name, messages=messages, **inf_args)
    responses.append(response)
  return responses


def embedding_from_string(
    model_name: str,
    batch: Sequence[str],
    client: OpenAI,
    inference_args: dict[str, Any]) -> list[Any]:
  """Request function that sends string inputs to OpenAI's Embeddings API.

  The batch of string inputs is sent to client.embeddings.create. The returned
  embeddings are sorted by their index to guarantee ordering matches the batch.

  Args:
    model_name: the OpenAI embedding model to use (e.g. 'text-embedding-3-small').
    batch: the string inputs to embed.
    client: the OpenAI client instance.
    inference_args: additional arguments passed to the embeddings.create call
      (e.g. 'dimensions', 'encoding_format').

  Returns:
    A list of Embedding objects matching the batch order.
  """
  inf_args = dict(inference_args)
  response = client.embeddings.create(
      model=model_name, input=list(batch), **inf_args)
  sorted_data = sorted(response.data, key=lambda x: x.index)
  return sorted_data


class OpenAIModelHandler(RemoteModelHandler[Any, PredictionResult, OpenAI]):
  def __init__(
      self,
      model_name: str,
      request_fn: Callable[[str, Sequence[Any], OpenAI, dict[str, Any]], Any],
      api_key: Optional[str] = None,
      *,
      organization: Optional[str] = None,
      project: Optional[str] = None,
      base_url: Optional[str] = None,
      client_args: Optional[dict[str, Any]] = None,
      system: Optional[str] = None,
      response_format: Optional[dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      batch_length_fn: Optional[Callable[[Any], int]] = None,
      batch_bucket_boundaries: Optional[list[int]] = None,
      **kwargs):
    """Implementation of the ModelHandler interface for OpenAI models.

    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.

    This handler connects to the OpenAI API using the OpenAI Python SDK
    to run inference using models such as GPT-4o, GPT-4o-mini, or embedding
    models. It supports chat completions from string prompts or multi-turn
    conversations, embeddings, system prompts, structured outputs, and
    custom OpenAI-compatible endpoints via `base_url`.

    Args:
      model_name: the OpenAI model to send requests to (e.g.
        'gpt-4o', 'gpt-4o-mini', 'text-embedding-3-small').
      request_fn: the function to use to send requests. Should take the
        model name, batch, client, and inference_args and return the
        responses from OpenAI. Built-in options are chat_completion_from_string,
        chat_completion_from_conversation, and embedding_from_string.
      api_key: the OpenAI API key. If not provided, the client will
        look for the OPENAI_API_KEY environment variable.
      organization: optional OpenAI organization ID. If not provided,
        the client will look for the OPENAI_ORG_ID environment variable.
      project: optional OpenAI project ID. If not provided, the client
        will look for the OPENAI_PROJECT_ID environment variable.
      base_url: optional base URL for the API requests. This can be used
        to target OpenAI-compatible servers (such as vLLM, Ollama, or
        Azure OpenAI endpoints).
      client_args: optional dictionary of additional keyword arguments
        passed when instantiating the OpenAI client (e.g. timeout,
        default_headers).
      system: optional system prompt to set the model's behavior for all
        requests. Per-request overrides can be passed via inference_args.
      response_format: optional response format specification (e.g.
        `{'type': 'json_object'}` or structured outputs schema) to
        constrain responses. Per-request overrides can be passed via
        inference_args.
      min_batch_size: optional. the minimum batch size to use when
        batching inputs.
      max_batch_size: optional. the maximum batch size to use when
        batching inputs.
      max_batch_duration_secs: optional. the maximum amount of time to
        buffer a batch before emitting; used in streaming contexts.
      max_batch_weight: optional. the maximum total weight of a batch.
      element_size_fn: optional. a function that returns the size
        (weight) of an element.
      batch_length_fn: optional. a callable that returns the length of an
        element for length-aware batching.
      batch_bucket_boundaries: optional. a sorted list of positive boundary
        values for length-aware batching buckets.
      **kwargs: optional arguments passed to RemoteModelHandler (e.g.
        num_retries, throttle_delay_secs, rate_limiter).
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
    if batch_length_fn is not None:
      self._batching_kwargs['length_fn'] = batch_length_fn
    if batch_bucket_boundaries is not None:
      self._batching_kwargs['bucket_boundaries'] = batch_bucket_boundaries

    self.model_name = model_name
    self.request_fn = request_fn
    self.api_key = api_key
    self.organization = organization
    self.project = project
    self.base_url = base_url
    self.client_args = client_args
    self.system = system
    self.response_format = response_format

    retry_filter = kwargs.pop('retry_filter', _retry_on_appropriate_error)

    super().__init__(
        namespace='OpenAIModelHandler', retry_filter=retry_filter, **kwargs)

  def batch_elements_kwargs(self):
    return self._batching_kwargs

  def create_client(self) -> OpenAI:
    """Creates the OpenAI client used to send requests.

    If api_key, organization, project, base_url, or client_args were
    provided at construction time, they are passed to the OpenAI constructor.
    Otherwise, the client falls back to standard environment variables
    (OPENAI_API_KEY, OPENAI_ORG_ID, etc.).
    """
    params: dict[str, Any] = {}
    if self.api_key is not None:
      params['api_key'] = self.api_key
    if self.organization is not None:
      params['organization'] = self.organization
    if self.project is not None:
      params['project'] = self.project
    if self.base_url is not None:
      params['base_url'] = self.base_url
    if self.client_args is not None:
      params.update(self.client_args)
    return OpenAI(**params)

  def request(
      self,
      batch: Sequence[Any],
      model: OpenAI,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Sends a prediction request to the OpenAI API.

    Handler-level system and response_format are injected into
    inference_args before calling the request function. Per-request
    values in inference_args take precedence over handler-level values.

    Args:
      batch: a sequence of inputs to be passed to the request function.
      model: an OpenAI client instance.
      inference_args: additional arguments to send as part of the
        prediction request (e.g. temperature, max_tokens, system,
        response_format).

    Returns:
      An iterable of PredictionResults.
    """
    if inference_args is None:
      inference_args = {}
    else:
      inference_args = dict(inference_args)

    if self.system is not None and 'system' not in inference_args:
      inference_args['system'] = self.system
    if self.response_format is not None and 'response_format' not in inference_args:
      inference_args['response_format'] = self.response_format

    responses = self.request_fn(self.model_name, batch, model, inference_args)
    return utils._convert_to_result(batch, responses, self.model_name)
