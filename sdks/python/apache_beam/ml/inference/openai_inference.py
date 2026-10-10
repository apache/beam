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

  # With system prompt and structured output passed via inference_args
  inference_args = {
      'system': 'You are a helpful assistant that responds concisely.',
      'response_format': {
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
  }

  with beam.Pipeline() as p:
    results = (
        p
        | beam.Create(['What is Apache Beam?', 'Explain MapReduce.'])
        | RunInference(model_handler, inference_args=inference_args)
    )
"""

import logging
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Optional
from typing import TypeVar

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

# Upper bound on the number of in-flight HTTP requests issued concurrently for
# a single batch by request functions that call per-item endpoints (e.g. chat
# completions). Bounds thread usage when adaptive batching produces large
# batches; use max_batch_size to further limit per-batch concurrency.
_MAX_CONCURRENT_REQUESTS_PER_BATCH = 32

_InputT = TypeVar('_InputT')
_OutputT = TypeVar('_OutputT')


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


def _fan_out(fn: Callable[[_InputT], _OutputT],
             items: Sequence[_InputT]) -> list[_OutputT]:
  """Applies fn to each item concurrently and returns results in input order.

  The synchronous OpenAI client is safe to share across threads (its
  underlying connection pool is guarded by thread locks), so per-item requests
  are dispatched on a bounded thread pool while reusing the client's pooled
  connections. If any request raises, pending requests are cancelled and the
  first exception (in input order) is propagated so Beam's retry logic can
  handle the batch.
  """
  if len(items) <= 1:
    return [fn(item) for item in items]
  executor = ThreadPoolExecutor(
      max_workers=min(len(items), _MAX_CONCURRENT_REQUESTS_PER_BATCH),
      thread_name_prefix='OpenAIModelHandler')
  try:
    return list(executor.map(fn, items))
  finally:
    executor.shutdown(wait=True, cancel_futures=True)


def chat_completion_from_string(
    model_name: str,
    batch: Sequence[str],
    client: OpenAI,
    inference_args: dict[str, Any]) -> list[Any]:
  """Request function that sends string prompts to OpenAI's Chat Completions API.

  The Chat Completions endpoint accepts a single conversation per HTTP
  request, so each string in the batch is sent as a separate user message
  request, dispatched concurrently. If a 'system' parameter is provided in
  inference_args, a system message is prepended. The results are returned as a
  list of ChatCompletion response objects matching the batch order.

  Args:
    model_name: the OpenAI model to use (e.g. 'gpt-4o', 'gpt-4o-mini').
    batch: the string prompts to send to OpenAI.
    client: the OpenAI client instance.
    inference_args: additional arguments passed to the chat.completions.create
      call (e.g. 'temperature', 'max_tokens', 'response_format', 'system').
  """
  inf_args = dict(inference_args)
  system = inf_args.pop('system', None)

  def _request(prompt: str) -> Any:
    messages: list[dict[str, Any]] = []
    if system is not None:
      messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    return client.chat.completions.create(
        model=model_name, messages=messages, **inf_args)

  return _fan_out(_request, batch)


def chat_completion_from_conversation(
    model_name: str,
    batch: Sequence[list[dict[str, Any]]],
    client: OpenAI,
    inference_args: dict[str, Any]) -> list[Any]:
  """Request function that sends multi-turn conversations to OpenAI.

  Each element in the batch is a list of message dicts (e.g. with 'role' and
  'content' keys), representing a multi-turn conversation. Conversations are
  sent as separate requests, dispatched concurrently. If a 'system' parameter
  is provided in inference_args, a system message is prepended.

  Args:
    model_name: the OpenAI model to use.
    batch: a sequence of conversations (each a list of message dicts).
    client: the OpenAI client instance.
    inference_args: additional arguments passed to the chat.completions.create
      call.
  """
  inf_args = dict(inference_args)
  system = inf_args.pop('system', None)

  def _request(conversation: list[dict[str, Any]]) -> Any:
    messages: list[dict[str, Any]] = []
    if system is not None:
      messages.append({"role": "system", "content": system})
    messages.extend(conversation)
    return client.chat.completions.create(
        model=model_name, messages=messages, **inf_args)

  return _fan_out(_request, batch)


def embedding_from_string(
    model_name: str,
    batch: Sequence[str],
    client: OpenAI,
    inference_args: dict[str, Any]) -> list[Any]:
  """Request function that sends string inputs to OpenAI's Embeddings API.

  The Embeddings endpoint natively accepts a list of inputs, so the entire
  batch is sent in a single request to ``client.embeddings.create``. The
  returned embeddings are sorted by their index to guarantee ordering matches
  the batch.

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
  return sorted(response.data, key=lambda x: x.index)


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
    conversations (with each batch's per-item requests dispatched
    concurrently), natively batched embeddings, system prompts, structured
    outputs, and custom OpenAI-compatible endpoints via `base_url`.

    A single thread-safe ``OpenAI`` client is created per worker process and
    shared across bundle-processing threads, so pooled HTTP connections are
    reused across batches.

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

    retry_filter = kwargs.pop('retry_filter', _retry_on_appropriate_error)

    super().__init__(
        namespace='OpenAIModelHandler', retry_filter=retry_filter, **kwargs)

  def batch_elements_kwargs(self):
    return self._batching_kwargs

  def create_client(self) -> OpenAI:
    """Creates the OpenAI client used to send requests.

    Sets ``max_retries=0`` by default so that Apache Beam's
    ``RemoteModelHandler`` retry and client-side throttling mechanics are the
    sole retry layer. If api_key, organization, project, base_url, or
    client_args were provided at construction time, they are passed to the
    OpenAI constructor. Otherwise, the client falls back to standard
    environment variables (OPENAI_API_KEY, OPENAI_ORG_ID, etc.).
    """
    params: dict[str, Any] = {'max_retries': 0}
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

    responses = self.request_fn(self.model_name, batch, model, inference_args)
    return utils._convert_to_result(batch, responses, self.model_name)
