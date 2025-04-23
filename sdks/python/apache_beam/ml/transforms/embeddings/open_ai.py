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

import logging
import time
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import TypeVar
from typing import Union
from typing import cast

import apache_beam as beam
from apache_beam.pvalue import PCollection
from apache_beam.pvalue import Row
from apache_beam.io.components.adaptive_throttler import AdaptiveThrottler
from apache_beam.metrics.metric import Metrics
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler
from apache_beam.utils import retry

import openai
from openai import RateLimitError
from openai import APIError

__all__ = ["OpenAITextEmbeddings"]

# Define a type variable for the output
MLTransformOutputT = TypeVar('MLTransformOutputT')

_MSEC_TO_SEC = 1000
_BATCH_SIZE = 20  # OpenAI can handle larger batches than Vertex

LOGGER = logging.getLogger("OpenAIEmbeddings")


def _retry_on_appropriate_openai_error(exception):
  """
  Retry filter that returns True if a returned error is rate limit (429) or server error (5xx).

  Args:
    exception: the returned exception encountered during the request/response
      loop.

  Returns:
    boolean indication whether or not the exception is a Server Error (5xx) or
      a RateLimitError (429) error.
  """
  return isinstance(exception, (RateLimitError, APIError))


class _OpenAITextEmbeddingHandler(ModelHandler):
  """
  Note: Intended for internal use and guarantees no backwards compatibility.
  """

  def __init__(
      self,
      model_name: str,
      api_key: Optional[str] = None,
      organization: Optional[str] = None,
      dimensions: Optional[int] = None,
      user: Optional[str] = None,
      batch_size: Optional[int] = None,
  ):
    self.model_name = model_name
    self.api_key = api_key
    self.organization = organization
    self.dimensions = dimensions
    self.user = user
    self.batch_size = batch_size or _BATCH_SIZE

    # Configure AdaptiveThrottler and throttling metrics for client-side
    # throttling behavior.
    self.throttled_secs = Metrics.counter(
        OpenAITextEmbeddings, "cumulativeThrottlingSeconds")
    self.throttler = AdaptiveThrottler(
        window_ms=1, bucket_ms=1, overload_ratio=2)

  @retry.with_exponential_backoff(
      num_retries=5, retry_filter=_retry_on_appropriate_openai_error)
  def get_request(
      self, text_batch: Sequence[str], model: Any, throttle_delay_secs: int):
    while self.throttler.throttle_request(time.time() * _MSEC_TO_SEC):
      LOGGER.info(
          "Delaying request for %d seconds due to previous failures",
          throttle_delay_secs)
      time.sleep(throttle_delay_secs)
      self.throttled_secs.inc(throttle_delay_secs)

    try:
      req_time = time.time()
      kwargs = {
          "model": self.model_name,
          "input": text_batch,
      }
      if self.dimensions:
        kwargs["dimensions"] = self.dimensions
      if self.user:
        kwargs["user"] = self.user

      response = model.embeddings.create(**kwargs)
      self.throttler.successful_request(req_time * _MSEC_TO_SEC)
      return [item.embedding for item in response.data]
    except RateLimitError as e:
      LOGGER.warning("Request was rate limited by OpenAI API")
      raise
    except Exception as e:
      LOGGER.error("Unexpected exception raised as part of request: %s", e)
      raise

  def batch_elements_kwargs(self) -> dict[str, Any]:
    """Returns kwargs suitable for beam.BatchElements with appropriate batch size."""
    return {'max_batch_size': self.batch_size}

  def run_inference(
      self,
      batch: Sequence[str],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None,
  ) -> Iterable:
    embeddings = []
    batch_size = self.batch_size
    for i in range(0, len(batch), batch_size):
      text_batch = batch[i:i + batch_size]
      embeddings_batch = self.get_request(
          text_batch=text_batch, model=model, throttle_delay_secs=5)
      embeddings.extend(embeddings_batch)
    return embeddings

  def load_model(self):
    if self.api_key:
      client = openai.OpenAI(
          api_key=self.api_key,
          organization=self.organization,
      )
    else:
      # Use environment variables or default configuration
      client = openai.OpenAI(organization=self.organization)

    return client

  def __repr__(self):
    return 'OpenAITextEmbeddings'


class OpenAITextEmbeddings(EmbeddingsManager):
  """
  A PTransform that uses OpenAI's API to generate embeddings from text inputs.

  Example Usage::

      with pipeline as p:
          text = p | "Create texts" >> beam.Create([{"text": "Hello world"}, {"text": "Beam ML"}])
          embeddings = text | OpenAITextEmbeddings(
              model_name="text-embedding-3-small",
              columns=["embedding_col"],
              api_key=api_key
          )
  """

  @beam.typehints.with_output_types(PCollection[Union[MLTransformOutputT, Row]])
  def __init__(
      self,
      model_name: str,
      columns: list[str],
      api_key: Optional[str] = None,
      organization: Optional[str] = None,
      dimensions: Optional[int] = None,
      user: Optional[str] = None,
      batch_size: Optional[int] = None,
      **kwargs):
    """
    Embedding Config for OpenAI Text Embedding models.
    Text Embeddings are generated for a batch of text using the OpenAI API.
    
    Args:
      model_name: Name of the OpenAI embedding model (e.g., "text-embedding-3-small")
      columns: The columns where the embeddings will be stored in the output
      api_key: OpenAI API key
      organization: OpenAI organization ID
      dimensions: Specific embedding dimensions to use (if model supports it)
      user: End-user identifier for tracking and rate limit calculations
      batch_size: Maximum batch size for requests to OpenAI API (default: 20)
    """
    self.model_name = model_name
    self.api_key = api_key
    self.organization = organization
    self.dimensions = dimensions
    self.user = user
    self.batch_size = batch_size
    super().__init__(columns=columns, **kwargs)

  def get_model_handler(self) -> ModelHandler:
    return _OpenAITextEmbeddingHandler(
        model_name=self.model_name,
        api_key=self.api_key,
        organization=self.organization,
        dimensions=self.dimensions,
        user=self.user,
        batch_size=self.batch_size,
    )

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    return RunInference(
        model_handler=_TextEmbeddingHandler(self),
        inference_args=self.inference_args)
