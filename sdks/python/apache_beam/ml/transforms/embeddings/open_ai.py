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
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import TypeVar
from typing import Union

import apache_beam as beam
import openai
from apache_beam.ml.inference.base import RemoteModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler
from apache_beam.pvalue import PCollection
from apache_beam.pvalue import Row
from openai import APIError
from openai import RateLimitError

__all__ = ["OpenAITextEmbeddings"]

# Define a type variable for the output
MLTransformOutputT = TypeVar('MLTransformOutputT')

# Default batch size for OpenAI API requests
_DEFAULT_BATCH_SIZE = 20

LOGGER = logging.getLogger("OpenAIEmbeddings")


def _retry_on_appropriate_openai_error(exception):
  """
  Retry filter that returns True for rate limit (429) or server (5xx) errors.

  Args:
    exception: the returned exception encountered during the request/response
      loop.

  Returns:
    boolean indication whether or not the exception is a Server Error (5xx) or
      a RateLimitError (429) error.
  """
  return isinstance(exception, (RateLimitError, APIError))


class _OpenAITextEmbeddingHandler(RemoteModelHandler):
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
      max_batch_size: Optional[int] = None,
  ):
    super().__init__(
        namespace="OpenAITextEmbeddings",
        num_retries=5,
        throttle_delay_secs=5,
        retry_filter=_retry_on_appropriate_openai_error)
    self.model_name = model_name
    self.api_key = api_key
    self.organization = organization
    self.dimensions = dimensions
    self.user = user
    self.max_batch_size = max_batch_size or _DEFAULT_BATCH_SIZE

  def create_client(self):
    """Creates and returns an OpenAI client."""
    if self.api_key:
      client = openai.OpenAI(
          api_key=self.api_key,
          organization=self.organization,
      )
    else:
      client = openai.OpenAI(organization=self.organization)

    return client

  def request(
      self,
      batch: Sequence[str],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None,
  ) -> Iterable:
    """Makes a request to OpenAI embedding API and returns embeddings."""
    # Prepare arguments for the API call
    kwargs = {
        "model": self.model_name,
        "input": batch,
    }
    if self.dimensions:
      kwargs["dimensions"] = [str(self.dimensions)]
    if self.user:
      kwargs["user"] = self.user

    # Make the API call - let RemoteModelHandler handle retries and exceptions
    response = model.embeddings.create(**kwargs)
    return [item.embedding for item in response.data]

  def batch_elements_kwargs(self) -> dict[str, Any]:
    """Return kwargs suitable for BatchElements with appropriate batch size"""
    return {'max_batch_size': self.max_batch_size}

  def __repr__(self):
    return 'OpenAITextEmbeddings'


class OpenAITextEmbeddings(EmbeddingsManager):
  @beam.typehints.with_output_types(PCollection[Union[MLTransformOutputT, Row]])
  def __init__(
      self,
      model_name: str,
      columns: list[str],
      api_key: Optional[str] = None,
      organization: Optional[str] = None,
      dimensions: Optional[int] = None,
      user: Optional[str] = None,
      max_batch_size: Optional[int] = None,
      **kwargs):
    """
    Embedding Config for OpenAI Text Embedding models.
    Text Embeddings are generated for a batch of text using the OpenAI API.
    
    Args:
      model_name: Name of the OpenAI embedding model
      columns: The columns where the embeddings will be stored in the output
      api_key: OpenAI API key
      organization: OpenAI organization ID
      dimensions: Specific embedding dimensions to use (if model supports it)
      user: End-user identifier for tracking and rate limit calculations
      max_batch_size: Maximum batch size for requests to OpenAI API
    """
    self.model_name = model_name
    self.api_key = api_key
    self.organization = organization
    self.dimensions = dimensions
    self.user = user
    self.max_batch_size = max_batch_size
    super().__init__(columns=columns, **kwargs)

  def get_model_handler(self) -> RemoteModelHandler:
    return _OpenAITextEmbeddingHandler(
        model_name=self.model_name,
        api_key=self.api_key,
        organization=self.organization,
        dimensions=self.dimensions,
        user=self.user,
        max_batch_size=self.max_batch_size,
    )

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    return RunInference(
        model_handler=_TextEmbeddingHandler(self),
        inference_args=self.inference_args)
