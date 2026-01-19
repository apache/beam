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

"""RAG-specific embedding implementations using OpenAI models."""

from typing import Optional

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.rag.embeddings.base import create_rag_adapter
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler
from apache_beam.ml.transforms.embeddings.open_ai import _OpenAITextEmbeddingHandler

__all__ = ['OpenAITextEmbeddings']


class OpenAITextEmbeddings(EmbeddingsManager):
  def __init__(
      self,
      model_name: str,
      *,
      api_key: Optional[str] = None,
      organization: Optional[str] = None,
      dimensions: Optional[int] = None,
      user: Optional[str] = None,
      max_batch_size: Optional[int] = None,
      **kwargs):
    """Utilizes OpenAI text embeddings for semantic search and RAG pipelines.

        Args:
            model_name: Name of the OpenAI embedding model
            api_key: OpenAI API key
            organization: OpenAI organization ID
            dimensions: Specific embedding dimensions to use (if supported)
            user: End-user identifier for tracking and rate limit calculations
            max_batch_size: Maximum batch size for requests to OpenAI API
            **kwargs: Additional arguments passed to EmbeddingsManager including
            ModelHandler inference_args.
        """
    super().__init__(type_adapter=create_rag_adapter(), **kwargs)
    self.model_name = model_name
    self.api_key = api_key
    self.organization = organization
    self.dimensions = dimensions
    self.user = user
    self.max_batch_size = max_batch_size

  def get_model_handler(self):
    """Returns model handler configured with RAG adapter."""
    return _OpenAITextEmbeddingHandler(
        model_name=self.model_name,
        api_key=self.api_key,
        organization=self.organization,
        dimensions=self.dimensions,
        user=self.user,
        max_batch_size=self.max_batch_size,
    )

  def get_ptransform_for_processing(
      self, **kwargs
  ) -> beam.PTransform[beam.PCollection[Chunk], beam.PCollection[Chunk]]:
    """Returns PTransform that uses the RAG adapter."""
    return RunInference(
        model_handler=_TextEmbeddingHandler(self),
        inference_args=self.inference_args).with_output_types(Chunk)
