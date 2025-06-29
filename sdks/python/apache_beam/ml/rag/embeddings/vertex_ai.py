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


# Vertex AI Python SDK is required for this module.
# Follow https://cloud.google.com/vertex-ai/docs/python-sdk/use-vertex-ai-python-sdk # pylint: disable=line-too-long
# to install Vertex AI Python SDK.

"""RAG-specific embedding implementations using Vertex AI models."""

from typing import Optional

from google.auth.credentials import Credentials

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.rag.embeddings.base import create_rag_adapter
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler
from apache_beam.ml.transforms.embeddings.vertex_ai import DEFAULT_TASK_TYPE
from apache_beam.ml.transforms.embeddings.vertex_ai import _VertexAITextEmbeddingHandler

try:
  import vertexai
except ImportError:
  vertexai = None  # type: ignore[assignment]


class VertexAITextEmbeddings(EmbeddingsManager):
  def __init__(
      self,
      model_name: str,
      *,
      title: Optional[str] = None,
      task_type: str = DEFAULT_TASK_TYPE,
      project: Optional[str] = None,
      location: Optional[str] = None,
      credentials: Optional[Credentials] = None,
      **kwargs):
    """Utilizes Vertex AI text embeddings for semantic search and RAG
        pipelines.
        
        Args:
            model_name: Name of the Vertex AI text embedding model
            title: Optional title for the text content
            task_type: Task type for embeddings (default: RETRIEVAL_DOCUMENT)
            project: GCP project ID
            location: GCP location
            credentials: Optional GCP credentials
            **kwargs: Additional arguments passed to EmbeddingsManager including
            ModelHandler inference_args.
        """
    if not vertexai:
      raise ImportError(
          "vertexai is required to use VertexAITextEmbeddings. "
          "Please install it with `pip install google-cloud-aiplatform`")

    super().__init__(type_adapter=create_rag_adapter(), **kwargs)
    self.model_name = model_name
    self.title = title
    self.task_type = task_type
    self.project = project
    self.location = location
    self.credentials = credentials

  def get_model_handler(self):
    """Returns model handler configured with RAG adapter."""
    return _VertexAITextEmbeddingHandler(
        model_name=self.model_name,
        title=self.title,
        task_type=self.task_type,
        project=self.project,
        location=self.location,
        credentials=self.credentials,
    )

  def get_ptransform_for_processing(
      self, **kwargs
  ) -> beam.PTransform[beam.PCollection[Chunk], beam.PCollection[Chunk]]:
    """Returns PTransform that uses the RAG adapter."""
    return RunInference(
        model_handler=_TextEmbeddingHandler(self),
        inference_args=self.inference_args).with_output_types(Chunk)
