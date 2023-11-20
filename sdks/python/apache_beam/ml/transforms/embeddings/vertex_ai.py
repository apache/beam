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

from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

import apache_beam as beam
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler
import vertexai
from vertexai.language_models import TextEmbeddingInput
from vertexai.language_models import TextEmbeddingModel
from google.auth.credentials import Credentials

__all__ = ["VertexAITextEmbeddings"]

TASK_TYPE = "RETRIEVAL_DOCUMENT"
TASK_TYPE_INPUTS = [
    "RETRIEVAL_DOCUMENT",
    "RETRIEVAL_QUERY",
    "SEMANTIC_SIMILARITY",
    "CLASSIFICATION",
    "CLUSTERING"
]


def yield_elements(elements):
  for element in elements:
    yield element


class _YieldPTransform(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.ParDo(yield_elements)


class _VertexAITextEmbeddingHandler(ModelHandler):
  """
  Note: Intended for internal use and guarantees no backwards compatibility.
  """
  def __init__(
      self,
      model_name: str,
      title: Optional[str] = None,
      task_type: str = TASK_TYPE,
      project: Optional[str] = None,
      location: Optional[str] = None,
      credentials: Optional[Credentials] = None,
  ):
    vertexai.init(project=project, location=location, credentials=credentials)
    self.model_name = model_name
    if task_type not in TASK_TYPE_INPUTS:
      raise ValueError(
          f"task_type must be one of {TASK_TYPE_INPUTS}, got {task_type}")
    self.task_type = task_type
    self.title = title

  def run_inference(
      self,
      batch: List[str],
      model: Any,
      inference_args: Optional[Dict[str, Any]] = None,
  ) -> Iterable:
    embeddings = []
    batch_size = 5  # Vertex AI limits requests to 5 at a time.
    for i in range(0, len(batch), batch_size):
      text_batch = batch[i:i + batch_size]
      text_batch = [
          TextEmbeddingInput(
              text=text, title=self.title, task_type=self.task_type)
          for text in text_batch
      ]
      embeddings_batch = model.get_embeddings(text_batch)
      embeddings.extend([el.values for el in embeddings_batch])
    return embeddings

  def load_model(self):
    model = TextEmbeddingModel.from_pretrained(self.model_name)
    return model


class VertexAITextEmbeddings(EmbeddingsManager):
  def __init__(
      self,
      model_name: str,
      columns: List[str],
      title: Optional[str] = None,
      task_type: str = TASK_TYPE,
      project: Optional[str] = None,
      location: Optional[str] = None,
      credentials: Optional[Credentials] = None,
      **kwargs,
  ):
    """
    Embedding Config for Vertex AI Text Embedding models following
    https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings # pylint: disable=line-too-long

    Text Embeddings are generated for a batch of text using the Vertex AI SDK.
    Embeddings are returned in a list for each text in the batch. Look at
    https://cloud.google.com/vertex-ai/docs/generative-ai/learn/model-versioning#stable-versions-available.md # pylint: disable=line-too-long
    for more information on model versions and lifecycle.

    Args:
      model_name: The name of the Vertex AI Text Embedding model.
      columns: The columns containing the text to be embedded.
      task_type: The name of the downstream task the embeddings will be used for.
        Valid values:
        RETRIEVAL_QUERY
            Specifies the given text is a query in a search/retrieval setting.
        RETRIEVAL_DOCUMENT
            Specifies the given text is a document from the corpus being searched.
        SEMANTIC_SIMILARITY
            Specifies the given text will be used for STS.
        CLASSIFICATION
            Specifies that the given text will be classified.
        CLUSTERING
            Specifies that the embeddings will be used for clustering.
      title: Optional identifier of the text content.
      project: The default GCP project to make Vertex API calls.
      location: The default location to use when making API calls.
      credentials: The default custom
        credentials to use when making API calls. If not provided credentials
        will be ascertained from the environment.

    """
    self.model_name = model_name
    self.project = project
    self.location = location
    self.credentials = credentials
    self.title = title
    self.task_type = task_type
    super().__init__(columns=columns, **kwargs)

  def get_model_handler(self) -> ModelHandler:
    return _VertexAITextEmbeddingHandler(
        model_name=self.model_name,
        project=self.project,
        location=self.location,
        credentials=self.credentials,
        title=self.title,
        task_type=self.task_type,
    )

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    return (
        RunInference(model_handler=_TextEmbeddingHandler(self))
        | _YieldPTransform())
