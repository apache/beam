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
from typing import Sequence

import apache_beam as beam
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import TextEmbeddingHandler
import vertexai
from vertexai.language_models import TextEmbeddingInput
from vertexai.language_models import TextEmbeddingModel

# TODO: make these user configurable
TASK_TYPE = "RETRIEVAL_DOCUMENT"
TITLE = "Google"


def yield_elements(elements):
  for element in elements:
    yield element


# TODO: Can we VertexAIModelHandlerJson here?
class VertexAITextEmbeddingHandler(ModelHandler):
  """
  ModelHandler for Vertex AI Text Embedding models.
  https://cloud.google.com/vertex-ai/docs/generative-ai/model-reference/text-embeddings
  The model handler follows the above documentation for generating embeddings
  for a batch of text.
  """
  def __init__(
      self,
      model_name: str,
  ):
    self.model_name = model_name

  def run_inference(
      self,
      batch: Sequence,
      model: Any,
      inference_args: Optional[Dict[str, Any]] = None,
  ) -> Iterable:
    embeddings = []
    batch_size = 5
    for i in range(0, len(batch), batch_size):
      text_batch = batch[i:i + batch_size]
      text_batch = [
          TextEmbeddingInput(text=text, task_type=TASK_TYPE, title=TITLE)
          for text in text_batch
      ]
      embeddings_batch = model.get_embeddings(text_batch)
      print(embeddings_batch)
      embeddings.extend([el.values for el in embeddings_batch])
    return embeddings

  def load_model(self):
    model = TextEmbeddingModel.from_pretrained(self.model_name)
    return model


class VertexAIEmbeddings(EmbeddingsManager):
  def __init__(
      self,
      model_name: str,
      project: str,
      location: str,
      columns: List[str],
      batch_size: int = 5,
      **kwargs,
  ):
    self.model_name = model_name
    self.batch_size = batch_size
    super().__init__(columns=columns, **kwargs)
    vertexai.init(project=project, location=location)

  def get_model_handler(self) -> ModelHandler:
    return VertexAITextEmbeddingHandler(self.model_name)

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    return (
        RunInference(model_handler=TextEmbeddingHandler(embedding_config=self))
        | beam.ParDo(yield_elements))
