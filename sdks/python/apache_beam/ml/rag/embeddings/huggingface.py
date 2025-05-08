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

"""RAG-specific embedding implementations using HuggingFace models."""

from typing import Optional

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.rag.embeddings.base import create_rag_adapter
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler
from apache_beam.ml.transforms.embeddings.huggingface import _SentenceTransformerModelHandler

try:
  from sentence_transformers import SentenceTransformer
except ImportError:
  SentenceTransformer = None


class HuggingfaceTextEmbeddings(EmbeddingsManager):
  def __init__(
      self, model_name: str, *, max_seq_length: Optional[int] = None, **kwargs):
    """Utilizes huggingface SentenceTransformer embeddings for RAG pipeline.

        Args:
            model_name: Name of the sentence-transformers model to use
            max_seq_length: Maximum sequence length for the model
            **kwargs: Additional arguments passed to 
            :class:`~apache_beam.ml.transforms.base.EmbeddingsManager`
            constructor including ModelHandler arguments
        """
    if not SentenceTransformer:
      raise ImportError(
          "sentence-transformers is required to use "
          "HuggingfaceTextEmbeddings."
          "Please install it with using `pip install sentence-transformers`.")
    super().__init__(type_adapter=create_rag_adapter(), **kwargs)
    self.model_name = model_name
    self.max_seq_length = max_seq_length
    self.model_class = SentenceTransformer

  def get_model_handler(self):
    """Returns model handler configured with RAG adapter."""
    return _SentenceTransformerModelHandler(
        model_class=self.model_class,
        max_seq_length=self.max_seq_length,
        model_name=self.model_name,
        load_model_args=self.load_model_args,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model)

  def get_ptransform_for_processing(
      self, **kwargs
  ) -> beam.PTransform[beam.PCollection[Chunk], beam.PCollection[Chunk]]:
    """Returns PTransform that uses the RAG adapter."""
    return RunInference(
        model_handler=_TextEmbeddingHandler(self),
        inference_args=self.inference_args).with_output_types(Chunk)
