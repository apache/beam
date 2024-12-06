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

from apache_beam.ml.transforms.base import EmbeddingTypeAdapter
from apache_beam.ml.rag.types import Embedding


def create_rag_adapter() -> EmbeddingTypeAdapter:
  """Creates adapter for converting between Chunk and Embedding types.
    
    The adapter:
    - Extracts text from Chunk.content.text for embedding
    - Creates Embedding objects from model output
    - Preserves Chunk.id and metadata in Embedding
    - Sets sparse_embedding to None (dense embeddings only)
    
    Returns:
        EmbeddingTypeAdapter configured for RAG pipeline types
    """
  return EmbeddingTypeAdapter(
      input_fn=lambda chunks: [chunk.content.text for chunk in chunks],
      output_fn=lambda chunks,
      embeddings: [
          Embedding(
              id=chunk.id,
              dense_embedding=embeddings,
              sparse_embedding=None,
              metadata=chunk.metadata,
              content=chunk.content) for chunk,
          embeddings in zip(chunks, embeddings)
      ])
