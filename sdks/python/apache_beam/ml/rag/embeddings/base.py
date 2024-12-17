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

from collections.abc import Sequence
from typing import List

from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Embedding
from apache_beam.ml.transforms.base import EmbeddingTypeAdapter


def create_rag_adapter() -> EmbeddingTypeAdapter[Chunk, Chunk]:
  """Creates adapter for converting between Chunk and Embedding types.
    
    The adapter:
    - Extracts text from Chunk.content.text for embedding
    - Creates Embedding objects from model output
    - Sets Embedding in Chunk.embedding
    
    Returns:
        EmbeddingTypeAdapter configured for RAG pipeline types
    """
  return EmbeddingTypeAdapter(
      input_fn=_extract_chunk_text, output_fn=_add_embedding_fn)


def _extract_chunk_text(chunks: Sequence[Chunk]) -> List[str]:
  """Extract text from chunks for embedding."""
  chunk_texts = []
  for chunk in chunks:
    if not chunk.content.text:
      raise ValueError("Expected chunk text content.")
    chunk_texts.append(chunk.content.text)
  return chunk_texts


def _add_embedding_fn(
    chunks: Sequence[Chunk], embeddings: Sequence[List[float]]) -> List[Chunk]:
  """Create Embeddings from chunks and embedding vectors."""
  for chunk, embedding in zip(chunks, embeddings):
    chunk.embedding = Embedding(dense_embedding=embedding)
  return list(chunks)
