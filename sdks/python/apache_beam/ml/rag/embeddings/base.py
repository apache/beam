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

"""RAG-specific embedding adapters.

This module provides adapters for extracting content from
EmbeddableItem instances and mapping embeddings back. Adapters
are used by EmbeddingsManager to support various input types.
"""

from collections.abc import Sequence
from typing import List

from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.ml.transforms.base import EmbeddingTypeAdapter


def create_text_adapter(
) -> EmbeddingTypeAdapter[EmbeddableItem, EmbeddableItem]:
  """Creates adapter for text content embedding.

  Works with any EmbeddableItem that has text content
  (content.text). Extracts text for embedding and maps
  results back as Embedding objects.

  Returns:
      EmbeddingTypeAdapter configured for text embedding
  """
  return EmbeddingTypeAdapter(
      input_fn=_extract_text, output_fn=_add_embedding_fn)


# Backward compatibility alias.
create_rag_adapter = create_text_adapter


def _extract_text(items: Sequence[EmbeddableItem]) -> List[str]:
  """Extract text from items for embedding."""
  texts = []
  for item in items:
    if not item.content.text:
      raise ValueError(
          f"Expected text content in {type(item).__name__} {item.id}, "
          "got None")
    texts.append(item.content.text)
  return texts


def _add_embedding_fn(
    items: Sequence[EmbeddableItem],
    embeddings: Sequence[List[float]]) -> List[EmbeddableItem]:
  """Create Embeddings from items and embedding vectors."""
  for item, embedding in zip(items, embeddings):
    item.embedding = Embedding(dense_embedding=embedding)
  return list(items)
