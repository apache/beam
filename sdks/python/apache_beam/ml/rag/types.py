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
#

"""Core types for RAG pipelines.

This module contains the core dataclasses used throughout the RAG pipeline
implementation. The primary type is EmbeddableItem, which represents any
content that can be embedded and stored in a vector database.

Types:
  - Content: Container for embeddable content
  - Embedding: Vector embedding with optional metadata
  - EmbeddableItem: Universal container for embeddable content
  - Chunk: Alias for EmbeddableItem (backward compatibility)
"""

import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple


@dataclass
class Content:
  """Container for embeddable content.

  Args:
      text: Text content to be embedded.
  """
  text: Optional[str] = None


@dataclass
class Embedding:
  """Represents vector embeddings with optional metadata.

  Args:
      dense_embedding: Dense vector representation.
      sparse_embedding: Optional sparse vector representation for hybrid search.
      metadata: Optional metadata associated with this embedding.
  """
  dense_embedding: Optional[List[float]] = None
  sparse_embedding: Optional[Tuple[List[int], List[float]]] = None
  metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EmbeddableItem:
  """Universal container for embeddable content.

  Represents any content that can be embedded and stored in a vector database.
  Use factory methods for convenient construction, or construct directly with
  a Content object.

  Examples:
      Text (via factory):
          item = EmbeddableItem.from_text(
              "hello world", metadata={'src': 'doc'})

      Text (direct, equivalent to old Chunk usage):
          item = EmbeddableItem(content=Content(text="hello"), index=3)

  Args:
      content: The content to embed.
      id: Unique identifier.
      index: Position within source document (for chunking use cases).
      metadata: Additional metadata (e.g., document source, language).
      embedding: Embedding populated by the embedding step.
  """
  content: Content
  id: str = field(default_factory=lambda: str(uuid.uuid4()))
  index: int = 0
  metadata: Dict[str, Any] = field(default_factory=dict)
  embedding: Optional[Embedding] = None

  @classmethod
  def from_text(
      cls,
      text: str,
      *,
      id: Optional[str] = None,
      index: int = 0,
      metadata: Optional[Dict[str, Any]] = None,
  ) -> 'EmbeddableItem':
    """Create an EmbeddableItem with text content.

    Args:
        text: The text content to embed
        id: Unique identifier (auto-generated if not provided)
        index: Position within source document (for chunking)
        metadata: Additional metadata
    """
    return cls(
        content=Content(text=text),
        id=id or str(uuid.uuid4()),
        index=index,
        metadata=metadata or {},
    )

  @property
  def dense_embedding(self) -> Optional[List[float]]:
    return self.embedding.dense_embedding if self.embedding else None

  @property
  def sparse_embedding(self) -> Optional[Tuple[List[int], List[float]]]:
    return self.embedding.sparse_embedding if self.embedding else None


# Backward compatibility alias. Existing code using Chunk continues to work
# unchanged since Chunk IS EmbeddableItem.
Chunk = EmbeddableItem
