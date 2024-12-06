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
implementation, including Chunk and Embedding types that define the data
contracts between different stages of the pipeline.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

@dataclass
class Content:
    """Container for embeddable content.
    """
    text: Optional[str] = None
    image_data: Optional[bytes] = None

@dataclass
class Chunk:
    """Represents a chunk of text with metadata.
    
    Attributes:
        text: The actual content of the chunk
        id: Unique identifier for the chunk
        index: Index of this chunk within the original document
        metadata: Additional metadata about the chunk (e.g., document source)
    """
    content: Content
    id: Optional[str] = None
    index: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class Embedding:
    """Represents vector embeddings with associated metadata.
    
    Attributes:
        id: Unique identifier for the embedding
        dense_embedding: Dense vector representation
        sparse_embedding: Optional sparse vector representation for hybrid
          search
        metadata: Additional metadata about the embedding
    """
    id: str
    dense_embedding: Optional[List[float]] = None
    # For hybrid search
    sparse_embedding: Optional[Tuple[List[int], List[float]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    content: Optional[Content] = None