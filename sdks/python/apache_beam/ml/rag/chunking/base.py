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

import abc
import functools
from collections.abc import Callable
from typing import Any
from typing import Dict
from typing import Optional

import apache_beam as beam
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.transforms.base import MLTransformProvider

ChunkIdFn = Callable[[Chunk], str]


def _assign_chunk_id(chunk_id_fn: ChunkIdFn, chunk: Chunk):
  chunk.id = chunk_id_fn(chunk)
  return chunk


class ChunkingTransformProvider(MLTransformProvider):
  def __init__(self, chunk_id_fn: Optional[ChunkIdFn] = None):
    """Base class for chunking transforms in RAG pipelines.
    
    ChunkingTransformProvider defines the interface for splitting documents
    into chunks for embedding and retrieval. Implementations should define how
    to split content while preserving metadata and managing chunk IDs.

    The transform flow:
    1. Takes input documents with content and metadata
    2. Splits content into chunks using implementation-specific logic
    3. Preserves document metadata in resulting chunks
    4. Optionally assigns unique IDs to chunks (configurable via chunk_id_fn).

    Example usage:
        ```python
        class MyChunker(ChunkingTransformProvider):
            def get_splitter_transform(self):
                return beam.ParDo(MySplitterDoFn())
                
        chunker = MyChunker(chunk_id_fn=my_id_function)
        
        with beam.Pipeline() as p:
            chunks = (
                p 
                | beam.Create([{'text': 'document...', 'source': 'doc.txt'}])
                | MLTransform(...).with_transform(chunker))
        ```

    Args:
        chunk_id_fn: Optional function to generate chunk IDs. If not provided,
          random UUIDs will be used. Function should take a Chunk and return
          str.
    """
    self.assign_chunk_id_fn = functools.partial(
        _assign_chunk_id, chunk_id_fn) if chunk_id_fn is not None else None

  @abc.abstractmethod
  def get_splitter_transform(
      self
  ) -> beam.PTransform[beam.PCollection[Dict[str, Any]],
                       beam.PCollection[Chunk]]:
    """Creates transforms that emits splits for given content."""
    raise NotImplementedError(
        "Subclasses must implement get_splitter_transform")

  def get_ptransform_for_processing(
      self, **kwargs
  ) -> beam.PTransform[beam.PCollection[Dict[str, Any]],
                       beam.PCollection[Chunk]]:
    """Creates transform for processing documents into chunks."""
    ptransform = (
        "Split document" >>
        self.get_splitter_transform().with_output_types(Chunk))
    if self.assign_chunk_id_fn:
      ptransform = (
          ptransform | "Assign chunk id" >> beam.Map(
              self.assign_chunk_id_fn).with_output_types(Chunk))
    return ptransform
