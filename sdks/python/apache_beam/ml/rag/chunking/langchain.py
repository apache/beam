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

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import apache_beam as beam
from apache_beam.ml.rag.chunking.base import ChunkIdFn
from apache_beam.ml.rag.chunking.base import ChunkingTransformProvider
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content

try:
  from langchain.text_splitter import TextSplitter
except ImportError:
  TextSplitter = None


class LangChainChunker(ChunkingTransformProvider):
  def __init__(
      self,
      text_splitter: TextSplitter,
      document_field: str,
      metadata_fields: List[str],
      chunk_id_fn: Optional[ChunkIdFn] = None):
    """A ChunkingTransformProvider that uses LangChain text splitters.
  
    This provider integrates LangChain's text splitting capabilities into
    Beam's MLTransform framework. It supports various text splitting strategies
    through LangChain's TextSplitter interface, including recursive character
    splitting and other methods. 

    The provider:
    - Takes documents with text content and metadata
    - Splits text using configured LangChain splitter
    - Preserves document metadata in resulting chunks
    - Assigns unique IDs to chunks (configurable via chunk_id_fn)

    Example usage:
      ```python
      from langchain.text_splitter import RecursiveCharacterTextSplitter
      
      splitter = RecursiveCharacterTextSplitter(
          chunk_size=100,
          chunk_overlap=20
      )
      
      chunker = LangChainChunker(text_splitter=splitter)
      
      with beam.Pipeline() as p:
        chunks = (
            p 
            | beam.Create([{'text': 'long document...', 'source': 'doc.txt'}])
            | MLTransform(...).with_transform(chunker))
      ```

    Args:
      text_splitter: A LangChain TextSplitter instance that defines how
        documents are split into chunks.
      metadata_fields: List of field names to copy from input documents to
        chunk metadata. These fields will be preserved in each chunk created
        from the document.
      chunk_id_fn: Optional function that take a Chunk and return str to
        generate chunk IDs. If not provided, random UUIDs will be used.
    """
    if not TextSplitter:
      raise ImportError(
          "langchain is required to use LangChainChunker"
          "Please install it with using `pip install langchain`.")
    if not isinstance(text_splitter, TextSplitter):
      raise TypeError("text_splitter must be a LangChain TextSplitter")
    if not document_field:
      raise ValueError("document_field cannot be empty")
    super().__init__(chunk_id_fn)
    self.text_splitter = text_splitter
    self.document_field = document_field
    self.metadata_fields = metadata_fields

  def get_splitter_transform(
      self
  ) -> beam.PTransform[beam.PCollection[Dict[str, Any]],
                       beam.PCollection[Chunk]]:
    return "Langchain text split" >> beam.ParDo(
        _LangChainTextSplitter(
            text_splitter=self.text_splitter,
            document_field=self.document_field,
            metadata_fields=self.metadata_fields))


class _LangChainTextSplitter(beam.DoFn):
  def __init__(
      self,
      text_splitter: TextSplitter,
      document_field: str,
      metadata_fields: List[str]):
    self.text_splitter = text_splitter
    self.document_field = document_field
    self.metadata_fields = metadata_fields

  def process(self, element):
    text_chunks = self.text_splitter.split_text(element[self.document_field])
    metadata = {field: element[field] for field in self.metadata_fields}
    for i, text_chunk in enumerate(text_chunks):
      yield Chunk(content=Content(text=text_chunk), index=i, metadata=metadata)
