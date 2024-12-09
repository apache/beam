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

import apache_beam as beam
from langchain.text_splitter import TextSplitter
from apache_beam.ml.rag.chunking.base import ChunkingTransformProvider, ChunkIdFn
from apache_beam.ml.rag.types import Chunk, Content
from typing import List, Optional


class LangChainChunkingProvider(ChunkingTransformProvider):
  def __init__(
      self,
      text_splitter: TextSplitter,
      document_field: str,
      metadata_fields: List[str],
      chunk_id_fn: Optional[ChunkIdFn] = None):
    if not isinstance(text_splitter, TextSplitter):
      raise TypeError("text_splitter must be a LangChain TextSplitter")
    if not document_field:
      raise ValueError("document_field cannot be empty")
    super().__init__(chunk_id_fn)
    self.text_splitter = text_splitter
    self.document_field = document_field
    self.metadata_fields = metadata_fields

  def get_text_splitter_transform(self) -> beam.DoFn:
    return "Langchain text split" >> beam.ParDo(
        LangChainTextSplitter(
            text_splitter=self.text_splitter,
            document_field=self.document_field,
            metadata_fields=self.metadata_fields))


class LangChainTextSplitter(beam.DoFn):
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
