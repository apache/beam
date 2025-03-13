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

from __future__ import absolute_import

import apache_beam as beam
from langchain.text_splitter import CharacterTextSplitter, RecursiveCharacterTextSplitter
from langchain_text_splitters import SentenceTransformersTokenTextSplitter

from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from enum import Enum


__all__ = [
    'ChunksGeneration',
    'ChunkingStrategy'
]

class ChunkingStrategy(Enum):
    SPLIT_BY_CHARACTER = 0
    RECURSIVE_SPLIT_BY_CHARACTER = 1
    SPLIT_BY_TOKENS = 2


class ChunksGeneration(PTransform):
    """ChunkingStrategy is a ``PTransform`` that takes a ``PCollection`` of
    key, value tuple or 2-element array and generates different chunks for documents.
    """

    def __init__(
            self,
            chunk_size: int,
            chunk_overlap: int,
            chunking_strategy: ChunkingStrategy
    ):
        """

        Args:
        chunk_size : Chunk size is the maximum number of characters that a chunk can contain
        chunk_overlap : the number of characters that should overlap between two adjacent chunks
        chunking_strategy : Defines the way to split text

        Returns:
        :class:`~apache_beam.transforms.ptransform.PTransform`

        """

        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.chunking_strategy = chunking_strategy

    def expand(self, pcoll):
        return pcoll \
               | "Generate text chunks" >> beam.ParDo(_GenerateChunksFn(self.chunk_size,
                                                                        self.chunk_overlap,
                                                                        self.chunking_strategy))


class _GenerateChunksFn(DoFn):
    """Abstract class that takes in ptransform
    and generate chunks.
    """

    def __init__(
            self,
            chunk_size: int,
            chunk_overlap: int,
            chunking_strategy: ChunkingStrategy
    ):

        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.chunking_strategy = chunking_strategy

    def process(self, element, *args, **kwargs):

        # For recursive split by character
        if self.chunking_strategy == ChunkingStrategy.RECURSIVE_SPLIT_BY_CHARACTER:
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=self.chunk_size,
                chunk_overlap=self.chunk_overlap,
                length_function=len,
                is_separator_regex=False,
            )

        # For  split by character
        elif self.chunking_strategy == ChunkingStrategy.SPLIT_BY_CHARACTER:
            text_splitter = CharacterTextSplitter(
                chunk_size=self.chunk_size,
                chunk_overlap=self.chunk_overlap,
                length_function=len,
                is_separator_regex=False,
            )

        # For split by tokens
        elif self.chunking_strategy == ChunkingStrategy.SPLIT_BY_TOKENS:
            text_splitter = SentenceTransformersTokenTextSplitter(
                chunk_overlap=self.chunk_overlap,
                model_name='all-MiniLM-L6-v2'
            )

        else:
            raise ValueError(f"Invalid chunking strategy: {self.chunking_strategy}")

        texts = text_splitter.split_text(element['text'])[:]

        element_copy = element.copy()
        del element_copy['text']
        for i, section in enumerate(texts):
            element_copy['text'] = section
            element_copy['section_id'] = i + 1
            yield element_copy


