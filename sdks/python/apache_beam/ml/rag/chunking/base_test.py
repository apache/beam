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

"""Tests for apache_beam.ml.rag.chunking.base."""

import unittest
from typing import Any
from typing import Dict
from typing import Optional

import pytest

import apache_beam as beam
from apache_beam.ml.rag.chunking.base import ChunkIdFn
from apache_beam.ml.rag.chunking.base import ChunkingTransformProvider
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class WordSplitter(beam.DoFn):
  def process(self, element):
    words = element['text'].split()
    for i, word in enumerate(words):
      yield Chunk(
          content=Content(text=word),
          index=i,
          metadata={'source': element['source']})


class MockChunkingProvider(ChunkingTransformProvider):
  def __init__(self, chunk_id_fn: Optional[ChunkIdFn] = None):
    super().__init__(chunk_id_fn=chunk_id_fn)

  def get_splitter_transform(
      self
  ) -> beam.PTransform[beam.PCollection[Dict[str, Any]],
                       beam.PCollection[Chunk]]:
    return beam.ParDo(WordSplitter())


def chunk_equals(expected, actual):
  """Custom equality function for Chunk objects."""
  if not isinstance(expected, Chunk) or not isinstance(actual, Chunk):
    return False
  # Don't compare IDs since they're randomly generated
  return (
      expected.index == actual.index and expected.content == actual.content and
      expected.metadata == actual.metadata)


def id_equals(expected, actual):
  """Custom equality function for Chunk object id's."""
  if not isinstance(expected, Chunk) or not isinstance(actual, Chunk):
    return False
  return (expected.id == actual.id)


@pytest.mark.uses_transformers
class ChunkingTransformProviderTest(unittest.TestCase):
  def setUp(self):
    self.test_doc = {'text': 'hello world test', 'source': 'test.txt'}

  def test_chunking_transform(self):
    """Test the complete chunking transform."""
    provider = MockChunkingProvider()

    with TestPipeline() as p:
      chunks = (
          p
          | beam.Create([self.test_doc])
          | provider.get_ptransform_for_processing())

      expected = [
          Chunk(
              content=Content(text="hello"),
              index=0,
              metadata={'source': 'test.txt'}),
          Chunk(
              content=Content(text="world"),
              index=1,
              metadata={'source': 'test.txt'}),
          Chunk(
              content=Content(text="test"),
              index=2,
              metadata={'source': 'test.txt'})
      ]

      assert_that(chunks, equal_to(expected, equals_fn=chunk_equals))

  def test_custom_chunk_id_fn(self):
    """Test the a custom chink id function."""
    def source_index_id_fn(chunk: Chunk):
      return f"{chunk.metadata['source']}_{chunk.index}"

    provider = MockChunkingProvider(chunk_id_fn=source_index_id_fn)

    with TestPipeline() as p:
      chunks = (
          p
          | beam.Create([self.test_doc])
          | provider.get_ptransform_for_processing())

      expected = [
          Chunk(content=Content(text="hello"), id="test.txt_0"),
          Chunk(content=Content(text="world"), id="test.txt_1"),
          Chunk(content=Content(text="test"), id="test.txt_2")
      ]

      assert_that(chunks, equal_to(expected, equals_fn=id_equals))


if __name__ == '__main__':
  unittest.main()
