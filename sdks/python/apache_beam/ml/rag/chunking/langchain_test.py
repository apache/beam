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

"""Tests for apache_beam.ml.rag.chunking.langchain."""

import functools
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_not_empty

try:
  from apache_beam.ml.rag.chunking.langchain import LangChainChunker

  from langchain.text_splitter import (
      CharacterTextSplitter, RecursiveCharacterTextSplitter)
  LANGCHAIN_AVAILABLE = True
except ImportError:
  LANGCHAIN_AVAILABLE = False

# Import optional dependencies
try:
  from transformers import AutoTokenizer
  TRANSFORMERS_AVAILABLE = True
except ImportError:
  TRANSFORMERS_AVAILABLE = False


def assert_true(elements, assert_fn, error_message_fn):
  if not assert_fn(elements):
    raise BeamAssertException(error_message_fn(elements))
  return True


@unittest.skipIf(not LANGCHAIN_AVAILABLE, 'langchain is not installed.')
class LangChainChunkingTest(unittest.TestCase):
  def setUp(self):
    self.simple_text = {
        'content': 'This is a simple test document. It has multiple sentences. '
        'We will use it to test basic splitting.',
        'source': 'simple.txt',
        'language': 'en'
    }

    self.complex_text = {
        'content': (
            'The patient arrived at 2 p.m. yesterday. '
            'Initial assessment was completed. '
            'Lab results showed normal ranges. '
            'Follow-up scheduled for next week.'),
        'source': 'medical.txt',
        'language': 'en'
    }

  def test_no_metadata_fields(self):
    """Test chunking with no metadata fields specified."""
    splitter = CharacterTextSplitter(chunk_size=100, chunk_overlap=20)
    provider = LangChainChunker(
        document_field='content', metadata_fields=[], text_splitter=splitter)

    with TestPipeline() as p:
      chunks = (
          p
          | beam.Create([self.simple_text])
          | provider.get_ptransform_for_processing())
      chunks_count = chunks | beam.combiners.Count.Globally()

      assert_that(chunks_count, is_not_empty(), 'Has chunks')

      assert_that(
          chunks,
          functools.partial(
              assert_true,
              assert_fn=lambda x: (all(c.metadata == {} for c in x)),
              error_message_fn=lambda x: f"Expected empty metadata, actual {x}")
      )

  def test_multiple_metadata_fields(self):
    """Test chunking with multiple metadata fields."""
    splitter = CharacterTextSplitter(chunk_size=100, chunk_overlap=20)
    provider = LangChainChunker(
        document_field='content',
        metadata_fields=['source', 'language'],
        text_splitter=splitter)
    expected_metadata = {'source': 'simple.txt', 'language': 'en'}

    with TestPipeline() as p:
      chunks = (
          p
          | beam.Create([self.simple_text])
          | provider.get_ptransform_for_processing())
      chunks_count = chunks | beam.combiners.Count.Globally()

      assert_that(chunks_count, is_not_empty(), 'Has chunks')
      assert_that(
          chunks,
          functools.partial(
              assert_true,
              assert_fn=lambda x: all(
                  c.metadata == expected_metadata for c in x),
              error_message_fn=lambda x:
              f"Expected metadata {expected_metadata}, actual {x}"))

  def test_recursive_splitter_no_overlap(self):
    """Test RecursiveCharacterTextSplitter with no overlap."""
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=30, chunk_overlap=0, separators=[".", " "])
    provider = LangChainChunker(
        document_field='content',
        metadata_fields=['source'],
        text_splitter=splitter)

    with TestPipeline() as p:
      chunks = (
          p
          | beam.Create([self.simple_text])
          | provider.get_ptransform_for_processing())
      chunks_count = chunks | beam.combiners.Count.Globally()

      assert_that(chunks_count, is_not_empty(), 'Has chunks')
      assert_that(
          chunks,
          functools.partial(
              assert_true,
              assert_fn=lambda x: all(len(c.content.text) <= 30 for c in x),
              error_message_fn=lambda x: f"Expected len(chunk) <= 30, \
                 actual {[len(c.content.text) for c in x]}"))

  @unittest.skipIf(not TRANSFORMERS_AVAILABLE, "transformers not available")
  def test_huggingface_tokenizer_splitter(self):
    """Test text splitter created from HuggingFace tokenizer."""
    tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
    splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
        tokenizer,
        chunk_size=10,  # tokens
        chunk_overlap=2  # tokens
    )

    provider = LangChainChunker(
        document_field='content',
        metadata_fields=['source'],
        text_splitter=splitter)

    with TestPipeline() as p:
      chunks = (
          p
          | beam.Create([self.simple_text])
          | provider.get_ptransform_for_processing())

      def check_token_lengths(chunks):
        for chunk in chunks:
          # Verify each chunk's token length is within limits
          num_tokens = len(tokenizer.tokenize(chunk.content.text))
          if not num_tokens <= 10:
            raise BeamAssertException(
                f"Chunk has {num_tokens} tokens, expected <= 10")
        return True

      chunks_count = chunks | beam.combiners.Count.Globally()

      assert_that(chunks_count, is_not_empty(), 'Has chunks')
      assert_that(chunks, check_token_lengths)

  def test_invalid_document_field(self):
    """Test that using an invalid document field raises KeyError."""
    splitter = CharacterTextSplitter(chunk_size=100, chunk_overlap=20)
    provider = LangChainChunker(
        document_field='nonexistent',
        metadata_fields={},
        text_splitter=splitter)

    with self.assertRaisesRegex(Exception, "nonexistent"):
      with TestPipeline() as p:
        _ = (
            p
            | beam.Create([self.simple_text])
            | provider.get_ptransform_for_processing())

  def test_empty_document_field(self):
    """Test that using an invalid document field raises KeyError."""
    splitter = CharacterTextSplitter(chunk_size=100, chunk_overlap=20)

    with self.assertRaises(ValueError):
      _ = LangChainChunker(
          document_field='', metadata_fields={}, text_splitter=splitter)

  def test_invalid_text_splitter(self):
    """Test that using an invalid document field raises KeyError."""

    with self.assertRaises(TypeError):
      _ = LangChainChunker(
          document_field='nonexistent', text_splitter="Not a text splitter!")

  def test_empty_text(self):
    """Test that empty text produces no chunks."""
    empty_doc = {'content': '', 'source': 'empty.txt'}

    splitter = CharacterTextSplitter(chunk_size=100, chunk_overlap=20)
    provider = LangChainChunker(
        document_field='content',
        metadata_fields=['source'],
        text_splitter=splitter)

    with TestPipeline() as p:
      chunks = (
          p
          | beam.Create([empty_doc])
          | provider.get_ptransform_for_processing())

      assert_that(chunks, equal_to([]))


if __name__ == '__main__':
  unittest.main()
