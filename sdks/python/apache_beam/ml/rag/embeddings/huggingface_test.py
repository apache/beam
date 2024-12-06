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

"""Tests for apache_beam.ml.rag.embeddings.huggingface."""

import tempfile

import unittest
import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.ml.transforms.base import MLTransform

try:
  from sentence_transformers import SentenceTransformer
  SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
  SENTENCE_TRANSFORMERS_AVAILABLE = False

from apache_beam.ml.rag.types import Chunk, Content, Embedding
from apache_beam.ml.rag.embeddings.huggingface import HuggingfaceTextEmbeddings


def embedding_approximately_equals(expected, actual):
  """Compare embeddings allowing for numerical differences."""
  if not isinstance(expected, Embedding) or not isinstance(actual, Embedding):
    return False

  return (
      expected.id == actual.id and expected.metadata == actual.metadata and
      len(expected.dense_embedding) == len(actual.dense_embedding) and
      all(isinstance(x, float) for x in actual.dense_embedding))


@pytest.mark.uses_transformers
@unittest.skipIf(
    not SENTENCE_TRANSFORMERS_AVAILABLE, "sentence-transformers not available")
class HuggingfaceTextEmbeddingsTest(unittest.TestCase):

  def setUp(self):
    self.artifact_location = tempfile.mkdtemp(prefix='sentence_transformers_')
    self.test_chunks = [
        Chunk(
            content=Content(text="This is a test sentence."),
            id="1",
            metadata={
                "source": "test.txt", "language": "en"
            }),
        Chunk(
            content=Content(text="Another example."),
            id="2",
            metadata={
                "source": "test.txt", "language": "en"
            })
    ]

  def test_embedding_pipeline(self):
    expected = [
        Embedding(
            id="1",
            dense_embedding=[0.0] * 384,  # Dimension placeholder
            sparse_embedding=None,
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="This is a test sentence.")),
        Embedding(
            id="2",
            dense_embedding=[0.0] * 384,
            sparse_embedding=None,
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="This is a test sentence."))
    ]
    """Test the complete embedding pipeline."""
    embedder = HuggingfaceTextEmbeddings(
        model_name="sentence-transformers/all-MiniLM-L6-v2")

    with TestPipeline() as p:
      embeddings = (
          p
          | beam.Create(self.test_chunks)
          | MLTransform(write_artifact_location=self.artifact_location).
          with_transform(embedder))

      assert_that(
          embeddings,
          equal_to(expected, equals_fn=embedding_approximately_equals))


if __name__ == '__main__':
  unittest.main()
