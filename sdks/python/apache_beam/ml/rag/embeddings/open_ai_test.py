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
import os
import shutil
import tempfile
import unittest

import apache_beam as beam
from apache_beam.ml.rag.embeddings.open_ai import OpenAITextEmbeddings
from apache_beam.ml.rag.test_utils import TestHelpers
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@unittest.skipIf(
    not os.environ.get('OPENAI_API_KEY'),
    'OPENAI_API_KEY environment variable is not set')
class OpenAITextEmbeddingsTest(unittest.TestCase):
  def setUp(self):
    self.artifact_location = tempfile.mkdtemp(prefix='openai_')
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

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_embedding_pipeline(self):
    expected = [
        Chunk(
            id="1",
            embedding=Embedding(dense_embedding=[0.0] * 1536),
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="This is a test sentence.")),
        Chunk(
            id="2",
            embedding=Embedding(dense_embedding=[0.0] * 1536),
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="Another example."))
    ]

    embedder = OpenAITextEmbeddings(
        model_name="text-embedding-3-small",
        dimensions=1536,
        api_key=os.environ.get("OPENAI_API_KEY"))

    with TestPipeline() as p:
      embeddings = (
          p
          | beam.Create(self.test_chunks)
          | MLTransform(write_artifact_location=self.artifact_location).
          with_transform(embedder))

      assert_that(
          embeddings,
          equal_to(expected, equals_fn=TestHelpers.chunk_approximately_equals))

  def test_embedding_pipeline_with_dimensions(self):
    expected = [
        Chunk(
            id="1",
            embedding=Embedding(dense_embedding=[0.0] * 512),
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="This is a test sentence.")),
        Chunk(
            id="2",
            embedding=Embedding(dense_embedding=[0.0] * 512),
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="Another example."))
    ]

    embedder = OpenAITextEmbeddings(
        model_name="text-embedding-3-small",
        dimensions=512,
        api_key=os.environ.get("OPENAI_API_KEY"))

    with TestPipeline() as p:
      embeddings = (
          p
          | beam.Create(self.test_chunks)
          | MLTransform(write_artifact_location=self.artifact_location).
          with_transform(embedder))

      assert_that(
          embeddings,
          equal_to(expected, equals_fn=TestHelpers.chunk_approximately_equals))


if __name__ == '__main__':
  unittest.main()
