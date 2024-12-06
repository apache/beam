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

import unittest
from apache_beam.ml.rag.types import Chunk, Content, Embedding
from apache_beam.ml.rag.embeddings.base import (create_rag_adapter)


class RAGBaseEmbeddingsTest(unittest.TestCase):

  def setUp(self):
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
                "source": "test2.txt", "language": "en"
            })
    ]

  def test_adapter_input_conversion(self):
    """Test the RAG type adapter converts correctly."""
    adapter = create_rag_adapter()

    # Test input conversion
    texts = adapter.input_fn(self.test_chunks)
    self.assertEqual(texts, ["This is a test sentence.", "Another example."])

  def test_adapter_output_conversion(self):
    """Test the RAG type adapter converts correctly."""
    # Test output conversion
    mock_embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
    # Expected outputs
    expected = [
        Embedding(
            id="1",
            dense_embedding=[0.1, 0.2, 0.3],
            metadata={
                'source': 'test.txt', 'language': 'en'
            },
            content=Content(text='This is a test sentence.')),
        Embedding(
            id="2",
            dense_embedding=[0.4, 0.5, 0.6],
            metadata={
                'source': 'test2.txt', 'language': 'en'
            },
            content=Content(text='Another example.')),
    ]
    adapter = create_rag_adapter()

    embeddings = adapter.output_fn(self.test_chunks, mock_embeddings)
    self.assertListEqual(embeddings, expected)


if __name__ == '__main__':
  unittest.main()
