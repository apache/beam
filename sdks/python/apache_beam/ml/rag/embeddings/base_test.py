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

from apache_beam.ml.rag.embeddings.base import create_text_adapter
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding


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
    adapter = create_text_adapter()

    # Test input conversion
    texts = adapter.input_fn(self.test_chunks)
    self.assertEqual(texts, ["This is a test sentence.", "Another example."])

  def test_adapter_input_conversion_missing_text_content(self):
    """Test the RAG type adapter converts correctly."""
    adapter = create_text_adapter()

    # Test input conversion
    with self.assertRaisesRegex(ValueError, "Expected text content"):
      adapter.input_fn([
          Chunk(
              content=Content(),
              id="1",
              metadata={
                  "source": "test.txt", "language": "en"
              })
      ])

  def test_adapter_output_conversion(self):
    """Test the RAG type adapter converts correctly."""
    # Test output conversion
    mock_embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
    # Expected outputs
    expected = [
        Chunk(
            id="1",
            embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
            metadata={
                'source': 'test.txt', 'language': 'en'
            },
            content=Content(text='This is a test sentence.')),
        Chunk(
            id="2",
            embedding=Embedding(dense_embedding=[0.4, 0.5, 0.6]),
            metadata={
                'source': 'test2.txt', 'language': 'en'
            },
            content=Content(text='Another example.')),
    ]
    adapter = create_text_adapter()

    embeddings = adapter.output_fn(self.test_chunks, mock_embeddings)
    self.assertListEqual(embeddings, expected)


class ImageEmbeddableItemTest(unittest.TestCase):
  def test_from_image_str(self):
    item = EmbeddableItem.from_image('gs://bucket/img.jpg', id='img1')
    self.assertEqual(item.content.image, 'gs://bucket/img.jpg')
    self.assertIsNone(item.content.text)
    self.assertEqual(item.id, 'img1')

  def test_from_image_bytes(self):
    data = b'\x89PNG\r\n'
    item = EmbeddableItem.from_image(data, id='img2')
    self.assertEqual(item.content.image, data)
    self.assertIsNone(item.content.text)

  def test_from_image_with_metadata(self):
    item = EmbeddableItem.from_image(
        'path/to/img.jpg', id='img3', metadata={'source': 'camera'})
    self.assertEqual(item.metadata, {'source': 'camera'})
    self.assertEqual(item.content.image, 'path/to/img.jpg')


class ContentStringTest(unittest.TestCase):
  def test_text_content(self):
    item = EmbeddableItem(content=Content(text="hello"), id="1")
    self.assertEqual(item.content_string, "hello")

  def test_image_uri_content(self):
    item = EmbeddableItem.from_image('gs://bucket/img.jpg', id='img1')
    self.assertEqual(item.content_string, 'gs://bucket/img.jpg')

  def test_image_bytes_raises(self):
    item = EmbeddableItem.from_image(b'\x89PNG\r\n', id='img2')
    with self.assertRaisesRegex(ValueError,
                                "EmbeddableItem does not contain.*"):
      item.content_string


if __name__ == '__main__':
  unittest.main()
