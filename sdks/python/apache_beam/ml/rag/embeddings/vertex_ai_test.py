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

"""Tests for apache_beam.ml.rag.embeddings.vertex_ai."""

import os
import shutil
import tempfile
import unittest

import apache_beam as beam
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  import vertexai  # pylint: disable=unused-import

  from apache_beam.ml.rag.embeddings.vertex_ai import VertexAIImageEmbeddings
  from apache_beam.ml.rag.embeddings.vertex_ai import VertexAITextEmbeddings
  from apache_beam.ml.rag.embeddings.vertex_ai import _create_image_adapter
  VERTEX_AI_AVAILABLE = True
except ImportError:
  VERTEX_AI_AVAILABLE = False


def chunk_approximately_equals(expected, actual):
  """Compare embeddings allowing for numerical differences."""
  if not isinstance(expected, Chunk) or not isinstance(actual, Chunk):
    return False

  return (
      expected.id == actual.id and expected.metadata == actual.metadata and
      expected.content == actual.content and
      len(expected.embedding.dense_embedding) == len(
          actual.embedding.dense_embedding) and
      all(isinstance(x, float) for x in actual.embedding.dense_embedding))


@unittest.skipIf(
    not VERTEX_AI_AVAILABLE, "Vertex AI dependencies not available")
class VertexAITextEmbeddingsTest(unittest.TestCase):
  def setUp(self):
    self.artifact_location = tempfile.mkdtemp(prefix='vertex_ai_')
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
    # gecko@002 produces 768-dimensional embeddings
    expected = [
        Chunk(
            id="1",
            embedding=Embedding(dense_embedding=[0.0] * 768),
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="This is a test sentence.")),
        Chunk(
            id="2",
            embedding=Embedding(dense_embedding=[0.0] * 768),
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="Another example."))
    ]

    embedder = VertexAITextEmbeddings(model_name="text-embedding-005")

    with TestPipeline() as p:
      embeddings = (
          p
          | beam.Create(self.test_chunks)
          | MLTransform(write_artifact_location=self.artifact_location).
          with_transform(embedder))

      assert_that(
          embeddings, equal_to(expected, equals_fn=chunk_approximately_equals))


@unittest.skipIf(
    not VERTEX_AI_AVAILABLE, "Vertex AI dependencies not available")
class VertexAIImageAdapterTest(unittest.TestCase):
  def test_image_adapter_missing_content(self):
    adapter = _create_image_adapter()
    item = EmbeddableItem(content=Content(), id="no_img")
    with self.assertRaisesRegex(ValueError, "Expected image content"):
      adapter.input_fn([item])

  def test_image_adapter_str_input(self):
    adapter = _create_image_adapter()
    item = EmbeddableItem.from_image('gs://bucket/img.jpg', id='img1')
    images = adapter.input_fn([item])
    self.assertEqual(len(images), 1)

  def test_image_adapter_bytes_input(self):
    adapter = _create_image_adapter()
    # Create a minimal valid PNG
    import struct
    import zlib
    png_sig = b'\x89PNG\r\n\x1a\n'
    ihdr_data = struct.pack('>IIBBBBB', 1, 1, 8, 2, 0, 0, 0)
    ihdr_crc = zlib.crc32(b'IHDR' + ihdr_data)
    ihdr = (
        struct.pack('>I', 13) + b'IHDR' + ihdr_data +
        struct.pack('>I', ihdr_crc))
    raw = zlib.compress(b'\x00\x00\x00\x00')
    idat_crc = zlib.crc32(b'IDAT' + raw)
    idat = (
        struct.pack('>I', len(raw)) + b'IDAT' + raw +
        struct.pack('>I', idat_crc))
    iend_crc = zlib.crc32(b'IEND')
    iend = (struct.pack('>I', 0) + b'IEND' + struct.pack('>I', iend_crc))
    png_bytes = png_sig + ihdr + idat + iend

    item = EmbeddableItem.from_image(png_bytes, id='img2')
    images = adapter.input_fn([item])
    self.assertEqual(len(images), 1)

  def test_image_adapter_output(self):
    adapter = _create_image_adapter()
    items = [
        EmbeddableItem.from_image('gs://bucket/img.jpg', id='img1'),
    ]
    mock_embeddings = [[0.1, 0.2, 0.3]]
    result = adapter.output_fn(items, mock_embeddings)
    self.assertEqual(len(result), 1)
    self.assertEqual(result[0].embedding.dense_embedding, [0.1, 0.2, 0.3])


@unittest.skipIf(
    not VERTEX_AI_AVAILABLE, "Vertex AI dependencies not available")
class VertexAIImageEmbeddingsTest(unittest.TestCase):
  def setUp(self):
    self.artifact_location = tempfile.mkdtemp(prefix='vertex_ai_img_')

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  @staticmethod
  def _create_png_bytes():
    """Create a minimal valid 1x1 PNG image."""
    import struct
    import zlib
    png_sig = b'\x89PNG\r\n\x1a\n'
    ihdr_data = struct.pack('>IIBBBBB', 1, 1, 8, 2, 0, 0, 0)
    ihdr_crc = zlib.crc32(b'IHDR' + ihdr_data)
    ihdr = (
        struct.pack('>I', 13) + b'IHDR' + ihdr_data +
        struct.pack('>I', ihdr_crc))
    raw = zlib.compress(b'\x00\x00\x00\x00')
    idat_crc = zlib.crc32(b'IDAT' + raw)
    idat = (
        struct.pack('>I', len(raw)) + b'IDAT' + raw +
        struct.pack('>I', idat_crc))
    iend_crc = zlib.crc32(b'IEND')
    iend = (struct.pack('>I', 0) + b'IEND' + struct.pack('>I', iend_crc))
    return png_sig + ihdr + idat + iend

  def test_image_embedding_pipeline(self):
    png_bytes = self._create_png_bytes()

    test_items = [
        EmbeddableItem.from_image(
            png_bytes, id="img1", metadata={"source": "test"}),
    ]

    # Default dimension for multimodal is 1408
    expected = [
        EmbeddableItem(
            id="img1",
            embedding=Embedding(dense_embedding=[0.0] * 1408),
            metadata={"source": "test"},
            content=Content(image=png_bytes)),
    ]

    embedder = VertexAIImageEmbeddings(model_name="multimodalembedding@001")
    with TestPipeline() as p:
      embeddings = (
          p
          | beam.Create(test_items)
          | MLTransform(write_artifact_location=(
              self.artifact_location)).with_transform(embedder))

      assert_that(
          embeddings, equal_to(expected, equals_fn=chunk_approximately_equals))

  def test_image_embedding_pipeline_from_path(self):
    png_bytes = self._create_png_bytes()
    img_path = os.path.join(self.artifact_location, 'test_img.png')
    with open(img_path, 'wb') as f:
      f.write(png_bytes)

    test_items = [
        EmbeddableItem.from_image(
            img_path, id="img1", metadata={"source": "test"}),
    ]

    expected = [
        EmbeddableItem(
            id="img1",
            embedding=Embedding(dense_embedding=[0.0] * 1408),
            metadata={"source": "test"},
            content=Content(image=img_path)),
    ]

    artifact_location = tempfile.mkdtemp(prefix='vertex_ai_img_path_')
    embedder = VertexAIImageEmbeddings(model_name="multimodalembedding@001")
    try:
      with TestPipeline() as p:
        embeddings = (
            p
            | beam.Create(test_items)
            | MLTransform(write_artifact_location=(
                artifact_location)).with_transform(embedder))

        assert_that(
            embeddings,
            equal_to(expected, equals_fn=chunk_approximately_equals))
    finally:
      shutil.rmtree(artifact_location)


if __name__ == '__main__':
  unittest.main()
