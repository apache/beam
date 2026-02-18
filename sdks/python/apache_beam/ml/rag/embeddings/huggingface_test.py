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

import io
import os
import shutil
import tempfile
import unittest

import pytest

import apache_beam as beam
from apache_beam.ml.rag.embeddings.huggingface import HuggingfaceImageEmbeddings
from apache_beam.ml.rag.embeddings.huggingface import HuggingfaceTextEmbeddings
from apache_beam.ml.rag.embeddings.huggingface import _create_hf_image_adapter
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=unused-import
try:
  from sentence_transformers import SentenceTransformer
  SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
  SENTENCE_TRANSFORMERS_AVAILABLE = False
try:
  from PIL import Image
  PIL_AVAILABLE = True
except ImportError:
  PIL_AVAILABLE = False


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

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_embedding_pipeline(self):
    expected = [
        Chunk(
            id="1",
            embedding=Embedding(dense_embedding=[0.0] * 384),
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="This is a test sentence.")),
        Chunk(
            id="2",
            embedding=Embedding(dense_embedding=[0.0] * 384),
            metadata={
                "source": "test.txt", "language": "en"
            },
            content=Content(text="Another example."))
    ]
    embedder = HuggingfaceTextEmbeddings(
        model_name="sentence-transformers/all-MiniLM-L6-v2")

    with TestPipeline() as p:
      embeddings = (
          p
          | beam.Create(self.test_chunks)
          | MLTransform(write_artifact_location=self.artifact_location).
          with_transform(embedder))

      assert_that(
          embeddings, equal_to(expected, equals_fn=chunk_approximately_equals))


@pytest.mark.uses_transformers
@unittest.skipIf(
    not SENTENCE_TRANSFORMERS_AVAILABLE or not PIL_AVAILABLE,
    "sentence-transformers or PIL not available")
class HuggingfaceImageAdapterTest(unittest.TestCase):
  def test_image_adapter_missing_content(self):
    adapter = _create_hf_image_adapter()
    item = EmbeddableItem(content=Content(), id="no_img")
    with self.assertRaisesRegex(ValueError, "Expected image content"):
      adapter.input_fn([item])

  def test_image_adapter_bytes_input(self):
    adapter = _create_hf_image_adapter()
    png_bytes = _create_png_bytes()
    item = EmbeddableItem.from_image(png_bytes, id='img1')
    images = adapter.input_fn([item])
    self.assertEqual(len(images), 1)

  def test_image_adapter_path_input(self):
    adapter = _create_hf_image_adapter()
    png_bytes = _create_png_bytes()
    tmpdir = tempfile.mkdtemp()
    try:
      img_path = os.path.join(tmpdir, 'test.png')
      with open(img_path, 'wb') as f:
        f.write(png_bytes)
      item = EmbeddableItem.from_image(img_path, id='img2')
      images = adapter.input_fn([item])
      self.assertEqual(len(images), 1)
    finally:
      shutil.rmtree(tmpdir)

  def test_image_adapter_output(self):
    adapter = _create_hf_image_adapter()
    items = [EmbeddableItem.from_image(b'\x89PNG', id='img1')]
    mock_embeddings = [[0.1, 0.2, 0.3]]
    result = adapter.output_fn(items, mock_embeddings)
    self.assertEqual(len(result), 1)
    self.assertEqual(result[0].embedding.dense_embedding, [0.1, 0.2, 0.3])


@pytest.mark.uses_transformers
@unittest.skipIf(
    not SENTENCE_TRANSFORMERS_AVAILABLE or not PIL_AVAILABLE,
    "sentence-transformers or PIL not available")
class HuggingfaceImageEmbeddingsTest(unittest.TestCase):
  def setUp(self):
    self.artifact_location = tempfile.mkdtemp(prefix='hf_image_')

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_image_embedding_pipeline(self):
    png_bytes = _create_png_bytes()
    test_items = [
        EmbeddableItem.from_image(
            png_bytes, id="img1", metadata={"source": "test"}),
    ]

    # clip-ViT-B-32 produces 512-dimensional embeddings
    expected = [
        EmbeddableItem(
            id="img1",
            embedding=Embedding(dense_embedding=[0.0] * 512),
            metadata={"source": "test"},
            content=Content(image=png_bytes)),
    ]

    embedder = HuggingfaceImageEmbeddings(model_name="clip-ViT-B-32")

    with TestPipeline() as p:
      embeddings = (
          p
          | beam.Create(test_items)
          | MLTransform(write_artifact_location=(
              self.artifact_location)).with_transform(embedder))

      assert_that(
          embeddings, equal_to(expected, equals_fn=chunk_approximately_equals))

  def test_image_embedding_pipeline_from_path(self):
    png_bytes = _create_png_bytes()
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
            embedding=Embedding(dense_embedding=[0.0] * 512),
            metadata={"source": "test"},
            content=Content(image=img_path)),
    ]

    artifact_location = tempfile.mkdtemp(prefix='hf_image_path_')
    embedder = HuggingfaceImageEmbeddings(model_name="clip-ViT-B-32")
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


def _create_png_bytes():
  """Create a small valid RGB PNG image."""
  from PIL import Image
  img = Image.new('RGB', (10, 10), color=(128, 64, 32))
  buf = io.BytesIO()
  img.save(buf, format='PNG')
  return buf.getvalue()


if __name__ == '__main__':
  unittest.main()
