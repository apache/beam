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

"""RAG-specific embedding implementations using HuggingFace models."""

import io
from collections.abc import Sequence
from typing import Optional

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.rag.embeddings.base import _add_embedding_fn
from apache_beam.ml.rag.embeddings.base import create_text_adapter
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import EmbeddingTypeAdapter
from apache_beam.ml.transforms.base import _ImageEmbeddingHandler
from apache_beam.ml.transforms.base import _TextEmbeddingHandler
from apache_beam.ml.transforms.embeddings.huggingface import _SentenceTransformerModelHandler

try:
  from sentence_transformers import SentenceTransformer
except ImportError:
  SentenceTransformer = None

try:
  from PIL import Image as PILImage
except ImportError:
  PILImage = None


class HuggingfaceTextEmbeddings(EmbeddingsManager):
  def __init__(
      self, model_name: str, *, max_seq_length: Optional[int] = None, **kwargs):
    """HuggingFace text embeddings for RAG pipelines.

    Args:
        model_name: Name of the sentence-transformers model to use.
        max_seq_length: Maximum sequence length for the model.
        **kwargs: Additional arguments passed to
            :class:`~apache_beam.ml.transforms.base.EmbeddingsManager`,
            including:

            - ``load_model_args``: dict passed to
              ``SentenceTransformer()`` constructor
              (e.g. ``device``, ``cache_folder``).
            - ``min_batch_size`` / ``max_batch_size``:
              Control batching for inference.
            - ``large_model``: If True, share the model
              across processes to reduce memory usage.
            - ``inference_args``: dict passed to
              ``model.encode()``
              (e.g. ``normalize_embeddings``).
    """
    if not SentenceTransformer:
      raise ImportError(
          "sentence-transformers is required to use "
          "HuggingfaceTextEmbeddings."
          "Please install it with using `pip install sentence-transformers`.")
    super().__init__(type_adapter=create_text_adapter(), **kwargs)
    self.model_name = model_name
    self.max_seq_length = max_seq_length
    self.model_class = SentenceTransformer

  def get_model_handler(self):
    """Returns model handler configured with RAG adapter."""
    return _SentenceTransformerModelHandler(
        model_class=self.model_class,
        max_seq_length=self.max_seq_length,
        model_name=self.model_name,
        load_model_args=self.load_model_args,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model)

  def get_ptransform_for_processing(
      self, **kwargs
  ) -> beam.PTransform[beam.PCollection[EmbeddableItem],
                       beam.PCollection[EmbeddableItem]]:
    """Returns PTransform that uses the RAG adapter."""
    return RunInference(
        model_handler=_TextEmbeddingHandler(self),
        inference_args=self.inference_args).with_output_types(EmbeddableItem)


def _extract_images(items: Sequence[EmbeddableItem]) -> list:
  """Extract images from items and convert to PIL.Image objects."""
  images = []
  for item in items:
    if not item.content.image:
      raise ValueError(
          "Expected image content in "
          f"{type(item).__name__} {item.id}, "
          "got None")
    img_data = item.content.image
    if isinstance(img_data, bytes):
      img = PILImage.open(io.BytesIO(img_data))
    else:
      img = PILImage.open(img_data)
    images.append(img.convert('RGB'))
  return images


def _create_hf_image_adapter(
) -> EmbeddingTypeAdapter[EmbeddableItem, EmbeddableItem]:
  """Creates adapter for HuggingFace image embedding.

  Extracts content.image from EmbeddableItems and converts
  to PIL.Image objects. Supports both raw bytes and file paths.

  Returns:
      EmbeddingTypeAdapter for HuggingFace image embedding.
  """
  return EmbeddingTypeAdapter(
      input_fn=_extract_images, output_fn=_add_embedding_fn)


class HuggingfaceImageEmbeddings(EmbeddingsManager):
  def __init__(
      self, model_name: str, *, max_seq_length: Optional[int] = None, **kwargs):
    """HuggingFace image embeddings for RAG pipelines.

    Generates embeddings for images using sentence-transformers
    models that support image input (e.g. clip-ViT-B-32).

    Args:
        model_name: Name of the sentence-transformers model.
            Must be an image-text model. See
            https://www.sbert.net/docs/sentence_transformer/pretrained_models.html#image-text-models
        max_seq_length: Maximum sequence length for the model
            if applicable.
        **kwargs: Additional arguments passed to
            :class:`~apache_beam.ml.transforms.base.EmbeddingsManager`,
            including:

            - ``load_model_args``: dict passed to
              ``SentenceTransformer()`` constructor
              (e.g. ``device``, ``cache_folder``,
              ``trust_remote_code``).
            - ``min_batch_size`` / ``max_batch_size``:
              Control batching for inference.
            - ``large_model``: If True, share the model
              across processes to reduce memory usage.
            - ``inference_args``: dict passed to
              ``model.encode()``
              (e.g. ``normalize_embeddings``).
    """
    if not SentenceTransformer:
      raise ImportError(
          "sentence-transformers is required to use "
          "HuggingfaceImageEmbeddings. "
          "Please install it with `pip install sentence-transformers`.")
    if not PILImage:
      raise ImportError(
          "Pillow is required to use HuggingfaceImageEmbeddings. "
          "Please install it with `pip install pillow`.")
    super().__init__(type_adapter=_create_hf_image_adapter(), **kwargs)
    self.model_name = model_name
    self.max_seq_length = max_seq_length
    self.model_class = SentenceTransformer

  def get_model_handler(self):
    """Returns model handler configured with RAG adapter."""
    return _SentenceTransformerModelHandler(
        model_class=self.model_class,
        max_seq_length=self.max_seq_length,
        model_name=self.model_name,
        load_model_args=self.load_model_args,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model)

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    """Returns PTransform for image embedding."""
    return RunInference(
        model_handler=_ImageEmbeddingHandler(self),
        inference_args=self.inference_args).with_output_types(EmbeddableItem)
