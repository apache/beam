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

"""
`sentence-transformers` package should be installed to use this module.

This module provides a config for generating text embeddings using
sentence-transformers. User can define an embedding config using this module and
use it with MLTransform to embed text data.

The ML models hosted on HuggingFace Hub -
https://huggingface.co/models?library=sentence-transformers can be used
 with this module.

Usage:
```
embedding_config = SentenceTransformerEmbeddings(
            model_uri=model_name,
            columns=['text'])
with beam.Pipeline(options=options) as p:
  data = (
    p
    | "CreateData" >> beam.Create([{"text": "This is a test"}])
  )
  (
    data
    | "MLTransform" >> MLTransform(write_artifact_location=artifact_location).
    with_transform(embedding_config)
  )
```
"""

__all__ = ["SentenceTransformerEmbeddings"]

from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler

from sentence_transformers import SentenceTransformer


def inference_fn(batch, model, inference_args, *args, **kwargs):
  return model.encode(batch, **inference_args)


class _YieldPTransform(beam.PTransform):
  def expand(self, pcoll):
    def yield_elements(elements):
      for element in elements:
        yield element

    return pcoll | beam.ParDo(yield_elements)


# TODO: Use HuggingFaceModelHandlerTensor once the import issue is fixed.
# Right now, the hugging face model handler import torch and tensorflow
# at the same time, which adds too much weigth to the container unnecessarily.
class _SentenceTransformerModelHandler(ModelHandler):
  """
  Note: Intended for internal use and guarantees no backwards compatibility.
  """
  def __init__(
      self,
      model_name: str,
      model_class: Callable,
      inference_fn: Callable = inference_fn,
      load_model_args: Optional[dict] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_seq_length: Optional[int] = None,
      large_model: Optional[bool] = None,
      **kwargs):
    self._max_seq_length = max_seq_length
    self._model_uri = model_name
    self._model_class = model_class
    self._load_model_args = load_model_args
    self._inference_fn = inference_fn
    self._min_batch_size = min_batch_size
    self._max_batch_size = max_batch_size
    self._large_model = large_model
    self._kwargs = kwargs

  def run_inference(
      self,
      batch: List[str],
      model: SentenceTransformer,
      inference_args: Optional[Dict[str, Any]] = None,
  ):
    inference_args = inference_args or {}
    return self._inference_fn(batch, model, inference_args, **self._kwargs)

  def load_model(self):
    model = self._model_class(self._model_uri)
    if self._max_seq_length:
      model.max_seq_length = self._max_seq_length
    return model

  def share_model_across_processes(self) -> bool:
    return self._large_model

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    batch_sizes = {}
    if self._min_batch_size:
      batch_sizes["min_batch_size"] = self._min_batch_size
    if self._max_batch_size:
      batch_sizes["max_batch_size"] = self._max_batch_size
    return batch_sizes


class SentenceTransformerEmbeddings(EmbeddingsManager):
  def __init__(
      self,
      model_name: str,
      columns: List[str],
      max_seq_length: Optional[int] = None,
      **kwargs):

    super().__init__(columns, **kwargs)
    self.model_name = model_name
    self.max_seq_length = max_seq_length

  def get_model_handler(self):
    return _SentenceTransformerModelHandler(
        model_class=SentenceTransformer,
        max_seq_length=self.max_seq_length,
        model_name=self.model_name,
        inference_fn=inference_fn,
        load_model_args=self.load_model_args,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model)

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    # wrap the model handler in a _TextEmbeddingHandler since
    # the SentenceTransformerEmbeddings works on text input data.
    return (
        RunInference(model_handler=_TextEmbeddingHandler(self))
        # This is required since RunInference performs batching and returns
        # batches. We need to decompose the batches and return the elements
        # in their initial shape to the downstream transforms.
        | _YieldPTransform())

  def requires_chaining(self):
    return False
