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

# pylint: skip-file

from typing import Callable

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
# HuggingFaceModelHandlerTensor requires both tensorflow and pytorch as imports.
from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerTensor
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import TextEmbeddingHandler

from sentence_transformers import SentenceTransformer


def inference_fn(batch, model, *args, **kwargs):
  return model.encode(batch)


def yield_elements(elements):
  for element in elements:
    yield element


class SentenceTransformerModelHandler(HuggingFaceModelHandlerTensor):

  # Loading the model is different for SentenceTransformer but
  # rest of the huggingface model handler logic works.
  def load_model(self):
    return self._model_class(self.model_uri)


class SentenceTransformerEmbeddings(EmbeddingsManager):
  def __init__(self, model_uri: str, **kwargs):

    super().__init__(**kwargs)
    self.model_uri = model_uri

  def get_model_handler(self):
    # TODO: try using PytorchModelHandlerTensor since SentenceTransformer
    # is a pytorch model.
    return SentenceTransformerModelHandler(
        model_class=SentenceTransformer,
        model_uri=self.model_uri,
        device=self.device,
        inference_fn=inference_fn,
        load_model_args=self.load_model_args,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model,
        **self.kwargs)

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    # wrap the model handler in a TextEmbeddingHandler since
    # the SentenceTransformerEmbeddings works on text input data.
    return (
        RunInference(model_handler=TextEmbeddingHandler(embedding_config=self))
        # TODO: Phrase comment better. The reason we need to yield
        # RunInference returns a list of Predictions, which if coupled
        # with MLTransfomr won't work.
        | beam.ParDo(yield_elements))

  def requires_chaining(self):
    return False
