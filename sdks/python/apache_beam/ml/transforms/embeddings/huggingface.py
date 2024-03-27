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

__all__ = ["SentenceTransformerEmbeddings", "InferenceAPIEmbeddings"]

import logging
import os
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence

import requests

import apache_beam as beam
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler

try:
  from sentence_transformers import SentenceTransformer
except ImportError:
  SentenceTransformer = None

_LOGGER = logging.getLogger(__name__)


# TODO: https://github.com/apache/beam/issues/29621
# Use HuggingFaceModelHandlerTensor once the import issue is fixed.
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
      load_model_args: Optional[dict] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_seq_length: Optional[int] = None,
      large_model: bool = False,
      **kwargs):
    self._max_seq_length = max_seq_length
    self.model_name = model_name
    self._model_class = model_class
    self._load_model_args = load_model_args
    self._min_batch_size = min_batch_size
    self._max_batch_size = max_batch_size
    self._large_model = large_model
    self._kwargs = kwargs

    if not SentenceTransformer:
      raise ImportError(
          "sentence-transformers is required to use "
          "SentenceTransformerEmbeddings."
          "Please install it with using `pip install sentence-transformers`.")

  def run_inference(
      self,
      batch: Sequence[str],
      model: SentenceTransformer,
      inference_args: Optional[Dict[str, Any]] = None,
  ):
    inference_args = inference_args or {}
    return model.encode(batch, **inference_args)

  def load_model(self):
    model = self._model_class(self.model_name, **self._load_model_args)
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

  def __repr__(self) -> str:
    # ModelHandler is internal to the user and is not exposed.
    # Hence we need to override the __repr__ method to expose
    # the name of the class.
    return 'SentenceTransformerEmbeddings'


class SentenceTransformerEmbeddings(EmbeddingsManager):
  def __init__(
      self,
      model_name: str,
      columns: List[str],
      max_seq_length: Optional[int] = None,
      **kwargs):
    """
    Embedding config for sentence-transformers. This config can be used with
    MLTransform to embed text data. Models are loaded using the RunInference
    PTransform with the help of ModelHandler.

    Args:
      model_name: Name of the model to use. The model should be hosted on
        HuggingFace Hub or compatible with sentence_transformers.
      columns: List of columns to be embedded.
      max_seq_length: Max sequence length to use for the model if applicable.
      min_batch_size: The minimum batch size to be used for inference.
      max_batch_size: The maximum batch size to be used for inference.
      large_model: Whether to share the model across processes.
    """
    super().__init__(columns, **kwargs)
    self.model_name = model_name
    self.max_seq_length = max_seq_length

  def get_model_handler(self):
    return _SentenceTransformerModelHandler(
        model_class=SentenceTransformer,
        max_seq_length=self.max_seq_length,
        model_name=self.model_name,
        load_model_args=self.load_model_args,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model)

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    # wrap the model handler in a _TextEmbeddingHandler since
    # the SentenceTransformerEmbeddings works on text input data.
    return (
        RunInference(
            model_handler=_TextEmbeddingHandler(self),
            inference_args=self.inference_args,
        ))


class _InferenceAPIHandler(ModelHandler):
  def __init__(self, config: 'InferenceAPIEmbeddings'):
    super().__init__()
    self._config = config

  def load_model(self):
    session = requests.Session()
    # if the token is not provided during construction time, it might have
    # been provided with custom container, which we can get it during runtume.
    if not self._config.hf_token:
      hf_token = os.environ.get("HF_TOKEN")
      if not hf_token:
        raise ValueError(
            'HF_TOKEN environment variable not set. '
            'Please set the environment variable or pass the token as an '
            'argument.')
      session.headers.update({"Authorization": f"Bearer {hf_token}"})
      return session

    session.headers.update(self._config.authorization_token)
    return session

  def run_inference(
      self, batch, session: requests.Session, inference_args=None):
    response = session.post(
        self._config.api_url,
        headers=self._config.authorization_token,
        json={
            "inputs": batch, "options": inference_args
        })
    return response.json()


class InferenceAPIEmbeddings(EmbeddingsManager):
  """
    Feature extraction using HuggingFace's Inference API.
    Intended to be used for feature-extraction. For other tasks, please
    refer to https://huggingface.co/inference-api.

    Args:
      hf_token: HuggingFace token.
      columns: List of columns to be embedded.
      model_name: Model name used for feature extraction.
      api_url: API url for feature extraction. If specified, model_name will be
        ignored. If none, the default url for feature extraction
        will be used.
  """
  def __init__(
      self,
      hf_token: Optional[str],
      columns: List[str],
      model_name: Optional[str] = None, # example: "sentence-transformers/all-MiniLM-l6-v2" # pylint: disable=line-too-long
      api_url: Optional[str] = None,
      **kwargs,
      ):
    super().__init__(columns, **kwargs)
    self._authorization_token = {"Authorization": f"Bearer {hf_token}"}
    self._model_name = model_name
    self.hf_token = hf_token
    if not api_url:
      if not self._model_name:
        raise ValueError("Either api_url or model_name must be provided.")
      self._api_url = (
          f"https://api-inference.huggingface.co/pipeline/feature-extraction/{self._model_name}"  # pylint: disable=line-too-long
      )
    else:
      self._api_url = api_url

    _LOGGER.info("HuggingFace API URL: %s")

  def get_token(self):
    return os.environ.get('HF_TOKEN')

  @property
  def api_url(self):
    return self._api_url

  @property
  def authorization_token(self):
    return self._authorization_token

  def get_model_handler(self):
    return _InferenceAPIHandler(self)

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    options = {
        # sometimes the model is not ready and returns an error response
        # instead of waiting. So we wait for the model to be ready.
        'wait_for_model': True,
        'use_cache': True,
    }
    self.inference_args.update(options)
    return (
        RunInference(
            model_handler=_TextEmbeddingHandler(self),
            inference_args=self.inference_args))
