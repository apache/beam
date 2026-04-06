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

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

try:
  from qdrant_client import QdrantClient, models
except ImportError:
  logging.warning("Qdrant client library is not installed.")

import apache_beam as beam
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.types import EmbeddableItem

DEFAULT_WRITE_BATCH_SIZE = 1000


@dataclass
class QdrantConnectionParameters:
  location: Optional[str] = None
  url: Optional[str] = None
  port: Optional[int] = 6333
  grpc_port: int = 6334
  prefer_grpc: bool = False
  https: Optional[bool] = None
  api_key: Optional[str] = None
  prefix: Optional[str] = None
  timeout: Optional[int] = None
  host: Optional[str] = None
  path: Optional[str] = None
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not (self.location or self.url or self.host or self.path):
      raise ValueError(
          "One of location, url, host, or path must be provided for Qdrant")


@dataclass
class QdrantWriteConfig(VectorDatabaseWriteConfig):
  connection_params: QdrantConnectionParameters
  collection_name: str
  timeout: Optional[float] = None
  batch_size: int = DEFAULT_WRITE_BATCH_SIZE
  kwargs: Dict[str, Any] = field(default_factory=dict)
  dense_embedding_key: str = "dense"
  sparse_embedding_key: str = "sparse"

  def __post_init__(self):
    if not self.collection_name:
      raise ValueError("Collection name must be provided")

  def create_write_transform(self) -> beam.PTransform[EmbeddableItem, Any]:
    return _QdrantWriteTransform(self)

  def create_converter(
      self) -> Callable[[EmbeddableItem], 'models.PointStruct']:
    def convert(item: EmbeddableItem) -> 'models.PointStruct':
      if item.dense_embedding is None and item.sparse_embedding is None:
        raise ValueError(
            "EmbeddableItem must have at least one embedding (dense or sparse)")
      vector = {}
      if item.dense_embedding is not None:
        vector[self.dense_embedding_key] = item.dense_embedding
      if item.sparse_embedding is not None:
        sparse_indices, sparse_values = item.sparse_embedding
        vector[self.sparse_embedding_key] = models.SparseVector(
            indices=sparse_indices,
            values=sparse_values,
        )
      id = (
          int(item.id)
          if isinstance(item.id, str) and item.id.isdigit() else item.id)
      return models.PointStruct(
          id=id,
          vector=vector,
          payload=item.metadata if item.metadata else None,
      )

    return convert


class _QdrantWriteTransform(beam.PTransform):
  def __init__(self, config: QdrantWriteConfig):
    self.config = config

  def expand(self, input_or_inputs: beam.PCollection[EmbeddableItem]):
    return (
        input_or_inputs
        | "Convert to Records" >> beam.Map(self.config.create_converter())
        | beam.ParDo(_QdrantWriteFn(self.config)))


class _QdrantWriteFn(beam.DoFn):
  def __init__(self, config: QdrantWriteConfig):
    self.config = config
    self._batch = []
    self._client: 'Optional[QdrantClient]' = None

  def process(self, element, *args, **kwargs):
    self._batch.append(element)
    if len(self._batch) >= self.config.batch_size:
      self._flush()

  def setup(self):
    params = self.config.connection_params
    self._client = QdrantClient(
        location=params.location,
        url=params.url,
        port=params.port,
        grpc_port=params.grpc_port,
        prefer_grpc=params.prefer_grpc,
        https=params.https,
        api_key=params.api_key,
        prefix=params.prefix,
        timeout=params.timeout,
        host=params.host,
        path=params.path,
        check_compatibility=False,
        **params.kwargs,
    )

  def teardown(self):
    if self._client:
      self._client.close()
      self._client = None

  def finish_bundle(self):
    self._flush()

  def _flush(self):
    if len(self._batch) == 0:
      return
    if not self._client:
      raise RuntimeError("Qdrant client is not initialized")
    self._client.upsert(
        collection_name=self.config.collection_name,
        points=self._batch,
        timeout=self.config.timeout,
        **self.config.kwargs,
    )
    self._batch = []

  def display_data(self):
    res = super().display_data()
    res["collection"] = self.config.collection_name
    res["batch_size"] = self.config.batch_size
    return res
