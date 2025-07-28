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
#

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, Tuple

from qdrant_client import QdrantClient, models

from apache_beam.ml.rag.types import Chunk, Embedding
from apache_beam.transforms.enrichment import EnrichmentSourceHandler


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
class QdrantDenseSearchParameters:
  vector_name: str = "dense"
  limit: int = 3
  filter: Optional[models.Filter] = None
  score_threshold: Optional[float] = None
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


@dataclass
class QdrantSparseSearchParameters:
  vector_name: str = "sparse"
  limit: int = 3
  filter: Optional[models.Filter] = None
  score_threshold: Optional[float] = None
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


@dataclass
class QdrantHybridSearchParameters:
  dense: QdrantDenseSearchParameters
  sparse: QdrantSparseSearchParameters
  limit: int = 3
  fusion: models.Fusion = models.Fusion.RRF
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.dense or not self.sparse:
      raise ValueError("Both dense and sparse search params are required")
    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


QdrantSearchStrategyType = Union[
    QdrantDenseSearchParameters,
    QdrantSparseSearchParameters,
    QdrantHybridSearchParameters,
]


@dataclass
class QdrantSearchParameters:
  collection_name: str
  search_strategy: QdrantSearchStrategyType
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.collection_name:
      raise ValueError("Collection name must be provided")
    if not self.search_strategy:
      raise ValueError("Search strategy must be provided")


@dataclass
class QdrantSearchResult:
  """Search result from Qdrant per chunk.

    Args:
        id: List of entity IDs returned from the search.
        score: List of scores (similarity or distance) for each returned entity.
        payload: List of dictionaries containing additional values for entities.
            Each dictionary corresponds to one returned entity.
    """

  id: List[Union[str, int]] = field(default_factory=list)
  score: List[float] = field(default_factory=list)
  payload: List[Dict[str, Any]] = field(default_factory=list)


InputT = Union[Chunk, List[Chunk]]
OutputT = List[Tuple[Chunk, Dict[str, Any]]]


class QdrantSearchEnrichmentHandler(EnrichmentSourceHandler[InputT, OutputT]):
  def __init__(
      self,
      connection_parameters: QdrantConnectionParameters,
      search_parameters: QdrantSearchParameters,
      **kwargs,
  ):
    self._connection_parameters = connection_parameters
    self._search_parameters = search_parameters
    self.kwargs = kwargs
    self.join_fn = join_fn
    self.use_custom_types = True

  def __enter__(self):
    params = self._connection_parameters
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

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._client:
      self._client.close()

  def __call__(
      self, request: Union[Chunk, List[Chunk]], *args, **kwargs) -> OutputT:
    reqs = request if isinstance(request, list) else [request]
    search_result = self._search_documents(reqs)

    results = self._get_call_response(reqs, search_result)

    if isinstance(request, list):
      return results
    else:
      return results[0] if results else (request, {})

  def _search_documents(self, chunks: List[Chunk]):
    strategy = self._search_parameters.search_strategy
    if isinstance(strategy, QdrantHybridSearchParameters):
      return self._hybrid_search(chunks)
    elif isinstance(strategy, QdrantDenseSearchParameters):
      return self._dense_search(chunks)
    elif isinstance(strategy, QdrantSparseSearchParameters):
      return self._sparse_search(chunks)
    else:
      raise ValueError("Unknown Qdrant search strategy")

  def _dense_search(self, chunks: List[Chunk]):
    strategy = self._search_parameters.search_strategy
    results = []
    for chunk in chunks:
      if not chunk.embedding or chunk.embedding.dense_embedding is None:
        raise ValueError(
            f"Chunk {getattr(chunk, 'id', None)} missing dense embedding")
      res = self._client.query_points(
          collection_name=self._search_parameters.collection_name,
          query=chunk.embedding.dense_embedding,
          using=strategy.vector_name,
          limit=strategy.limit,
          query_filter=strategy.filter,
          with_payload=True,
          with_vectors=False,
          score_threshold=strategy.score_threshold,
          **strategy.kwargs,
      ).points
      results.append(res)
    return results

  def _sparse_search(self, chunks: List[Chunk]):
    strategy = self._search_parameters.search_strategy
    results = []
    for chunk in chunks:
      if not chunk.embedding or chunk.embedding.sparse_embedding is None:
        raise ValueError(
            f"Chunk {getattr(chunk, 'id', None)} missing sparse embedding")
      indices, values = chunk.embedding.sparse_embedding
      sparse_vec = models.SparseVector(indices=indices, values=values)
      res = self._client.query_points(
          collection_name=self._search_parameters.collection_name,
          query=sparse_vec,
          using=strategy.vector_name,
          limit=strategy.limit,
          query_filter=strategy.filter,
          with_payload=True,
          with_vectors=False,
          score_threshold=strategy.score_threshold,
          **strategy.kwargs,
      ).points
      results.append(res)
    return results

  def _hybrid_search(self, chunks: List[Chunk]):
    strategy = self._search_parameters.search_strategy
    results = []
    for chunk in chunks:
      dense_vec = chunk.embedding.dense_embedding if chunk.embedding else None
      sparse_emb = chunk.embedding.sparse_embedding if chunk.embedding else None
      if dense_vec is None or sparse_emb is None:
        raise ValueError(
            f"Chunk {getattr(chunk, 'id', None)} missing dense/sparse embedding"
        )
      indices, values = sparse_emb
      sparse_vec = models.SparseVector(indices=indices, values=values)
      res = self._client.query_points(
          collection_name=self._search_parameters.collection_name,
          prefetch=[
              models.Prefetch(
                  using=strategy.dense.vector_name,
                  query=dense_vec,
                  filter=strategy.dense.filter,
                  limit=strategy.dense.limit,
                  params=None,
              ),
              models.Prefetch(
                  using=strategy.sparse.vector_name,
                  query=sparse_vec,
                  filter=strategy.sparse.filter,
                  limit=strategy.sparse.limit,
                  params=None,
              ),
          ],
          query=models.FusionQuery(fusion=strategy.fusion),
          limit=strategy.limit,
          with_payload=True,
          with_vectors=False,
          **strategy.kwargs,
      ).points
      results.append(res)
    return results

  def _get_call_response(
      self, chunks: List[Chunk], search_result: List[List[models.ScoredPoint]]):
    response = []
    for i in range(len(chunks)):
      chunk = chunks[i]
      hits = search_result[i]
      result = QdrantSearchResult()
      for hit in hits:
        result.id.append(hit.id)
        result.score.append(hit.score)
        result.payload.append(hit.payload)
      response.append((chunk, result.__dict__))
    return response


def join_fn(left: Embedding, right: Dict[str, Any]) -> Embedding:
  left.metadata["enrichment_data"] = right
  return left
