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

from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import Any
from typing import Optional
from typing import Union

import grpc
import httpx
from qdrant_client import QdrantClient
from qdrant_client import models
from qdrant_client.common.client_exceptions import ResourceExhaustedResponse
from qdrant_client.http.exceptions import ResponseHandlingException
from qdrant_client.http.exceptions import UnexpectedResponse

from apache_beam.io.requestresponse import UserCodeQuotaException
from apache_beam.io.requestresponse import UserCodeTimeoutException
from apache_beam.ml.rag.ingestion.qdrant import QdrantConnectionParameters
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

__all__ = [
    'QdrantConnectionParameters',
    'QdrantDenseSearchParameters',
    'QdrantSparseSearchParameters',
    'QdrantHybridSearchParameters',
    'QdrantSearchParameters',
    'QdrantSearchResult',
    'QdrantSearchEnrichmentHandler',
]


@dataclass
class QdrantDenseSearchParameters:
  """Parameters for dense vector similarity search.

  Args:
    vector_name: Name of the dense vector in the Qdrant collection. Defaults to
      "dense", matching the default `dense_embedding_key` of
      :class:`~apache_beam.ml.rag.ingestion.qdrant.QdrantWriteConfig`.
    limit: Maximum number of results to return per query. Must be positive.
      Defaults to 3 search results.
    filter: Optional payload filter applied to the search, e.g.
      `models.Filter(must=[models.FieldCondition(key="source",
      match=models.MatchValue(value="docs"))])`.
    score_threshold: Optional minimal score; results scoring worse are dropped.
    search_params: Optional `models.SearchParams` such as `hnsw_ef` or `exact`.
    kwargs: Optional keyword arguments for additional `models.QueryRequest`
      fields. Enables forward compatibility. Explicit non-None fields take
      precedence over the corresponding entries in `kwargs`.
  """
  vector_name: str = "dense"
  limit: int = 3
  filter: Optional[models.Filter] = None
  score_threshold: Optional[float] = None
  search_params: Optional[models.SearchParams] = None
  kwargs: dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.vector_name:
      raise ValueError("Vector name must be provided")

    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


@dataclass
class QdrantSparseSearchParameters:
  """Parameters for sparse vector (keyword) search.

  Args:
    vector_name: Name of the sparse vector in the Qdrant collection. Defaults to
      "sparse", matching the default `sparse_embedding_key` of
      :class:`~apache_beam.ml.rag.ingestion.qdrant.QdrantWriteConfig`.
    limit: Maximum number of results to return per query. Must be positive.
      Defaults to 3 search results.
    filter: Optional payload filter applied to the search.
    score_threshold: Optional minimal score; results scoring worse are dropped.
    search_params: Optional `models.SearchParams`.
    kwargs: Optional keyword arguments for additional `models.QueryRequest`
      fields. Enables forward compatibility. Explicit non-None fields take
      precedence over the corresponding entries in `kwargs`.
  """
  vector_name: str = "sparse"
  limit: int = 3
  filter: Optional[models.Filter] = None
  score_threshold: Optional[float] = None
  search_params: Optional[models.SearchParams] = None
  kwargs: dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.vector_name:
      raise ValueError("Vector name must be provided")

    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


@dataclass
class QdrantHybridSearchParameters:
  """Parameters for hybrid (dense + sparse) search.

  The dense and sparse searches run as prefetches and their results are fused
  server side with the Qdrant Query API. Configure `search_params` on `dense`
  and `sparse` to control their respective prefetches; fusion does not run a
  vector search.

  Args:
    dense: Parameters for the dense search component. Its `limit` is the
      number of candidates prefetched before fusion.
    sparse: Parameters for the sparse search component. Its `limit` is the
      number of candidates prefetched before fusion.
    fusion: Fusion method used to combine both result lists. Defaults to
      reciprocal rank fusion (`models.Fusion.RRF`).
    limit: Maximum number of fused results to return per query. Defaults to 3
      search results.
    filter: Optional common payload filter on the outer query. Qdrant also
      applies it to the prefetches, together with their component filters.
    score_threshold: Optional minimum fused score. Component thresholds apply
      to the individual dense or sparse scores before fusion instead.
    kwargs: Optional keyword arguments for additional `models.QueryRequest`
      fields. Explicit non-None `filter` and `score_threshold` fields take
      precedence over the corresponding entries in `kwargs`.
  """
  dense: QdrantDenseSearchParameters
  sparse: QdrantSparseSearchParameters
  fusion: models.Fusion = models.Fusion.RRF
  limit: int = 3
  kwargs: dict[str, Any] = field(default_factory=dict)
  filter: Optional[models.Filter] = None
  score_threshold: Optional[float] = None

  def __post_init__(self):
    if not self.dense or not self.sparse:
      raise ValueError(
          "Dense and sparse search parameters must be provided for "
          "hybrid search")

    if not self.fusion:
      raise ValueError("Fusion must be provided for hybrid search")

    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


QdrantSearchStrategyType = Union[QdrantDenseSearchParameters,
                                 QdrantSparseSearchParameters,
                                 QdrantHybridSearchParameters]


@dataclass
class QdrantSearchParameters:
  """Parameters configuring Qdrant search operations.

  Args:
    collection_name: Name of the collection to search in.
    search_strategy: Dense, sparse or hybrid search parameters.
    with_payload: Whether to return the payload of each point. Either a bool
      or a list of payload keys to return. Defaults to True.
    consistency: Optional read consistency for the search, e.g.
      `models.ReadConsistencyType.MAJORITY`.
    timeout: Optional search timeout in seconds. If not specified, the client's
      default timeout is used.
  """
  collection_name: str
  search_strategy: QdrantSearchStrategyType
  with_payload: Union[bool, list[str]] = True
  consistency: Optional[models.ReadConsistency] = None
  timeout: Optional[int] = None

  def __post_init__(self):
    if not self.collection_name:
      raise ValueError("Collection name must be provided")

    if not self.search_strategy:
      raise ValueError("Search strategy must be provided")


@dataclass
class QdrantSearchResult:
  """Search result from Qdrant per embeddable_item.

  Args:
    id: List of point IDs returned from the search. Can be either string (UUID)
      or integer IDs.
    score: List of scores for each returned point. Higher is more similar for
      cosine and dot product, lower is more similar for Euclidean and Manhattan
      distances.
    payload: List of payload dictionaries, one for each returned point. Empty
      if the point has no payload or `with_payload` is False.
  """
  id: list[Union[str, int]] = field(default_factory=list)
  score: list[float] = field(default_factory=list)
  payload: list[dict[str, Any]] = field(default_factory=list)


InputT, OutputT = (Union[EmbeddableItem, list[EmbeddableItem]],
                    list[tuple[EmbeddableItem, dict[str, Any]]])


class QdrantSearchEnrichmentHandler(EnrichmentSourceHandler[InputT, OutputT]):
  """Enrichment handler for Qdrant vector database searches.

  This handler is designed to work with the
  :class:`apache_beam.transforms.enrichment.EnrichmentSourceHandler` transform.
  It enables enriching data through dense, sparse, or hybrid searches against
  Qdrant collections, such as ones written by
  :class:`~apache_beam.ml.rag.ingestion.qdrant.QdrantWriteConfig`.

  The handler supports different search strategies:
  * Dense search - For finding similar embeddings based on vector similarity
  * Sparse search - For keyword-style retrieval with sparse embeddings
  * Hybrid search - For fusing dense and sparse search results server side

  This handler queries Qdrant per element by default. To enable batching for
  improved performance, set the `min_batch_size` and `max_batch_size`
  parameters. These control the batching behavior in the
  :class:`apache_beam.transforms.utils.BatchElements` transform. Each batch is
  sent to Qdrant as a single batch query request.

  REST and gRPC quota and timeout errors are translated to Beam exceptions
  so that `Enrichment` can retry them using its configured repeater. Other
  client errors propagate unchanged.
  """
  def __init__(
      self,
      connection_parameters: QdrantConnectionParameters,
      search_parameters: QdrantSearchParameters,
      *,
      min_batch_size: int = 1,
      max_batch_size: int = 1000,
      **kwargs):
    """
    Example Usage:
      connection_parameters = QdrantConnectionParameters(
        url="http://localhost:6333")
      search_parameters = QdrantSearchParameters(
        collection_name="my_collection",
        search_strategy=QdrantDenseSearchParameters(limit=5))
      qdrant_handler = QdrantSearchEnrichmentHandler(
        connection_parameters,
        search_parameters,
        min_batch_size=10,
        max_batch_size=100)

    Args:
      connection_parameters (QdrantConnectionParameters): Configuration for
        connecting to Qdrant, shared with
        :class:`~apache_beam.ml.rag.ingestion.qdrant.QdrantWriteConfig`.
      search_parameters (QdrantSearchParameters): Configuration for search
        operations, including collection name and search strategy.
      min_batch_size (int): Minimum number of elements to batch together when
        querying Qdrant. Default is 1 (no batching when max_batch_size is 1).
      max_batch_size (int): Maximum number of elements to batch together.
        Default is 1000. Higher values may improve throughput but increase
        memory usage.
      **kwargs: Additional keyword arguments for Qdrant Enrichment Handler.
    """
    self._connection_parameters = connection_parameters
    self._search_parameters = search_parameters
    self._batching_kwargs = {
        'min_batch_size': min_batch_size, 'max_batch_size': max_batch_size
    }
    self.kwargs = kwargs
    self._client = None
    self.join_fn = join_fn
    self.use_custom_types = True

  def __enter__(self):
    """Enters the context manager and creates the Qdrant client.

    Returns:
      Self, enabling use in 'with' statements.
    """
    if not self._client:
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
    return self

  def __call__(
      self,
      request: Union[EmbeddableItem, list[EmbeddableItem]],
      *args,
      **kwargs) -> list[tuple[EmbeddableItem, dict[str, Any]]]:
    reqs = request if isinstance(request, list) else [request]
    # Early return for empty requests to avoid unnecessary round trips.
    if not reqs:
      return []
    requests = [self._get_query_request(item) for item in reqs]
    try:
      responses = self._client.query_batch_points(
          collection_name=self.collection_name,
          requests=requests,
          consistency=self._search_parameters.consistency,
          timeout=self._search_parameters.timeout)
    except ResourceExhaustedResponse as exc:
      raise UserCodeQuotaException(str(exc)) from exc
    except UnexpectedResponse as exc:
      if exc.status_code == 429:
        raise UserCodeQuotaException(str(exc)) from exc
      if exc.status_code in (408, 504):
        raise UserCodeTimeoutException(str(exc)) from exc
      raise
    except ResponseHandlingException as exc:
      if isinstance(exc.source, httpx.TimeoutException):
        raise UserCodeTimeoutException(str(exc)) from exc
      raise
    except grpc.RpcError as exc:
      if exc.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
        raise UserCodeQuotaException(str(exc)) from exc
      if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        raise UserCodeTimeoutException(str(exc)) from exc
      raise
    return self._get_call_response(reqs, responses)

  def _get_query_request(
      self, embeddable_item: EmbeddableItem) -> models.QueryRequest:
    strategy = self.search_strategy
    with_payload = self._search_parameters.with_payload
    if isinstance(strategy, QdrantHybridSearchParameters):
      return models.QueryRequest(
          **_merge_query_kwargs(
              strategy.kwargs,
              prefetch=[
                  self._get_prefetch(
                      strategy.dense, self._get_dense_query(embeddable_item)),
                  self._get_prefetch(
                      strategy.sparse, self._get_sparse_query(embeddable_item)),
              ],
              query=models.FusionQuery(fusion=strategy.fusion),
              limit=strategy.limit,
              with_payload=with_payload,
              filter=strategy.filter,
              score_threshold=strategy.score_threshold))
    elif isinstance(strategy, QdrantDenseSearchParameters):
      return self._get_single_query_request(
          strategy, self._get_dense_query(embeddable_item), with_payload)
    elif isinstance(strategy, QdrantSparseSearchParameters):
      return self._get_single_query_request(
          strategy, self._get_sparse_query(embeddable_item), with_payload)
    else:
      raise ValueError(f"Not supported search strategy yet: {strategy}")

  def _get_single_query_request(
      self,
      strategy: Union[QdrantDenseSearchParameters,
                      QdrantSparseSearchParameters],
      query: Union[list[float], models.SparseVector],
      with_payload: Union[bool, list[str]]) -> models.QueryRequest:
    return models.QueryRequest(
        **_merge_query_kwargs(
            strategy.kwargs,
            query=query,
            using=strategy.vector_name,
            filter=strategy.filter,
            score_threshold=strategy.score_threshold,
            params=strategy.search_params,
            limit=strategy.limit,
            with_payload=with_payload))

  def _get_prefetch(
      self,
      strategy: Union[QdrantDenseSearchParameters,
                      QdrantSparseSearchParameters],
      query: Union[list[float], models.SparseVector]) -> models.Prefetch:
    return models.Prefetch(
        **_merge_query_kwargs(
            strategy.kwargs,
            query=query,
            using=strategy.vector_name,
            filter=strategy.filter,
            score_threshold=strategy.score_threshold,
            params=strategy.search_params,
            limit=strategy.limit))

  def _get_dense_query(self, embeddable_item: EmbeddableItem) -> list[float]:
    if not embeddable_item.dense_embedding:
      raise ValueError(
          f"Item {embeddable_item.id} missing dense embedding required for"
          " dense search")
    return embeddable_item.dense_embedding

  def _get_sparse_query(
      self, embeddable_item: EmbeddableItem) -> models.SparseVector:
    if not embeddable_item.sparse_embedding:
      raise ValueError(
          f"Item {embeddable_item.id} missing sparse embedding required for"
          " sparse search")
    indices, values = embeddable_item.sparse_embedding
    return models.SparseVector(indices=indices, values=values)

  def _get_call_response(
      self,
      embeddable_items: list[EmbeddableItem],
      responses: list[models.QueryResponse]):
    response = []
    for embeddable_item, query_response in zip(embeddable_items, responses):
      result = QdrantSearchResult()
      for point in query_response.points:
        result.id.append(point.id)
        result.score.append(point.score)
        result.payload.append(point.payload or {})
      response.append((embeddable_item, result.__dict__))
    return response

  @property
  def collection_name(self):
    """Getter method for collection_name property"""
    return self._search_parameters.collection_name

  @property
  def search_strategy(self):
    """Getter method for search_strategy property"""
    return self._search_parameters.search_strategy

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._client:
      try:
        self._client.close()
      finally:
        self._client = None

  def batch_elements_kwargs(self) -> dict[str, int]:
    """Returns kwargs for beam.BatchElements."""
    return self._batching_kwargs


def _merge_query_kwargs(kwargs: dict[str, Any], **explicit) -> dict[str, Any]:
  """Merges user `kwargs` with explicit query fields.

  Explicit fields that are set (not None) take precedence over the same keys
  in `kwargs`; unset explicit fields fall back to `kwargs`. This avoids a
  "multiple values for keyword argument" TypeError at query time.
  """
  merged = dict(kwargs)
  merged.update({k: v for k, v in explicit.items() if v is not None})
  return merged


def join_fn(left: EmbeddableItem, right: dict[str, Any]) -> EmbeddableItem:
  return replace(left, metadata={**left.metadata, 'enrichment_data': right})
