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

from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from enum import Enum

from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Embedding
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from dataclasses import field
from pymilvus import MilvusClient, AnnSearchRequest, SearchResult, Hits, Hit
from google.protobuf.json_format import MessageToDict
from collections.abc import Sequence


class SearchStrategy(Enum):
  """Search strategies for information retrieval.

  Args:
    HYBRID: Combines vector and keyword search approaches. Leverages both
      semantic understanding and exact matching. Typically provides the most
      comprehensive results. Useful for queries with both conceptual and
      specific keyword components.
    VECTOR: Vector similarity search only. Based on semantic similarity between
      query and documents. Effective for conceptual searches and finding related
      content. Less sensitive to exact terminology than keyword search.
    KEYWORD: Keyword/text search only. Based on exact or fuzzy matching of
      specific terms. Effective for precise queries where exact wording matters.
      Less effective for conceptual or semantic searches.
  """
  HYBRID = "hybrid"
  VECTOR = "vector"
  KEYWORD = "keyword"


class KeywordSearchMetrics(Enum):
  """Metrics for keyword search.

  Args:
    BM25: Best Match 25 ranking algorithm for text relevance. Combines term
      frequency, inverse document frequency, and document length. Higher scores
      indicate greater relevance. Takes into account diminishing returns of term
      frequency. Balances between exact matching and semantic relevance.
  """
  BM25 = "BM25"


class VectorSearchMetrics(Enum):
  """Metrics for vector search.

  Args:
    COSINE: Range [0 to 1], higher indicate greater similarity. Value 1 means
      vectors point in identical direction. Value 0 means vectors are
      perpendicular to each other (no relationship).
    EUCLIDEAN_DISTANCE (L2): Range [0 to ∞), lower values indicate greater
      similarity. Value 0 means vectors are identical. Larger values mean more
      dissimilarity between vectors.
    INNER_PRODUCT (IP): Range varies based on vector magnitudes, higher values
      indicate greater similarity. Value 0 means vectors are perpendicular to
      each other. Positive values mean vectors share some directional component.
      Negative values mean vectors point in opposing directions.
  """
  COSINE = "COSINE"
  EUCLIDEAN_DISTANCE = "L2"
  INNER_PRODUCT = "IP"


class MilvusBaseRanker:
  """Base class for ranking algorithms in Milvus hybrid search strategy."""
  def __int__(self):
    return

  def dict(self):
    return {}

  def __str__(self):
    return self.dict().__str__()


@dataclass
class MilvusConnectionParameters:
  """Parameters for establishing connections to Milvus servers.

  Args:
    uri: URI endpoint for connecting to Milvus server in the format
      "http(s)://hostname:port".
    user: Username for authentication. Required if authentication is enabled and
      not using token authentication.
    password: Password for authentication. Required if authentication is enabled
      and not using token authentication.
    db_id: Database ID to connect to. Specifies which Milvus database to use.
      Defaults to 'default'.
    token: Authentication token as an alternative to username/password.
    timeout: Connection timeout in seconds. Uses client default if None.
    kwargs: Optional keyword arguments for additional connection parameters.
      Enables forward compatibility.
  """
  uri: str
  user: str = field(default_factory=str)
  password: str = field(default_factory=str)
  db_id: str = "default"
  token: str = field(default_factory=str)
  timeout: Optional[float] = None
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.uri:
      raise ValueError("URI must be provided for Milvus connection")


@dataclass
class BaseSearchParameters:
  """Base parameters for both vector and keyword search operations.

  Args:
    anns_field: Approximate nearest neighbor search field indicates field name
      containing the embedding to search. Required for both vector and keyword
      search.
    limit: Maximum number of results to return per query. Must be positive.
      Defaults to 3 search results.
    filter: Boolean expression string for filtering search results.
      Example: 'price <= 1000 AND category == "electronics"'.
    search_params: Additional search parameters specific to the search type.
      Example: {"metric_type": VectorSearchMetrics.EUCLIDEAN_DISTANCE}.
    consistency_level: Consistency level for read operations.
      Options: "Strong", "Session", "Bounded", "Eventually". Defaults to
      "Bounded" if not specified when creating the collection.
  """
  anns_field: str
  limit: int = 3
  filter: str = field(default_factory=str)
  search_params: Dict[str, Any] = field(default_factory=dict)
  consistency_level: Optional[str] = None

  def __post_init__(self):
    if not self.anns_field:
      raise ValueError(
          "Approximate Nearest Neighbor Search (ANNS) field must be provided")

    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


@dataclass
class VectorSearchParameters(BaseSearchParameters):
  """Parameters for vector similarity search operations.

  Inherits all parameters from BaseSearchParameters with the same semantics.
  The anns_field should contain dense vector embeddings for this search type.

  Args:
    kwargs: Optional keyword arguments for additional vector search parameters.
      Enables forward compatibility.

  Note:
    For inherited parameters documentation, see BaseSearchParameters.
  """
  kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class KeywordSearchParameters(BaseSearchParameters):
  """Parameters for keyword/text search operations.

  This class inherits all parameters from BaseSearchParameters with the same
  semantics. The anns_field should contain sparse vector embeddings content for
  this search type.

  Args:
    kwargs: Optional keyword arguments for additional keyword search parameters.
      Enables forward compatibility.

  Note:
    For inherited parameters documentation, see BaseSearchParameters.
  """
  kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HybridSearchParameters:
  """Parameters for hybrid (vector + keyword) search operations.

  Args:
    ranker: Ranker for combining vector and keyword search results.
      Example: RRFRanker(k=100).
    limit: Maximum number of results to return per query. Defaults to 3 search
      results.
    kwargs: Optional keyword arguments for additional hybrid search parameters.
      Enables forward compatibility.
  """
  ranker: MilvusBaseRanker
  limit: int = 3
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.ranker:
      raise ValueError("Ranker must be provided for hybrid search")

    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


@dataclass
class HybridSearchNamespace:
  """Namespace containing all parameters for hybrid search operations.

  Args:
    vector: Parameters for the vector search component.
    keyword: Parameters for the keyword search component.
    hybrid: Parameters for combining the vector and keyword results.
  """
  vector: VectorSearchParameters
  keyword: KeywordSearchParameters
  hybrid: HybridSearchParameters

  def __post_init__(self):
    if not self.vector or not self.keyword or not self.hybrid:
      raise ValueError(
          "Vector, keyword, and hybrid search parameters must be provided for "
          "hybrid search")


SearchStrategyType = Union[VectorSearchParameters,
                           KeywordSearchParameters,
                           HybridSearchNamespace]


@dataclass
class MilvusSearchParameters:
  """Parameters configuring Milvus search operations.

  This class encapsulates all parameters needed to execute searches against
  Milvus collections, supporting vector, keyword, and hybrid search strategies.

  Args:
    collection_name: Name of the collection to search in.
    search_strategy: Type of search to perform (VECTOR, KEYWORD, or HYBRID).
    partition_names: List of partition names to restrict the search to. If
      empty, all partitions will be searched.
    output_fields: List of field names to include in search results. If empty,
      only primary fields including distances will be returned.
    timeout: Search operation timeout in seconds. If not specified, the client's
      default timeout is used.
    round_decimal: Number of decimal places for distance/similarity scores.
      Defaults to -1 means no rounding.
  """
  collection_name: str
  search_strategy: SearchStrategyType
  partition_names: List[str] = field(default_factory=list)
  output_fields: List[str] = field(default_factory=list)
  timeout: Optional[float] = None
  round_decimal: int = -1

  def __post_init__(self):
    if not self.collection_name:
      raise ValueError("Collection name must be provided")

    if not self.search_strategy:
      raise ValueError("Search strategy must be provided")


@dataclass
class MilvusCollectionLoadParameters:
  """Parameters that control how Milvus loads a collection into memory.

  This class provides fine-grained control over collection loading, which is
  particularly important in resource-constrained environments. Proper 
  configuration can significantly reduce memory usage and improve query 
  performance by loading only necessary data.

  Args:
    refresh: If True, forces a reload of the collection even if already loaded.
      Ensures the most up-to-date data is in memory.
    resource_groups: List of resource groups to load the collection into. Can be
      used for load balancing across multiple query nodes.
    load_fields: Specify which fields to load into memory. Loading only
      necessary fields reduces memory usage. If empty, all fields loaded.
    skip_load_dynamic_field: If True, dynamic/growing fields will not be loaded
      into memory. Saves memory when dynamic fields aren't needed.
    kwargs: Optional keyword arguments for additional collection load
      parameters. Enables forward compatibility.
  """
  refresh: bool = field(default_factory=bool)
  resource_groups: List[str] = field(default_factory=list)
  load_fields: List[str] = field(default_factory=list)
  skip_load_dynamic_field: bool = field(default_factory=bool)
  kwargs: Dict[str, Any] = field(default_factory=dict)


InputT, OutputT = Union[Chunk, List[Chunk]], List[Tuple[Chunk, Dict[str, Any]]]


class MilvusSearchEnrichmentHandler(EnrichmentSourceHandler[InputT, OutputT]):
  """Enrichment handler for Milvus vector database searches.

  This handler is designed to work with the
  :class:`apache_beam.transforms.enrichment.EnrichmentSourceHandler` transform.
  It enables enriching data through vector similarity, keyword, or hybrid
  searches against Milvus collections.

  The handler supports different search strategies:
  * Vector search - For finding similar embeddings based on vector similarity
  * Keyword search - For text-based retrieval using BM25 or other text metrics
  * Hybrid search - For combining vector and keyword search results

  This handler queries the Milvus database per element by default. To enable
  batching for improved performance, set the `min_batch_size` and 
  `max_batch_size` parameters. These control the batching behavior in the
  :class:`apache_beam.transforms.utils.BatchElements` transform.

  For memory-intensive operations, the handler allows fine-grained control over
  collection loading through the `collection_load_parameters`.
  """
  def __init__(
      self,
      connection_parameters: MilvusConnectionParameters,
      search_parameters: MilvusSearchParameters,
      collection_load_parameters: MilvusCollectionLoadParameters,
      *,
      min_batch_size: int = 1,
      max_batch_size: int = 1000,
      **kwargs):
    """
    Example Usage:
      connection_paramters = MilvusConnectionParameters(
        uri="http://localhost:19530")
      search_parameters = MilvusSearchParameters(
        collection_name="my_collection",
        search_strategy=VectorSearchParameters(anns_field="embedding"))
      collection_load_parameters = MilvusCollectionLoadParameters(
        load_fields=["embedding", "metadata"]),
      milvus_handler = MilvusSearchEnrichmentHandler(
        connection_paramters,
        search_parameters,
        collection_load_parameters,
        min_batch_size=10,
        max_batch_size=100)

    Args:
      connection_parameters (MilvusConnectionParameters): Configuration for
        connecting to the Milvus server, including URI, credentials, and
        connection options.
      search_parameters (MilvusSearchParameters): Configuration for search
        operations, including collection name, search strategy, and output
        fields.
      collection_load_parameters (MilvusCollectionLoadParameters): Parameters
        controlling how collections are loaded into memory, which can
        significantly impact resource usage and performance.
      min_batch_size (int): Minimum number of elements to batch together when
        querying Milvus. Default is 1 (no batching when max_batch_size is 1).
      max_batch_size (int): Maximum number of elements to batch together.Default
        is 1000. Higher values may improve throughput but increase memory usage.
      **kwargs: Additional keyword arguments for Milvus Enrichment Handler.

    Note:
      * For large collections, consider setting appropriate values in
        collection_load_parameters to reduce memory usage.
      * The search_strategy in search_parameters determines the type of search
        (vector, keyword, or hybrid) and associated parameters.
      * Batching can significantly improve performance but requires more memory.
    """
    self._connection_parameters = connection_parameters
    self._search_parameters = search_parameters
    self._collection_load_parameters = collection_load_parameters
    self.kwargs = kwargs
    self._batching_kwargs = {
        'min_batch_size': min_batch_size, 'max_batch_size': max_batch_size
    }
    self.join_fn = join_fn
    self.use_custom_types = True

  def __enter__(self):
    connectionParams = unpack_dataclass_with_kwargs(self._connection_parameters)
    loadCollectionParams = unpack_dataclass_with_kwargs(
        self._collection_load_parameters)
    self._client = MilvusClient(**connectionParams)
    self._client.load_collection(
        collection_name=self.collection_name,
        partition_names=self.partition_names,
        **loadCollectionParams)

  def __call__(self, request: Union[Chunk, List[Chunk]], *args,
               **kwargs) -> List[Tuple[Chunk, Dict[str, Any]]]:
    reqs = request if isinstance(request, list) else [request]
    search_result = self._search_documents(reqs)
    return self._get_call_response(reqs, search_result)

  def _search_documents(self, chunks: List[Chunk]):
    if isinstance(self.search_strategy, HybridSearchNamespace):
      data = self._get_hybrid_search_data(chunks)
      hybridSearchParmas = unpack_dataclass_with_kwargs(
          self.search_strategy.hybrid)
      return self._client.hybrid_search(
          collection_name=self.collection_name,
          partition_names=self.partition_names,
          output_fields=self.output_fields,
          timeout=self.timeout,
          round_decimal=self.round_decimal,
          reqs=data,
          **hybridSearchParmas)
    elif isinstance(self.search_strategy, VectorSearchParameters):
      data = list(map(self._get_vector_search_data, chunks))
      vectorSearchParams = unpack_dataclass_with_kwargs(self.search_strategy)
      return self._client.search(
          collection_name=self.collection_name,
          partition_names=self.partition_names,
          output_fields=self.output_fields,
          timeout=self.timeout,
          round_decimal=self.round_decimal,
          data=data,
          **vectorSearchParams)
    elif isinstance(self.search_strategy, KeywordSearchParameters):
      data = list(map(self._get_keyword_search_data, chunks))
      keywordSearchParams = unpack_dataclass_with_kwargs(self.search_strategy)
      return self._client.search(
          collection_name=self.collection_name,
          partition_names=self.partition_names,
          output_fields=self.output_fields,
          timeout=self.timeout,
          round_decimal=self.round_decimal,
          data=data,
          **keywordSearchParams)
    else:
      raise ValueError(
          f"Not supported search strategy yet: {self.search_strategy}")

  def _get_hybrid_search_data(self, chunks: List[Chunk]):
    vector_search_data = list(map(self._get_vector_search_data, chunks))
    keyword_search_data = list(map(self._get_keyword_search_data, chunks))

    vector_search_req = AnnSearchRequest(
        data=vector_search_data,
        anns_field=self.search_strategy.vector.anns_field,
        param=self.search_strategy.vector.search_params,
        limit=self.search_strategy.vector.limit,
        expr=self.search_strategy.vector.filter)

    keyword_search_req = AnnSearchRequest(
        data=keyword_search_data,
        anns_field=self.search_strategy.keyword.anns_field,
        param=self.search_strategy.keyword.search_params,
        limit=self.search_strategy.keyword.limit,
        expr=self.search_strategy.keyword.filter)

    reqs = [vector_search_req, keyword_search_req]
    return reqs

  def _get_vector_search_data(self, chunk: Chunk):
    dense_vector_found = chunk.embedding and chunk.embedding.dense_embedding
    if not dense_vector_found:
      raise ValueError(
          f"Chunk {chunk.id} missing dense embedding required for vector search"
      )
    return chunk.embedding.dense_embedding

  def _get_keyword_search_data(self, chunk: Chunk):
    sparse_vector_found = chunk.embedding and chunk.embedding.sparse_embedding
    if not chunk.content.text and not sparse_vector_found:
      raise ValueError(
          f"Chunk {chunk.id} missing both text content and sparse embedding "
          "required for keyword search")
    return chunk.content.text or chunk.embedding.sparse_embedding

  def _get_call_response(
      self, chunks: List[Chunk], search_result: SearchResult[Hits]):
    response = []
    for i in range(len(chunks)):
      chunk = chunks[i]
      hits: Hits = search_result[i]
      result = defaultdict(list)
      for i in range(len(hits)):
        hit: Hit = hits[i]
        normalized_fields = self._normalize_milvus_fields(hit.fields)
        result["id"].append(hit.id)
        result["distance"].append(hit.distance)
        result["fields"].append(normalized_fields)
      response.append((chunk, result))
    return response

  def _normalize_milvus_fields(self, fields: Dict[str, Any]):
    normalized_fields = {}
    for field, value in fields.items():
      value = self._normalize_milvus_value(value)
      normalized_fields[field] = value
    return normalized_fields

  def _normalize_milvus_value(self, value: Any):
    # Convert Milvus-specific types to Python native types.
    is_not_string_dict_or_bytes = not isinstance(value, (str, dict, bytes))
    if isinstance(value, Sequence) and is_not_string_dict_or_bytes:
      return list(value)
    elif hasattr(value, 'DESCRIPTOR'):
      # Handle protobuf messages.
      return MessageToDict(value)
    else:
      # Keep other types as they are.
      return value

  @property
  def collection_name(self):
    """Getter method for collection_name property"""
    return self._search_parameters.collection_name

  @property
  def search_strategy(self):
    """Getter method for search_strategy property"""
    return self._search_parameters.search_strategy

  @property
  def partition_names(self):
    """Getter method for partition_names property"""
    return self._search_parameters.partition_names

  @property
  def output_fields(self):
    """Getter method for output_fields property"""
    return self._search_parameters.output_fields

  @property
  def timeout(self):
    """Getter method for search timeout property"""
    return self._search_parameters.timeout

  @property
  def round_decimal(self):
    """Getter method for search round_decimal property"""
    return self._search_parameters.round_decimal

  def __exit__(self, exc_type, exc_val, exc_tb):
    self._client.release_collection(self.collection_name)
    self._client.close()
    self._client = None

  def batch_elements_kwargs(self) -> Dict[str, int]:
    """Returns kwargs for beam.BatchElements."""
    return self._batching_kwargs


def join_fn(left: Embedding, right: Dict[str, Any]) -> Embedding:
  left.metadata['enrichment_data'] = right
  return left


def unpack_dataclass_with_kwargs(dataclass_instance):
  # Create a copy of the dataclass's __dict__.
  params_dict: dict = dataclass_instance.__dict__.copy()

  # Extract the nested kwargs dictionary.
  nested_kwargs = params_dict.pop('kwargs', {})

  # Merge the dictionaries, with nested_kwargs taking precedence
  # in case of duplicate keys.
  return {**params_dict, **nested_kwargs}
