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
  HYBRID = "hybrid"  # Combined vector and keyword search
  VECTOR = "vector"  # Vector similarity search only
  KEYWORD = "keyword"  # Keyword/text search only


class KeywordSearchMetrics(Enum):
  """Metrics for keyword search."""
  BM25 = "BM25"  # BM25 ranking algorithm for text relevance


class VectorSearchMetrics(Enum):
  """Metrics for vector search."""
  COSINE = "COSINE"  # Cosine similarity (1 = identical, 0 = orthogonal)
  L2 = "L2"  # Euclidean distance (smaller = more similar)
  IP = "IP"  # Inner product (larger = more similar)


class MilvusBaseRanker:
  def __int__(self):
    return

  def dict(self):
    return {}

  def __str__(self):
    return self.dict().__str__()


@dataclass
class MilvusConnectionParameters:
  # URI endpoint for connecting to Milvus server.
  # Format: "http(s)://hostname:port".
  uri: str

  # Username for authentication.
  # Required if not using token authentication.
  user: str = field(default_factory=str)

  # Password for authentication.
  # Required if not using token authentication.
  password: str = field(default_factory=str)

  # Database ID to connect to.
  # Specifies which Milvus database to use.
  db_id: str = "default"

  # Authentication token.
  # Alternative to username/password authentication.
  token: str = field(default_factory=str)

  # Connection timeout in seconds.
  # If None, the client's default timeout is used.
  timeout: Optional[float] = None

  def __post_init__(self):
    if not self.uri:
      raise ValueError("URI must be provided for Milvus connection")


@dataclass
class BaseSearchParameters:
  """Parameters for base (vector or keyword) search."""
  # Boolean expression string for filtering search results.
  # Example: 'price <= 1000 AND category == "electronics"'.
  filter: str = field(default_factory=str)

  # Maximum number of results to return per query.
  # Must be a positive integer.
  limit: int = 3

  # Additional search parameters specific to the search type.
  search_params: Dict[str, Any] = field(default_factory=dict)

  # Field name containing the vector or text to search.
  # Required for both vector and keyword search.
  anns_field: Optional[str] = None

  # Consistency level for read operations
  # Options: "Strong", "Session", "Bounded", "Eventually".
  consistency_level: Optional[str] = None

  def __post_init__(self):
    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


@dataclass
class VectorSearchParameters(BaseSearchParameters):
  """Parameters for vector search."""
  # Inherits all fields from BaseSearchParameters.
  # Can add vector-specific parameters here.


@dataclass
class KeywordSearchParameters(BaseSearchParameters):
  """Parameters for keyword search."""
  # Inherits all fields from BaseSearchParameters.
  # Can add keyword-specific parameters here.


@dataclass
class HybridSearchParameters:
  """Parameters for hybrid (vector + keyword) search."""
  # Ranker for combining vector and keyword search results.
  # Example: RRFRanker(weight_vector=0.6, weight_keyword=0.4).
  ranker: MilvusBaseRanker

  # Maximum number of results to return per query
  # Must be a positive integer.
  limit: int = 3

  def __post_init__(self):
    if not self.ranker:
      raise ValueError("Ranker must be provided for hybrid search")

    if self.limit <= 0:
      raise ValueError(f"Search limit must be positive, got {self.limit}")


@dataclass
class MilvusSearchParameters:
  """Parameters configuring Milvus vector/keyword/hybrid search operations."""
  # Name of the collection to search in.
  # Must be an existing collection in the Milvus database.
  collection_name: str

  # Type of search to perform (VECTOR, KEYWORD, or HYBRID).
  # Specifies the search approach that determines which parameters and Milvus
  # APIs will be utilized.
  search_strategy: SearchStrategy

  # Parameters for vector search.
  # Required when search_strategy is VECTOR or HYBRID.
  vector: Optional[VectorSearchParameters] = None

  # Parameters for keyword search.
  # Required when search_strategy is KEYWORD or HYBRID.
  keyword: Optional[KeywordSearchParameters] = None

  # Parameters for hybrid search.
  # Required when search_strategy is HYBRID.
  hybrid: Optional[HybridSearchParameters] = None

  # List of partition names to restrict the search to.
  # If None or empty, all partitions will be searched.
  partition_names: List[str] = field(default_factory=list)

  # List of field names to include in search results.
  # If None or empty, only primary fields including distances will be returned.
  output_fields: List[str] = field(default_factory=list)

  # Search operation timeout in seconds
  # If None, the client's default timeout is used.
  timeout: Optional[float] = None

  # Number of decimal places for distance/similarity scores.
  # -1 means no rounding.
  round_decimal: int = -1

  def __post_init__(self):
    # Validate that collection_name is set
    if not self.collection_name:
      raise ValueError("Collection name must be provided")

    # Validate that search_strategy is set
    if not self.search_strategy:
      raise ValueError("Search strategy must be provided")

    # Validate that the search_strategy variant chosen has all parameters it needs.
    if self.search_strategy == SearchStrategy.VECTOR and not self.vector:
      raise ValueError(
          "Vector search parameters must be provided for VECTOR search strategy"
      )

    if self.search_strategy == SearchStrategy.KEYWORD and not self.keyword:
      raise ValueError(
          "Keyword search parameters must be provided for KEYWORD search strategy"
      )

    if self.search_strategy == SearchStrategy.HYBRID:
      if not self.vector:
        raise ValueError(
            "Vector search parameters must be provided for HYBRID search strategy"
        )
      if not self.keyword:
        raise ValueError(
            "Keyword search parameters must be provided for HYBRID search strategy"
        )
      if not self.hybrid:
        raise ValueError(
            "Hybrid search parameters must be provided for HYBRID search strategy"
        )


@dataclass
class MilvusCollectionLoadParameters:
  """Parameters that control how Milvus loads a collection into memory."""
  # If True, forces a reload of the collection even if already loaded
  # Use this when you need to ensure the most up-to-date data is in memory.
  refresh: bool = field(default_factory=bool)

  # List of resource groups to load the collection into
  # Can be used for load balancing across multiple query nodes.
  resource_groups: List[str] = field(default_factory=list)

  # Specify which fields to load into memory
  # Loading only necessary fields reduces memory usage.
  # If empty, all fields will be loaded.
  load_fields: List[str] = field(default_factory=list)

  # If True, dynamic/growing fields will not be loaded into memory
  # Use this to save memory when dynamic fields aren't needed for queries.
  skip_load_dynamic_field: bool = field(default_factory=bool)


class MilvusSearchEnrichmentHandler(
    EnrichmentSourceHandler[Union[Chunk, List[Chunk]],
                            List[Tuple[Chunk, Dict[str, Any]]]]):
  def __init__(
      self,
      connection_parameters: MilvusConnectionParameters,
      search_parameters: MilvusSearchParameters,
      collection_load_parameters: MilvusCollectionLoadParameters,
      *,
      min_batch_size: int = 1,
      max_batch_size: int = 1000,
      **kwargs):
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
    self._client = MilvusClient(**self._connection_parameters.__dict__)
    self._client.load_collection(
        collection_name=self.collection_name,
        partition_names=self.partition_names,
        **self._collection_load_parameters.__dict__)

  def __call__(self, request: Union[Chunk, List[Chunk]], *args,
               **kwargs) -> List[Tuple[Chunk, Dict[str, Any]]]:
    reqs = request if isinstance(request, list) else [request]
    search_result = self._search_documents(reqs, self._search_parameters)
    return self._get_call_response(reqs, search_result)

  def _search_documents(
      self, chunks: List[Chunk], search_parameters: MilvusSearchParameters):
    if self.search_strategy == SearchStrategy.HYBRID:
      data = self._get_hybrid_search_data(
          chunks, search_parameters.vector, search_parameters.keyword)
      return self._client.hybrid_search(
          collection_name=self.collection_name,
          partition_names=self.partition_names,
          output_fields=self.output_fields,
          timeout=self.timeout,
          round_decimal=self.round_decimal,
          reqs=data,
          **search_parameters.hybrid.__dict__)
    elif self.search_strategy == SearchStrategy.VECTOR:
      data = list(map(self._get_vector_search_data, chunks))
      return self._client.search(
          collection_name=self.collection_name,
          partition_names=self.partition_names,
          output_fields=self.output_fields,
          timeout=self.timeout,
          round_decimal=self.round_decimal,
          data=data,
          **search_parameters.vector.__dict__)
    elif self.search_strategy == SearchStrategy.KEYWORD:
      data = list(map(self._get_keyword_search_data, chunks))
      return self._client.search(
          collection_name=self.collection_name,
          partition_names=self.partition_names,
          output_fields=self.output_fields,
          timeout=self.timeout,
          round_decimal=self.round_decimal,
          data=data,
          **search_parameters.keyword.__dict__)
    else:
      raise ValueError(
          f"Not supported search strategy yet: {self.search_strategy}")

  def _get_hybrid_search_data(
      self,
      chunks: List[Chunk],
      vector_search_params: VectorSearchParameters,
      keyword_search_params: KeywordSearchParameters):
    vector_search_data = list(map(self._get_vector_search_data, chunks))
    keyword_search_data = list(map(self._get_keyword_search_data, chunks))

    vector_search_req = AnnSearchRequest(
        data=vector_search_data,
        anns_field=vector_search_params.anns_field,
        param=vector_search_params.search_params,
        limit=vector_search_params.limit,
        expr=vector_search_params.filter)

    keyword_search_req = AnnSearchRequest(
        data=keyword_search_data,
        anns_field=keyword_search_params.anns_field,
        param=keyword_search_params.search_params,
        limit=keyword_search_params.limit,
        expr=keyword_search_params.filter)

    reqs = [vector_search_req, keyword_search_req]
    return reqs

  def _get_vector_search_data(self, chunk: Chunk):
    if not getattr(chunk.embedding, 'dense_embedding', None):
      raise ValueError(
          f"Chunk {chunk.id} missing dense embedding required for vector search"
      )
    return chunk.embedding.dense_embedding

  def _get_keyword_search_data(self, chunk: Chunk):
    if not chunk.content.text and not getattr(
        chunk.embedding, 'sparse_embedding', None):
      raise ValueError(
          f"Chunk {chunk.id} missing both text content and sparse embedding required for keyword search"
      )
    return chunk.content.text or chunk.embedding.sparse_embedding

  def _get_call_response(
      self, chunks: List[Chunk], search_result: SearchResult[Hits]):
    response = []
    for i in range(len(chunks)):
      chunk = chunks[i]
      hits: Hits = search_result[i]
      result = defaultdict(list)
      for hit in hits:
        hit: Hit
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
    if isinstance(value, Sequence) and not isinstance(value,
                                                      (str, dict, bytes)):
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
