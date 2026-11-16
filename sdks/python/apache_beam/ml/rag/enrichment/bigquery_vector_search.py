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
from collections.abc import Sequence
from dataclasses import dataclass
import logging
import re
from typing import Any
from typing import Callable
from typing import Optional
from typing import Union

from google.cloud import bigquery

from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.bigquery import _COLUMN_NAME_PATTERN
from apache_beam.transforms.enrichment_handlers.bigquery import _TABLE_NAME_PATTERN
from apache_beam.transforms.enrichment_handlers.bigquery import _to_query_parameter
from apache_beam.transforms.enrichment_handlers.bigquery import _validate_bigquery_identifier

_LOGGER = logging.getLogger(__name__)

_METADATA_PLACEHOLDER_PATTERN = re.compile(
    r"'\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}'|"
    r'"\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}"|'
    r'\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}')
_DISTANCE_TYPE_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


@dataclass
class BigQueryVectorSearchParameters:
  """Parameters for configuring BigQuery vector similarity search.

    This class is used by BigQueryVectorSearchEnrichmentHandler to perform
    vector similarity search using BigQuery's VECTOR_SEARCH function. It
    processes :class:`~apache_beam.ml.rag.types.EmbeddableItem` objects that
    contain :class:`~apache_beam.ml.rag.types.Embedding` and returns similar
    vectors from a BigQuery table.

    BigQueryVectorSearchEnrichmentHandler is used with
    :class:`~apache_beam.transforms.enrichment.Enrichment` transform to enrich
    EmbeddableItems with similar content from a vector database. For example:

    >>> # Create search parameters
    >>> params = BigQueryVectorSearchParameters(
    ...     table_name='project.dataset.embeddings',
    ...     embedding_column='embedding',
    ...     columns=['content'],
    ...     neighbor_count=5
    ... )
    >>> # Use in pipeline
    >>> enriched = (
    ...     embeddable_items
    ...     | "Generate Embeddings" >> MLTransform(...)
    ...     | "Find Similar" >> Enrichment(
    ...         BigQueryVectorSearchEnrichmentHandler(
    ...             project='my-project',
    ...             vector_search_parameters=params
    ...         )
    ...     )
    ... )

    BigQueryVectorSearchParameters encapsulates the configuration needed to
    perform vector similarity search using BigQuery's VECTOR_SEARCH function.
    It handles formatting the query with proper embedding vectors and metadata
    restrictions.

    Example with flattened metadata column:
    
    Table schema::

        embedding: ARRAY<FLOAT64>  # Vector embedding
        content: STRING           # Document content
        language: STRING          # Direct metadata column

    Code::

        >>> params = BigQueryVectorSearchParameters(
        ...     table_name='project.dataset.embeddings',
        ...     embedding_column='embedding',
        ...     columns=['content', 'language'],
        ...     neighbor_count=5,
        ...     # For column 'language', value comes from 
        ...     # embeddable_item.metadata['language']
        ...     metadata_restriction_template="language = '{language}'"
        ... )
        >>> # When processing a embeddable_item with
        >>> # metadata={'language': 'en'}, generates: WHERE language = 'en'

    Example with nested repeated metadata:
    
    Table schema::

        embedding: ARRAY<FLOAT64>  # Vector embedding
        content: STRING           # Document content
        metadata: ARRAY<STRUCT>   # Nested repeated metadata
          key: STRING,
          value: STRING
        >>

    Code::

        >>> params = BigQueryVectorSearchParameters(
        ...     table_name='project.dataset.embeddings',
        ...     embedding_column='embedding',
        ...     columns=['content', 'metadata'],
        ...     neighbor_count=5,
        ...     # check_metadata(field_name, key_to_search,
        ...     # value_from_embeddable_item)
        ...     metadata_restriction_template=(
        ...         "check_metadata(metadata, 'language', '{language}')"
        ...     )
        ... )
        >>> # When processing a embeddable_item with
        >>> # metadata={'language': 'en'},
        >>> # generates: WHERE check_metadata(metadata, 'language', 'en')
        >>> # Searches for {key: 'language', value: 'en'} in metadata array

    Args:
        project: GCP project ID containing the BigQuery dataset
        table_name: Fully qualified BigQuery table name containing vectors.
        embedding_column: Column name containing the embedding vectors.
        columns: List of columns to retrieve from matched vectors.
        neighbor_count: Number of similar vectors to return (top-k).
        metadata_restriction_template: Template string or callable for filtering
            vectors. Template string supports two formats:
            
            1. For flattened metadata columns: 
               ``column_name = '{metadata_key}'`` where column_name is the 
               BigQuery column and metadata_key is used to get the value from 
               embeddable_item.metadata[metadata_key].
            2. For nested repeated metadata (ARRAY<STRUCT<key,value>>):
               ``check_metadata(field_name, 'key_to_match', '{metadata_key}')``
               where field_name is the ARRAY<STRUCT> column in BigQuery,
               key_to_match is the literal key to search for in the array, and
               metadata_key is used to get value from
               embeddable_item.metadata[metadata_key].
            
            Multiple conditions can be combined using AND/OR operators. For
            example::
            
                >>> # Combine metadata check with column filter
                >>> template = (
                ...     "check_metadata(metadata, 'language', '{language}') "
                ...     "AND source = '{source}'"
                ... )
                >>> # When embeddable_item.metadata = {'language': 'en',
                >>> # 'source': 'web'}
                >>> # Generates: WHERE 
                >>> #             check_metadata(metadata, 'language', 'en')
                >>> #           AND source = 'web'
        
        distance_type: Optional distance metric to use. Supported values:
            COSINE (default), EUCLIDEAN, or DOT_PRODUCT.
        options: Optional dictionary of additional VECTOR_SEARCH options.
        include_distance: Reurns the vector search similarity score if True.
    """
  project: str
  table_name: str
  embedding_column: str
  columns: list[str]
  neighbor_count: int
  metadata_restriction_template: Optional[Union[
      str, Callable[[EmbeddableItem], Union[str, tuple[str,
                                                       Sequence[Any]]]]]] = None
  distance_type: Optional[str] = None
  options: Optional[dict[str, Any]] = None
  include_distance: bool = False

  def __post_init__(self) -> None:
    _validate_bigquery_identifier(
        self.table_name, 'table_name', _TABLE_NAME_PATTERN)
    _validate_bigquery_identifier(
        self.embedding_column, 'embedding_column', _COLUMN_NAME_PATTERN)
    for col in self.columns:
      _validate_bigquery_identifier(col, 'column_name', _COLUMN_NAME_PATTERN)
    if self.distance_type is not None:
      _validate_bigquery_identifier(
          self.distance_type, 'distance_type', _DISTANCE_TYPE_PATTERN)

  def _format_restrict(self, item: EmbeddableItem) -> str:
    assert self.metadata_restriction_template is not None, (
        "metadata_restriction_template cannot be None when formatting. "
        "This indicates a logical error in the code."
    )

    if callable(self.metadata_restriction_template):
      res = self.metadata_restriction_template(item)
      return res[0] if isinstance(res, tuple) else res
    return self.metadata_restriction_template.format(**item.metadata)

  def _parameterize_restrict(
      self,
      item: EmbeddableItem,
      param_counter: list[int],
  ) -> tuple[str, list[Any]]:
    assert self.metadata_restriction_template is not None, (
        "metadata_restriction_template cannot be None when formatting. "
        "This indicates a logical error in the code."
    )
    if callable(self.metadata_restriction_template):
      res = self.metadata_restriction_template(item)
      if isinstance(res, tuple):
        cond_sql, raw_params = res
        return cond_sql, [_to_query_parameter(p) for p in raw_params]
      return res, []

    params: list[Any] = []

    def _replace(match: re.Match[str]) -> str:
      key = match.group(1) or match.group(2) or match.group(3)
      assert key is not None
      val = item.metadata[key]
      param_name = f"_bq_meta_{param_counter[0]}"
      param_counter[0] += 1
      params.append(_to_query_parameter(val, name=param_name))
      return f"@{param_name}"

    cond_sql = _METADATA_PLACEHOLDER_PATTERN.sub(
        _replace, self.metadata_restriction_template)
    return cond_sql, params

  def _metadata_group_key(self, item: EmbeddableItem) -> Any:
    if not self.metadata_restriction_template:
      return ""
    if callable(self.metadata_restriction_template):
      res = self.metadata_restriction_template(item)
      if isinstance(res, tuple):
        cond_sql, raw_params = res
        return (cond_sql, tuple(repr(p) for p in raw_params))
      return res
    keys = [
        m.group(1) or m.group(2) or m.group(3)
        for m in _METADATA_PLACEHOLDER_PATTERN.finditer(
            self.metadata_restriction_template)
    ]
    return tuple((k, repr(item.metadata[k])) for k in keys if k is not None)

  def build_query(self, items: list[EmbeddableItem]) -> tuple[str, list[Any]]:
    """Build the parameterized vector search SQL query and query parameters."""
    base_columns_str = ", ".join(f"base.{col}" for col in self.columns)
    columns_str = ", ".join(self.columns)
    distance_clause = (
        f", distance_type => '{self.distance_type}'"
        if self.distance_type else "")
    options_clause = (f", options => {self.options}" if self.options else "")

    metadata_fn = """
        CREATE TEMP FUNCTION check_metadata(
          metadata ARRAY<STRUCT<key STRING, value STRING>>,
          search_key STRING,
          search_value STRING
        )
        AS ((
            SELECT COUNT(*) > 0
            FROM UNNEST(metadata)
            WHERE key = search_key AND value = search_value
        ));
    """ if self.metadata_restriction_template else ""

    condition_groups: dict[Any, list[EmbeddableItem]] = defaultdict(list)
    if self.metadata_restriction_template:
      for item in items:
        group_key = self._metadata_group_key(item)
        condition_groups[group_key].append(item)
    else:
      condition_groups[""] = items

    vector_searches = []
    query_parameters: list[Any] = []
    param_counter = [0]
    item_counter = 0

    for group_items in condition_groups.values():
      condition = ""
      if self.metadata_restriction_template and group_items:
        condition, cond_params = self._parameterize_restrict(
            group_items[0], param_counter)
        query_parameters.extend(cond_params)

      embedding_unions = []
      for item in group_items:
        if not item.dense_embedding:
          raise ValueError(f"Item {item.id} missing embedding")
        id_param_name = f"_bq_item_id_{item_counter}"
        item_counter += 1
        query_parameters.append(
            _to_query_parameter(str(item.id), name=id_param_name))
        embedding_str = (
            f"SELECT @{id_param_name} as id, "
            f"{[float(x) for x in item.dense_embedding]} "
            f"as embedding")
        embedding_unions.append(embedding_str)
      group_embeddings = " UNION ALL ".join(embedding_unions)

      where_clause = f"WHERE {condition}" if condition else ""
      vector_search = f"""
            SELECT
                query.id,
                ARRAY_AGG(
                    STRUCT({"distance, " if self.include_distance else ""}\
 {base_columns_str})
                ) as chunks
            FROM VECTOR_SEARCH(
                (SELECT {columns_str}, {self.embedding_column}
                 FROM `{self.table_name}`
                 {where_clause}),
                '{self.embedding_column}',
                (SELECT * FROM ({group_embeddings})),
                top_k => {self.neighbor_count}
                {distance_clause}
                {options_clause}
            )
            GROUP BY query.id
        """
      vector_searches.append(vector_search)

    combined_searches = " UNION ALL ".join(vector_searches)
    query = f"""
        {metadata_fn}

        {combined_searches}
    """
    return query, query_parameters

  def format_query(self, items: list[EmbeddableItem]) -> str:
    """Format the vector search query template."""
    query, _ = self.build_query(items)
    return query


class BigQueryVectorSearchEnrichmentHandler(
    EnrichmentSourceHandler[Union[EmbeddableItem, list[EmbeddableItem]],
                            list[tuple[EmbeddableItem, dict[str, Any]]]]):
  """Enrichment handler that performs vector similarity search using BigQuery.
  
  This handler enriches EmbeddableItems by finding similar vectors in a
  BigQuery table using the VECTOR_SEARCH function. It supports batching
  requests for efficiency and preserves the original metadata while adding
  the search results.

  Example:
      >>> from apache_beam.ml.rag.types import EmbeddableItem
      >>> from apache_beam.ml.rag.types import Content, Embedding
      >>> 
      >>> # Configure vector search
      >>> params = BigQueryVectorSearchParameters(
      ...     table_name='project.dataset.embeddings',
      ...     embedding_column='embedding',
      ...     columns=['content', 'metadata'],
      ...     neighbor_count=2,
      ...     metadata_restriction_template="language = '{language}'"
      ... )
      >>> 
      >>> # Create handler
      >>> handler = BigQueryVectorSearchEnrichmentHandler(
      ...     project='my-project',
      ...     vector_search_parameters=params,
      ...     min_batch_size=100,
      ...     max_batch_size=1000
      ... )
      >>> 
      >>> # Use in pipeline
      >>> with beam.Pipeline() as p:
      ...     enriched = (
      ...         p 
      ...         | beam.Create([
      ...             EmbeddableItem(
      ...                 id='query1',
      ...                 embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]),
      ...                 content=Content(text='test query'),
      ...                 metadata={'language': 'en'}
      ...             )
      ...         ])
      ...         | Enrichment(handler)
      ...     )

  Args:
      vector_search_parameters: Configuration for the vector search query
      min_batch_size: Minimum number of items to process in one batch
      max_batch_size: Maximum number of items to process in one batch
      log_query: Debug option to log the BigQuery query
      **kwargs: Additional arguments passed to bigquery.Client

  The handler will:
  1. Batch incoming embeddable_items according to batch size parameters
  2. Format and execute vector search query for each batch
  3. Join results back to original embeddable_items
  4. Return tuples of (original_embeddable_item, search_results)
  """
  def __init__(
      self,
      vector_search_parameters: BigQueryVectorSearchParameters,
      *,
      min_batch_size: int = 1,
      max_batch_size: int = 1000,
      log_query=False,
      **kwargs):
    self.project = vector_search_parameters.project
    self.vector_search_parameters = vector_search_parameters
    self.kwargs = kwargs
    self._batching_kwargs = {
        'min_batch_size': min_batch_size, 'max_batch_size': max_batch_size
    }
    self.join_fn = join_fn
    self.use_custom_types = True
    self.log_query = log_query

  def __enter__(self):
    self.client = bigquery.Client(project=self.project, **self.kwargs)

  def __call__(
      self,
      request: Union[EmbeddableItem, list[EmbeddableItem]],
      *args,
      **kwargs) -> list[tuple[EmbeddableItem, dict[str, Any]]]:
    """Process request(s) using BigQuery vector search.
        
        Args:
            request: Single EmbeddableItem with embedding or list of
            EmbeddableItems with embeddings to process

        Returns:
            EmbeddableItem(s) where metadata['enrichment_output'] contains
            the data retrieved via BigQuery VECTOR_SEARCH.
        """
    # Convert single request to list for uniform processing
    requests = request if isinstance(request, list) else [request]

    # Generate and execute query
    query, query_parameters = self.vector_search_parameters.build_query(
        requests)
    if self.log_query:
      _LOGGER.info("Executing query %s", query)
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = self.client.query(query, job_config=job_config)
    results = query_job.result()

    # Create results dict with empty embeddable_items list as default
    results_by_id = {}
    for result_row in results:
      result_dict = dict(result_row.items())
      results_by_id[result_row.id] = result_dict

    # Return all embeddable_items in original order, with empty results if
    # no matches
    response = []
    for embeddable_item in requests:
      result_dict = results_by_id.get(embeddable_item.id, {})
      response.append((embeddable_item, result_dict))

    return response

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()

  def batch_elements_kwargs(self) -> dict[str, int]:
    """Returns kwargs for beam.BatchElements."""
    return self._batching_kwargs


def join_fn(left: Embedding, right: dict[str, Any]) -> Embedding:
  left.metadata['enrichment_data'] = right
  return left
