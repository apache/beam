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

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from google.cloud import bigquery

from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Embedding
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

_LOGGER = logging.getLogger(__name__)


@dataclass
class BigQueryVectorSearchParameters:
  """Parameters for configuring BigQuery vector similarity search.

    This class is used by BigQueryVectorSearchEnrichmentHandler to perform
    vector similarity search using BigQuery's VECTOR_SEARCH function. It
    processes :class:`~apache_beam.ml.rag.types.Chunk` objects that contain
    :class:`~apache_beam.ml.rag.types.Embedding` and returns similar vectors
    from a BigQuery table.

    BigQueryVectorSearchEnrichmentHandler is used with
    :class:`~apache_beam.transforms.enrichment.Enrichment` transform to enrich
    Chunks with similar content from a vector database. For example:

    >>> # Create search parameters
    >>> params = BigQueryVectorSearchParameters(
    ...     table_name='project.dataset.embeddings',
    ...     embedding_column='embedding',
    ...     columns=['content'],
    ...     neighbor_count=5
    ... )
    >>> # Use in pipeline
    >>> enriched = (
    ...     chunks
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
        ...     # chunk.metadata['language']
        ...     metadata_restriction_template="language = '{language}'"
        ... )
        >>> # When processing a chunk with metadata={'language': 'en'},
        >>> # generates: WHERE language = 'en'

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
        ...     # check_metadata(field_name, key_to_search, value_from_chunk)
        ...     metadata_restriction_template=(
        ...         "check_metadata(metadata, 'language', '{language}')"
        ...     )
        ... )
        >>> # When processing a chunk with metadata={'language': 'en'},
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
               chunk.metadata[metadata_key].
            2. For nested repeated metadata (ARRAY<STRUCT<key,value>>):
               ``check_metadata(field_name, 'key_to_match', '{metadata_key}')``
               where field_name is the ARRAY<STRUCT> column in BigQuery,
               key_to_match is the literal key to search for in the array, and
               metadata_key is used to get value from
               chunk.metadata[metadata_key].
            
            Multiple conditions can be combined using AND/OR operators. For
            example::
            
                >>> # Combine metadata check with column filter
                >>> template = (
                ...     "check_metadata(metadata, 'language', '{language}') "
                ...     "AND source = '{source}'"
                ... )
                >>> # When chunk.metadata = {'language': 'en', 'source': 'web'}
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
  columns: List[str]
  neighbor_count: int
  metadata_restriction_template: Optional[Union[str, Callable[[Chunk],
                                                              str]]] = None
  distance_type: Optional[str] = None
  options: Optional[Dict[str, Any]] = None
  include_distance: bool = False

  def _format_restrict(self, chunk: Chunk) -> str:
    assert self.metadata_restriction_template is not None, (
        "metadata_restriction_template cannot be None when formatting. "
        "This indicates a logical error in the code."
    )

    if callable(self.metadata_restriction_template):
      return self.metadata_restriction_template(chunk)
    return self.metadata_restriction_template.format(**chunk.metadata)

  def format_query(self, chunks: List[Chunk]) -> str:
    """Format the vector search query template."""
    base_columns_str = ", ".join(f"base.{col}" for col in self.columns)
    columns_str = ", ".join(self.columns)
    distance_clause = (
        f", distance_type => '{self.distance_type}'"
        if self.distance_type else "")
    options_clause = (f", options => {self.options}" if self.options else "")

    # Create metadata check function only if needed
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

    # Group chunks by their metadata conditions
    condition_groups = defaultdict(list)
    if self.metadata_restriction_template:
      for chunk in chunks:
        condition = self._format_restrict(chunk)
        condition_groups[condition].append(chunk)
    else:
      # No metadata filtering - all chunks in one group
      condition_groups[""] = chunks

    # Generate VECTOR_SEARCH subqueries for each condition group
    vector_searches = []
    for condition, group_chunks in condition_groups.items():
      # Create embeddings subquery for this group
      embedding_unions = []
      for chunk in group_chunks:
        if not chunk.dense_embedding:
          raise ValueError(f"Chunk {chunk.id} missing embedding")
        embedding_str = (
            f"SELECT '{chunk.id}' as id, "
            f"{[float(x) for x in chunk.dense_embedding]} "
            f"as embedding")
        embedding_unions.append(embedding_str)
      group_embeddings = " UNION ALL ".join(embedding_unions)

      where_clause = f"WHERE {condition}" if condition else ""
      # Create VECTOR_SEARCH for this condition group
      vector_search = f"""
            SELECT 
                query.id,
                ARRAY_AGG(
                    STRUCT({"distance, " if self.include_distance else ""} {base_columns_str})
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

    # Combine all vector searches
    combined_searches = " UNION ALL ".join(vector_searches)

    return f"""
        {metadata_fn}

        {combined_searches}
    """


class BigQueryVectorSearchEnrichmentHandler(
    EnrichmentSourceHandler[Union[Chunk, List[Chunk]],
                            List[Tuple[Chunk, Dict[str, Any]]]]):
  """Enrichment handler that performs vector similarity search using BigQuery.
  
  This handler enriches Chunks by finding similar vectors in a BigQuery table
  using the VECTOR_SEARCH function. It supports batching requests for efficiency
  and preserves the original Chunk metadata while adding the search results.

  Example:
      >>> from apache_beam.ml.rag.types import Chunk, Content, Embedding
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
      ...             Chunk(
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
      min_batch_size: Minimum number of chunks to batch before processing
      max_batch_size: Maximum number of chunks to process in one batch
      log_query: Debug option to log the BigQuery query
      **kwargs: Additional arguments passed to bigquery.Client

  The handler will:
  1. Batch incoming chunks according to batch size parameters
  2. Format and execute vector search query for each batch
  3. Join results back to original chunks
  4. Return tuples of (original_chunk, search_results)
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

  def __call__(self, request: Union[Chunk, List[Chunk]], *args,
               **kwargs) -> List[Tuple[Chunk, Dict[str, Any]]]:
    """Process request(s) using BigQuery vector search.
        
        Args:
            request: Single Chunk with embedding or list of Chunk's with
            embeddings to process
            
        Returns:
            Chunk(s) where chunk.metadata['enrichment_output'] contains the
            data retrieved via BigQuery VECTOR_SEARCH.
        """
    # Convert single request to list for uniform processing
    requests = request if isinstance(request, list) else [request]

    # Generate and execute query
    query = self.vector_search_parameters.format_query(requests)
    if self.log_query:
      _LOGGER.info("Executing query %s", query)
    query_job = self.client.query(query)
    results = query_job.result()

    # Create results dict with empty chunks list as default
    results_by_id = {}
    for result_row in results:
      result_dict = dict(result_row.items())
      results_by_id[result_row.id] = result_dict

    # Return all chunks in original order, with empty results if no matches
    response = []
    for chunk in requests:
      result_dict = results_by_id.get(chunk.id, {})
      response.append((chunk, result_dict))

    return response

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()

  def batch_elements_kwargs(self) -> Dict[str, int]:
    """Returns kwargs for beam.BatchElements."""
    return self._batching_kwargs


def join_fn(left: Embedding, right: Dict[str, Any]) -> Embedding:
  left.metadata['enrichment_data'] = right
  return left
