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
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from google.cloud import bigquery

from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Embedding
from apache_beam.transforms.enrichment import EnrichmentSourceHandler


@dataclass
class BigQueryVectorSearchParameters:
  """Parameters for configuring BigQuery vector similarity search.

    This class encapsulates the configuration needed to perform vector
    similarity search using BigQuery's VECTOR_SEARCH function. It handles
    formatting the query with proper embedding vectors and metadata
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
        metadata: ARRAY<STRUCT<   # Nested repeated metadata
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
        table_name: Fully qualified BigQuery table name containing vectors.
        embedding_column: Column name containing the embedding vectors.
        columns: List of columns to retrieve from matched vectors.
        neighbor_count: Number of similar vectors to return (top-k).
        metadata_restriction_template: Template string for filtering vectors. 
            Two formats supported:
            
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
        
        distance_type: Optional distance metric to use. Supported values:
            COSINE (default), EUCLIDEAN, or DOT_PRODUCT.
        options: Optional dictionary of additional VECTOR_SEARCH options.
    """
  table_name: str
  embedding_column: str
  columns: List[str]
  neighbor_count: int
  metadata_restriction_template: str
  distance_type: Optional[str] = None
  options: Optional[Dict[str, Any]] = None

  def format_query(self, chunks: List[Chunk]) -> str:
    """Format the vector search query template."""
    base_columns_str = ", ".join(f"base.{col}" for col in self.columns)
    columns_str = ", ".join(self.columns)
    distance_clause = (
        f", distance_type => '{self.distance_type}'"
        if self.distance_type else "")
    options_clause = (f", options => {self.options}" if self.options else "")

    # Create metadata check function
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
        """

    # Union embeddings with IDs
    embedding_unions = []
    for chunk in chunks:
      if chunk.embedding is None or chunk.embedding.dense_embedding is None:
        raise ValueError(f"Chunk {chunk.id} missing embedding")
      embedding_str = (
          f"SELECT '{chunk.id}' as id, "
          f"{[float(x) for x in chunk.embedding.dense_embedding]} as embedding")
      embedding_unions.append(embedding_str)
    embeddings_query = " UNION ALL ".join(embedding_unions)

    # Format metadata restrictions for each embedding
    metadata_restrictions = [
        f"({self.metadata_restriction_template.format(**chunk.metadata)})"
        for chunk in chunks
    ]
    combined_restrictions = " OR ".join(metadata_restrictions)

    return f"""
        {metadata_fn}

        WITH query_embeddings AS ({embeddings_query})

        SELECT 
            query.id,
            ARRAY_AGG(
                STRUCT({base_columns_str})
            ) as chunks
        FROM VECTOR_SEARCH(
            (SELECT {columns_str}, {self.embedding_column} 
             FROM `{self.table_name}`
             WHERE {combined_restrictions}),
            '{self.embedding_column}',
            TABLE query_embeddings,
            top_k => {self.neighbor_count}
            {distance_clause}
            {options_clause}
        )
        GROUP BY query.id
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
      project: GCP project ID containing the BigQuery dataset
      vector_search_parameters: Configuration for the vector search query
      min_batch_size: Minimum number of chunks to batch before processing
      max_batch_size: Maximum number of chunks to process in one batch
      **kwargs: Additional arguments passed to bigquery.Client

  The handler will:
  1. Batch incoming chunks according to batch size parameters
  2. Format and execute vector search query for each batch
  3. Join results back to original chunks
  4. Return tuples of (original_chunk, search_results)
  """
  def __init__(
      self,
      project: str,
      vector_search_parameters: BigQueryVectorSearchParameters,
      *,
      min_batch_size: int = 1,
      max_batch_size: int = 1000,
      **kwargs):
    self.project = project
    self.vector_search_parameters = vector_search_parameters
    self.kwargs = kwargs
    self._batching_kwargs = {
        'min_batch_size': min_batch_size, 'max_batch_size': max_batch_size
    }
    self.join_fn = join_fn
    self.use_custom_types = True

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
    query_job = self.client.query(query)
    results = query_job.result()

    # Map results back to embeddings
    id_to_embedding = {emb.id: emb for emb in requests}
    response = []
    for result_row in results:
      result_dict = dict(result_row.items())
      embedding = id_to_embedding[result_row.id]
      response.append((embedding, result_dict))

    return response

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()

  def batch_elements_kwargs(self) -> Dict[str, int]:
    """Returns kwargs for beam.BatchElements."""
    return self._batching_kwargs


def join_fn(left: Embedding, right: Dict[str, Any]) -> Embedding:
  left.metadata['enrichment_data'] = right
  return left
