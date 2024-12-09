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
from typing import Optional, List, Dict, Any, Union
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.ml.rag.types import Embedding
from google.cloud import bigquery


@dataclass
class BigQueryVectorSearchParameters:
  """Parameters for BigQuery vector search."""
  table_name: str
  embedding_column: str
  columns: List[str]
  neighbor_count: int
  metadata_restriction_template: str
  distance_type: Optional[str] = None
  options: Optional[Dict[str, Any]] = None

  def format_query(self, embeddings: List[Embedding]) -> str:
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
    for emb in embeddings:
      embedding_str = (
          f"SELECT '{emb.id}' as id, "
          f"{[float(x) for x in emb.dense_embedding]} as embedding")
      embedding_unions.append(embedding_str)
    embeddings_query = " UNION ALL ".join(embedding_unions)

    # Format metadata restrictions for each embedding
    metadata_restrictions = [
        f"({self.metadata_restriction_template.format(**emb.metadata)})"
        for emb in embeddings
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
    EnrichmentSourceHandler[Union[Embedding, List[Embedding]],
                            Union[Embedding, List[Embedding]]]):
  """Enrichment handler for BigQuery vector search."""
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

  def __call__(
      self, request: Union[Embedding, List[Embedding]], *args,
      **kwargs) -> Union[Embedding, List[Embedding]]:
    """Process request(s) using BigQuery vector search.
        
        Args:
            request: Single Embedding or list of Embeddings to process
            
        Returns:
            Processed Embedding(s) with enrichment_output in metadata
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
