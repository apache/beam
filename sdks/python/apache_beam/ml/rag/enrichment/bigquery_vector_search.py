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

import json
import logging
import math
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from typing import Optional
from typing import Union

from google.cloud import bigquery

from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.bigquery import COLUMN_NAME_PATTERN
from apache_beam.transforms.enrichment_handlers.bigquery import MAX_BQ_QUERY_PARAMETERS
from apache_beam.transforms.enrichment_handlers.bigquery import SIMPLE_IDENTIFIER_PATTERN
from apache_beam.transforms.enrichment_handlers.bigquery import TABLE_NAME_PATTERN
from apache_beam.transforms.enrichment_handlers.bigquery import scan_sql_template
from apache_beam.transforms.enrichment_handlers.bigquery import to_query_parameter
from apache_beam.transforms.enrichment_handlers.bigquery import validate_bigquery_identifier

_LOGGER = logging.getLogger(__name__)


def _format_sql_string_literal(value: str) -> str:
  escaped = value.replace('\\', '\\\\').replace("'", "\\'")
  return f"'{escaped}'"


def _extract_metadata_template_keys(template: str) -> tuple[str, ...]:
  keys: list[str] = []

  def _collect(kind: str, spec: str, in_string: bool) -> str:
    del in_string
    if kind == 'qmark' or (kind == 'brace' and (spec == '' or spec.isdigit())):
      raise ValueError(
          "String metadata_restriction_template only supports named metadata "
          f"placeholders such as '{{key}}' or '@key', got: {template!r}")
    keys.append(spec)
    return '@_placeholder'

  scan_sql_template(template, _collect)
  return tuple(dict.fromkeys(keys))


def _parameterize_callable_tuple(
    cond_sql: str,
    raw_params: Sequence[Any],
    param_counter: list[int],
) -> tuple[str, list[Any]]:
  if not raw_params:

    def _reject_unbound(kind: str, spec: str, in_string: bool) -> str:
      del in_string
      raise ValueError(
          f"Unbound placeholder ({kind}:{spec!r}) in callable "
          f"metadata_restriction_template with empty parameters: {cond_sql!r}")

    return scan_sql_template(cond_sql, _reject_unbound), []

  named_flags = [
      hasattr(p, 'to_api_repr') and getattr(p, 'name', None) is not None
      for p in raw_params
  ]
  if any(named_flags) and not all(named_flags):
    raise ValueError(
        "Cannot mix named and positional query parameters in callable "
        "metadata_restriction_template.")

  cond_params: list[Any] = []
  if all(named_flags):
    orig_by_name = {p.name: p for p in raw_params}
    renamed_map: dict[str, str] = {}

    def _render_named(kind: str, spec: str, in_string: bool) -> str:
      del in_string
      if kind == 'qmark' or (kind == 'brace' and
                             (spec == '' or spec.isdigit())):
        raise ValueError(
            "Positional placeholder used with named query parameters in "
            f"callable metadata_restriction_template: {cond_sql!r}")
      if spec not in orig_by_name:
        raise ValueError(
            f"Unknown query parameter {spec!r} in callable "
            f"metadata_restriction_template: {cond_sql!r}")
      if spec not in renamed_map:
        new_name = f"_bq_meta_{param_counter[0]}"
        param_counter[0] += 1
        renamed_map[spec] = new_name
        cond_params.append(
            to_query_parameter(orig_by_name[spec], name=new_name))
      return f"@{renamed_map[spec]}"

    rewritten_sql = scan_sql_template(cond_sql, _render_named)
    return rewritten_sql, cond_params

  seq_idx = 0
  has_auto = False
  has_manual = False
  slot_param_names: dict[int, str] = {}

  def _render_positional(kind: str, spec: str, in_string: bool) -> str:
    del in_string
    nonlocal seq_idx, has_auto, has_manual
    if kind == 'qmark' or (kind == 'brace' and spec == ''):
      has_auto = True
      if has_manual:
        raise ValueError(
            "Cannot mix automatic positional placeholders with manual "
            "placeholders in callable metadata_restriction_template.")
      idx = seq_idx
      seq_idx += 1
    elif kind == 'brace' and spec.isdigit():
      has_manual = True
      if has_auto:
        raise ValueError(
            "Cannot mix automatic positional placeholders with manual "
            "placeholders in callable metadata_restriction_template.")
      idx = int(spec)
    else:
      raise ValueError(
          f"Named placeholder {spec!r} used with positional parameters in "
          f"callable metadata_restriction_template: {cond_sql!r}")

    if idx >= len(raw_params):
      raise ValueError(
          f"Placeholder index {idx} out of range for {len(raw_params)} "
          f"parameter(s) in callable metadata_restriction_template.")
    if idx not in slot_param_names:
      new_name = f"_bq_meta_{param_counter[0]}"
      param_counter[0] += 1
      slot_param_names[idx] = new_name
      cond_params.append(to_query_parameter(raw_params[idx], name=new_name))
    return f"@{slot_param_names[idx]}"

  rewritten_sql = scan_sql_template(cond_sql, _render_positional)
  if has_auto and seq_idx != len(raw_params):
    raise ValueError(
        f"Callable metadata_restriction_template provided {len(raw_params)} "
        f"positional parameter(s), but SQL contains {seq_idx} placeholder(s).")
  return rewritten_sql, cond_params


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
    ...     project='my-project',
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
    ...             vector_search_parameters=params
    ...         )
    ...     )
    ... )

    BigQueryVectorSearchParameters encapsulates the configuration needed to
    perform vector similarity search using BigQuery's VECTOR_SEARCH function.
    It handles formatting the query with parameterized embedding vectors and
    metadata restrictions.

    Example with flattened metadata column:

    Table schema::

        embedding: ARRAY<FLOAT64>  # Vector embedding
        content: STRING           # Document content
        language: STRING          # Direct metadata column

    Code::

        >>> params = BigQueryVectorSearchParameters(
        ...     project='my-project',
        ...     table_name='project.dataset.embeddings',
        ...     embedding_column='embedding',
        ...     columns=['content', 'language'],
        ...     neighbor_count=5,
        ...     # For column 'language', value comes from
        ...     # embeddable_item.metadata['language']
        ...     metadata_restriction_template="language = '{language}'"
        ... )
        >>> # Binds embeddable_item.metadata['language'] as a named BigQuery
        >>> # query parameter (e.g., WHERE language = @_bq_meta_0).

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
        ...     project='my-project',
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
        >>> # Binds embeddable_item.metadata['language'] as a named BigQuery
        >>> # query parameter in check_metadata(metadata, 'language', ...).

    Args:
        project: GCP project ID containing the BigQuery dataset.
        table_name: Fully qualified BigQuery table name containing vectors.
        embedding_column: Column name containing the embedding vectors.
        columns: List of columns to retrieve from matched vectors.
        neighbor_count: Number of similar vectors to return (top-k).
        metadata_restriction_template: Template string or callable for filtering
            vectors.

            * **Template string**: Supports named placeholders ``{metadata_key}``
              (or legacy quoted ``'{metadata_key}'``, whose surrounding quotes
              are automatically stripped) and ``@metadata_key``, bound safely via
              `bigquery.QueryJobConfig` named query parameters:

              1. Flattened metadata columns:
                 ``column_name = '{metadata_key}'`` or
                 ``column_name = @metadata_key``.
              2. Nested repeated metadata (``ARRAY<STRUCT<key, value>>``):
                 ``check_metadata(field_name, 'key_to_match', '{metadata_key}')``.

            * **Callable**: Accepts an `EmbeddableItem` and returns either a
              ``(condition_sql, query_parameters)`` tuple (recommended, using
              ``?`` positional or ``@name`` named parameters) or a raw SQL
              condition string. Warning: returning a raw ``str`` bypasses
              automatic parameterization; callers returning raw SQL strings must
              never interpolate untrusted metadata values directly into SQL text.

        distance_type: Optional distance metric to use. Supported values:
            COSINE (default), EUCLIDEAN, or DOT_PRODUCT.
        options: Optional dictionary of additional VECTOR_SEARCH options
            serialized as a JSON-formatted SQL string literal.
        include_distance: Returns the vector search similarity score if True.
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
    validate_bigquery_identifier(
        self.table_name, 'table_name', TABLE_NAME_PATTERN)
    validate_bigquery_identifier(
        self.embedding_column, 'embedding_column', SIMPLE_IDENTIFIER_PATTERN)
    if not self.columns:
      raise ValueError("BigQueryVectorSearchParameters.columns cannot be empty")
    for col in self.columns:
      validate_bigquery_identifier(col, 'column_name', COLUMN_NAME_PATTERN)
    if (isinstance(self.neighbor_count, bool) or
        not isinstance(self.neighbor_count, int) or self.neighbor_count <= 0):
      raise ValueError(
          f"Invalid BigQuery neighbor_count: {self.neighbor_count!r}. "
          "Must be a positive integer.")
    if self.distance_type is not None:
      validate_bigquery_identifier(
          self.distance_type, 'distance_type', SIMPLE_IDENTIFIER_PATTERN)
    self._formatted_options = ""
    if self.options is not None:
      if (not isinstance(self.options, Mapping) or
          not all(isinstance(k, str) for k in self.options.keys())):
        raise ValueError(
            "Invalid VECTOR_SEARCH options: must be a mapping with string "
            "keys.")
      try:
        options_json = json.dumps(
            dict(self.options), allow_nan=False, sort_keys=True)
      except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid VECTOR_SEARCH options: {e}") from e
      if self.options:
        self._formatted_options = _format_sql_string_literal(options_json)
    self._metadata_keys: tuple[str, ...] = ()
    if isinstance(self.metadata_restriction_template, str):
      self._metadata_keys = _extract_metadata_template_keys(
          self.metadata_restriction_template)

  def _evaluate_restrict_for_grouping(self,
                                      item: EmbeddableItem) -> tuple[Any, Any]:
    if not self.metadata_restriction_template:
      return "", None
    if callable(self.metadata_restriction_template):
      res = self.metadata_restriction_template(item)
      if isinstance(res, tuple):
        cond_sql, raw_params = res
        key_params = tuple(
            repr(p.to_api_repr()) if hasattr(p, 'to_api_repr') else (
                type(p).__qualname__, repr(p)) for p in raw_params)
        return (cond_sql, key_params), res
      return res, res
    group_key = tuple(
        (k, type(item.metadata[k]).__qualname__, repr(item.metadata[k]))
        for k in self._metadata_keys)
    return group_key, None

  def _parameterize_restrict(
      self,
      item: EmbeddableItem,
      raw_restrict: Any,
      param_counter: list[int],
  ) -> tuple[str, list[Any]]:
    assert self.metadata_restriction_template is not None, (
        "metadata_restriction_template cannot be None when formatting. "
        "This indicates a logical error in the code."
    )
    if callable(self.metadata_restriction_template):
      if isinstance(raw_restrict, tuple):
        cond_sql, raw_params = raw_restrict
        return _parameterize_callable_tuple(cond_sql, raw_params, param_counter)
      return raw_restrict, []

    params: list[Any] = []
    group_key_to_param: dict[str, str] = {}

    def _render_meta(kind: str, spec: str, in_string: bool) -> str:
      del kind, in_string
      if spec not in group_key_to_param:
        val = item.metadata[spec]
        param_name = f"_bq_meta_{param_counter[0]}"
        param_counter[0] += 1
        group_key_to_param[spec] = param_name
        params.append(to_query_parameter(val, name=param_name))
      return f"@{group_key_to_param[spec]}"

    cond_sql = scan_sql_template(
        self.metadata_restriction_template, _render_meta)
    return cond_sql, params

  def build_query(self, items: list[EmbeddableItem]) -> tuple[str, list[Any]]:
    """Build the parameterized vector search SQL query and query parameters."""
    if not items:
      raise ValueError("Cannot build vector search query for empty items list.")
    base_columns_str = ", ".join(f"base.{col}" for col in self.columns)
    columns_str = ", ".join(self.columns)
    table_ref = (
        self.table_name if '`' in self.table_name else f"`{self.table_name}`")
    distance_clause = (
        f", distance_type => '{self.distance_type}'"
        if self.distance_type else "")
    options_clause = (
        f", options => {self._formatted_options}"
        if self._formatted_options else "")

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

    condition_groups: dict[Any, tuple[Any, list[EmbeddableItem]]] = {}
    if self.metadata_restriction_template:
      for item in items:
        group_key, raw_restrict = self._evaluate_restrict_for_grouping(item)
        if group_key not in condition_groups:
          condition_groups[group_key] = (raw_restrict, [])
        condition_groups[group_key][1].append(item)
    else:
      condition_groups[""] = (None, list(items))

    vector_searches = []
    query_parameters: list[Any] = []
    param_counter = [0]
    item_counter = 0

    for raw_restrict, group_items in condition_groups.values():
      condition = ""
      if self.metadata_restriction_template and group_items:
        condition, cond_params = self._parameterize_restrict(
            group_items[0], raw_restrict, param_counter)
        query_parameters.extend(cond_params)

      embedding_unions = []
      for item in group_items:
        if not item.dense_embedding:
          raise ValueError(f"Item {item.id} missing embedding")
        emb_values = [float(x) for x in item.dense_embedding]
        if not all(math.isfinite(v) for v in emb_values):
          raise ValueError(
              f"Item {item.id} contains non-finite embedding value(s)")
        id_param_name = f"_bq_item_id_{item_counter}"
        emb_param_name = f"_bq_item_emb_{item_counter}"
        item_counter += 1
        query_parameters.append(
            to_query_parameter(str(item.id), name=id_param_name))
        query_parameters.append(
            bigquery.ArrayQueryParameter(emb_param_name, 'FLOAT64', emb_values))
        embedding_str = (
            f"SELECT @{id_param_name} as id, "
            f"@{emb_param_name} as embedding")
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
                 FROM {table_ref}
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
    """Format the parameterized vector search SQL query template.

    Note:
        Dynamic item IDs, embedding vectors, and metadata restriction values are
        emitted as named BigQuery query parameter placeholders
        (``@_bq_item_id_*``, ``@_bq_item_emb_*``, ``@_bq_meta_*``). Call
        :meth:`build_query` to retrieve both the SQL string and the bound
        ``query_parameters`` list required by ``bigquery.QueryJobConfig``.
    """
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
      ...     project='my-project',
      ...     table_name='project.dataset.embeddings',
      ...     embedding_column='embedding',
      ...     columns=['content', 'metadata'],
      ...     neighbor_count=2,
      ...     metadata_restriction_template="language = '{language}'"
      ... )
      >>>
      >>> # Create handler
      >>> handler = BigQueryVectorSearchEnrichmentHandler(
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

  def _execute_chunk(self, chunk: list[EmbeddableItem]) -> dict[str, Any]:
    query, query_parameters = self.vector_search_parameters.build_query(chunk)
    if (len(query_parameters) > MAX_BQ_QUERY_PARAMETERS and len(chunk) > 1):
      mid = len(chunk) // 2
      merged = self._execute_chunk(chunk[:mid])
      merged.update(self._execute_chunk(chunk[mid:]))
      return merged

    if self.log_query:
      _LOGGER.info("Executing query %s", query)
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = self.client.query(query, job_config=job_config)
    results = query_job.result()

    results_by_id: dict[str, Any] = {}
    for result_row in results:
      result_dict = dict(result_row.items())
      results_by_id[result_row.id] = result_dict
    return results_by_id

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
            List of tuples of ``(EmbeddableItem, search_result_dict)``.
        """
    requests = request if isinstance(request, list) else [request]
    if not requests:
      return []

    results_by_id = self._execute_chunk(requests)

    response = []
    for embeddable_item in requests:
      result_dict = results_by_id.get(
          embeddable_item.id, results_by_id.get(str(embeddable_item.id), {}))
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
